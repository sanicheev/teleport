/*
 * Teleport
 * Copyright (C) 2025  Gravitational, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package alpnproxy

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"

	"github.com/gravitational/teleport/lib/utils"
)

// proxyConnWithResponseTimeout proxies traffic between the client and the
// upstream connection, resetting both after timeout without application payload
// in either direction so the client fails instead of waiting on a tunnel that is
// dead at the application level but alive at the TCP level. Keepalive pings are
// stripped before they reach this layer and do not count. Connections the client
// has not used are left alone; connections left idle between requests are not,
// since request boundaries are not visible in an opaque byte stream.
func proxyConnWithResponseTimeout(
	ctx context.Context,
	client, upstream net.Conn,
	timeout time.Duration,
	clock clockwork.Clock,
	log *slog.Logger,
) error {
	waiter := &responseWaiter{clock: clock, timeout: timeout}

	watchCtx, stopWatching := context.WithCancel(ctx)
	defer stopWatching()

	go func() {
		if !waiter.waitForTimeout(watchCtx) {
			return
		}
		log.WarnContext(ctx, "Resetting connection, application did not respond in time",
			"timeout", timeout,
			"client_addr", client.RemoteAddr().String(),
		)
		if err := resetConns(client, upstream); err != nil {
			log.DebugContext(ctx, "Failed to reset connections", "error", err)
		}
	}()

	return trace.Wrap(utils.ProxyConn(ctx,
		&observedConn{Conn: client, onRead: waiter.clientPayload},
		&observedConn{Conn: upstream, onRead: waiter.upstreamPayload},
	))
}

// responseWaiter tracks inactivity once the client has started using the
// connection. The deadline is armed by the client's first payload and extended
// by payload in either direction; it is never cleared.
type responseWaiter struct {
	clock   clockwork.Clock
	timeout time.Duration

	mu sync.Mutex
	// deadline is zero until the client sends its first payload.
	deadline time.Time
}

// clientPayload arms and extends the deadline.
func (w *responseWaiter) clientPayload() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deadline = w.clock.Now().Add(w.timeout)
}

// upstreamPayload extends the deadline, but never arms it: a client that has
// sent nothing is not waiting for anything.
func (w *responseWaiter) upstreamPayload() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.deadline.IsZero() {
		w.deadline = w.clock.Now().Add(w.timeout)
	}
}

// remaining reports how long is left before the connection is considered dead.
// Until the client sends its first payload there is nothing to wait for, so a
// full timeout is returned.
func (w *responseWaiter) remaining() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.deadline.IsZero() {
		return w.timeout
	}
	return w.deadline.Sub(w.clock.Now())
}

// waitForTimeout blocks until the application has failed to respond in time,
// returning false if ctx is canceled first.
func (w *responseWaiter) waitForTimeout(ctx context.Context) bool {
	timer := w.clock.NewTimer(w.timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.Chan():
			remaining := w.remaining()
			if remaining <= 0 {
				// ctx can be canceled in the same moment the timer fires.
				return ctx.Err() == nil
			}
			timer.Reset(remaining)
		}
	}
}

// observedConn reports successful reads to onRead.
type observedConn struct {
	net.Conn
	onRead func()
}

func (c *observedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.onRead()
	}
	return n, err
}

// resetConns closes conns, sending a TCP RST to each peer where possible. A
// reset fails the peer's pending reads immediately, whereas a graceful close
// can leave a peer that is waiting for a response hanging.
func resetConns(conns ...net.Conn) error {
	// Arm every connection before closing any of them: closing one ends the
	// copy loops, which would otherwise close the others gracefully first.
	for _, conn := range conns {
		if tcpConn, ok := underlyingTCPConn(conn); ok {
			// Best effort: on failure the peer gets a FIN instead of a RST.
			_ = tcpConn.SetLinger(0)
		}
	}

	var errs []error
	for _, conn := range conns {
		if err := conn.Close(); err != nil && !utils.IsUseOfClosedNetworkError(err) {
			errs = append(errs, err)
		}
	}
	return trace.NewAggregate(errs...)
}

// underlyingTCPConn unwraps conn to find the TCP connection it is built on.
func underlyingTCPConn(conn net.Conn) (*net.TCPConn, bool) {
	for conn != nil {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			return tcpConn, true
		}
		unwrapper, ok := conn.(interface{ NetConn() net.Conn })
		if !ok {
			return nil, false
		}
		conn = unwrapper.NetConn()
	}
	return nil, false
}
