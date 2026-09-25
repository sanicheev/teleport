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
	"bytes"
	"context"
	"log/slog"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestResponseWaiter(t *testing.T) {
	t.Parallel()

	const timeout = 10 * time.Second

	type step struct {
		client   bool
		upstream bool
		advance  time.Duration
	}
	tests := []struct {
		name        string
		steps       []step
		wantExpired bool
		// wantRemaining, when set, is the exact time left on the deadline.
		wantRemaining time.Duration
	}{
		{
			name:  "idle connection is never reset",
			steps: []step{{advance: time.Hour}},
		},
		{
			name: "armed deadline reports the time left",
			steps: []step{
				{client: true},
				{advance: timeout - 3*time.Second},
			},
			wantRemaining: 3 * time.Second,
		},
		{
			name: "upstream payload alone never arms the deadline",
			steps: []step{
				{upstream: true},
				{advance: time.Hour},
			},
		},
		{
			name: "request unanswered within the timeout",
			steps: []step{
				{client: true},
				{advance: timeout - time.Second},
			},
		},
		{
			name: "request unanswered past the timeout",
			steps: []step{
				{client: true},
				{advance: timeout},
			},
			wantExpired: true,
		},
		{
			name: "ongoing request extends the deadline",
			steps: []step{
				{client: true},
				{advance: timeout - time.Second},
				{client: true},
				{advance: timeout - time.Second},
			},
		},
		{
			name: "ongoing response extends the deadline",
			steps: []step{
				{client: true},
				{advance: timeout - time.Second},
				{upstream: true},
				{advance: timeout - time.Second},
			},
		},
		{
			name: "stall after a partial response expires",
			steps: []step{
				{client: true},
				{advance: timeout - time.Second},
				{upstream: true},
				{advance: timeout},
			},
			wantExpired: true,
		},
		{
			name: "answered connection left idle expires",
			steps: []step{
				{client: true},
				{upstream: true},
				{advance: time.Hour},
			},
			wantExpired: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			clock := clockwork.NewFakeClock()
			waiter := &responseWaiter{clock: clock, timeout: timeout}

			for _, step := range test.steps {
				switch {
				case step.client:
					waiter.clientPayload()
				case step.upstream:
					waiter.upstreamPayload()
				default:
					clock.Advance(step.advance)
				}
			}

			remaining := waiter.remaining()
			require.Equal(t, test.wantExpired, remaining <= 0)
			if test.wantRemaining != 0 {
				require.Equal(t, test.wantRemaining, remaining)
			}
		})
	}
}

func TestProxyConnWithResponseTimeout(t *testing.T) {
	t.Parallel()

	const timeout = 200 * time.Millisecond

	tests := []struct {
		name        string
		clientSends bool
		appReplies  bool
		wantReset   bool
	}{
		{
			name:        "application never responds",
			clientSends: true,
			wantReset:   true,
		},
		{
			// Traffic flows both ways first, so this also covers the
			// no-reset-while-active path.
			name:        "application responds then goes silent",
			clientSends: true,
			appReplies:  true,
			wantReset:   true,
		},
		{
			name: "client sends no request",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client, downstream := tcpConnPair(t)
			upstream, app := tcpConnPair(t)

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			go proxyConnWithResponseTimeout(
				ctx, downstream, upstream, timeout,
				clockwork.NewRealClock(), slog.New(slog.DiscardHandler),
			)

			if test.clientSends {
				_, err := client.Write([]byte("request"))
				require.NoError(t, err)
				requireReads(t, app, "request")
			}
			if test.appReplies {
				_, err := app.Write([]byte("response"))
				require.NoError(t, err)
				requireReads(t, client, "response")
			}

			// Both ends of the tunnel must be reset, or neither.
			clientErr := readErrorAfter(t, client, 5*timeout)
			appErr := readErrorAfter(t, app, 5*timeout)
			if test.wantReset {
				require.ErrorIs(t, clientErr, syscall.ECONNRESET, "client was not reset")
				require.ErrorIs(t, appErr, syscall.ECONNRESET, "application was not reset")
				return
			}
			require.ErrorIs(t, clientErr, os.ErrDeadlineExceeded, "client was reset")
			require.ErrorIs(t, appErr, os.ErrDeadlineExceeded, "application was reset")
		})
	}
}

// TestProxyConnWithResponseTimeoutExits checks that the watchdog goroutine is
// gone once the connection closes on its own, well before the timeout.
func TestProxyConnWithResponseTimeoutExits(t *testing.T) {
	ignoreExisting := goleak.IgnoreCurrent()
	defer goleak.VerifyNone(t, ignoreExisting)

	client, downstream := tcpConnPair(t)
	upstream, app := tcpConnPair(t)

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		proxyConnWithResponseTimeout(
			context.Background(), downstream, upstream, time.Hour,
			clockwork.NewRealClock(), slog.New(slog.DiscardHandler),
		)
	}()

	_, err := client.Write([]byte("request"))
	require.NoError(t, err)
	requireReads(t, app, "request")
	require.NoError(t, client.Close())

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("proxyConnWithResponseTimeout did not return after the client closed")
	}
}

func TestUnderlyingTCPConn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		conn   func(t *testing.T) net.Conn
		wantOK bool
	}{
		{
			name:   "tcp conn",
			conn:   func(t *testing.T) net.Conn { conn, _ := tcpConnPair(t); return conn },
			wantOK: true,
		},
		{
			name: "wrapped tcp conn",
			conn: func(t *testing.T) net.Conn {
				conn, _ := tcpConnPair(t)
				return newBufferedConn(conn, bytes.NewReader(nil))
			},
			wantOK: true,
		},
		{
			name: "non-tcp conn",
			conn: func(t *testing.T) net.Conn {
				conn, _ := net.Pipe()
				return conn
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			tcpConn, ok := underlyingTCPConn(test.conn(t))
			require.Equal(t, test.wantOK, ok)
			require.Equal(t, test.wantOK, tcpConn != nil)
		})
	}
}

func TestResetConns(t *testing.T) {
	t.Parallel()

	t.Run("tcp peer sees a reset", func(t *testing.T) {
		t.Parallel()

		conn, peer := tcpConnPair(t)
		require.NoError(t, resetConns(conn))
		require.ErrorIs(t, readErrorAfter(t, peer, time.Second), syscall.ECONNRESET)
	})

	t.Run("non-tcp conn is closed", func(t *testing.T) {
		t.Parallel()

		conn, _ := net.Pipe()
		require.NoError(t, resetConns(conn))
	})
}

// tcpConnPair returns both ends of an established TCP connection.
func tcpConnPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	type accepted struct {
		conn net.Conn
		err  error
	}
	acceptedCh := make(chan accepted, 1)
	go func() {
		conn, err := listener.Accept()
		acceptedCh <- accepted{conn, err}
	}()

	client, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	server := <-acceptedCh
	require.NoError(t, server.err)
	t.Cleanup(func() { server.conn.Close() })

	return client, server.conn
}

func requireReads(t *testing.T, conn net.Conn, want string) {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, len(want))
	_, err := conn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, want, string(buf))
	require.NoError(t, conn.SetReadDeadline(time.Time{}))
}

// readErrorAfter returns the error from a read that is given up on after wait.
func readErrorAfter(t *testing.T, conn net.Conn, wait time.Duration) error {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(wait)))
	_, err := conn.Read(make([]byte, 64))
	require.Error(t, err)
	return err
}
