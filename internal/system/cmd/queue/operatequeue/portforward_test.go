/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package operatequeue

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
)

// fakeKubelet stands in for the kubelet end of a port-forward connection. It pairs the error and
// data streams of each request ID and hands the data stream to serve, the way the CRI connects it
// to the port inside the pod; a serve error is reported on the error stream, as the kubelet does.
type fakeKubelet struct {
	serve func(port string, conn net.Conn) error

	mu        sync.Mutex
	created   []http.Header
	pending   map[string]*fakeErrorStream
	closed    chan bool
	closeOnce sync.Once
}

func (k *fakeKubelet) CreateStream(headers http.Header) (httpstream.Stream, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.created = append(k.created, headers.Clone())
	requestID := headers.Get(corev1.PortForwardRequestIDHeader)

	switch headers.Get(corev1.StreamType) {
	case corev1.StreamTypeError:
		r, w := io.Pipe()
		stream := &fakeErrorStream{PipeReader: r, w: w}
		k.pending[requestID] = stream

		return stream, nil
	case corev1.StreamTypeData:
		errorStream, ok := k.pending[requestID]
		if !ok {
			return nil, fmt.Errorf("data stream %s created before its error stream", requestID)
		}

		delete(k.pending, requestID)

		client, server := net.Pipe()
		port := headers.Get(corev1.PortHeader)

		go func() {
			err := k.serve(port, server)
			_ = server.Close()

			if err != nil {
				fmt.Fprintf(errorStream.w, "error forwarding port %s to pod d8-system/deckhouse-0: %v", port, err)
			}

			_ = errorStream.w.Close()
		}()

		return &fakeDataStream{Conn: client}, nil
	}

	return nil, fmt.Errorf("unexpected stream type %q", headers.Get(corev1.StreamType))
}

func (k *fakeKubelet) Close() error {
	k.closeOnce.Do(func() { close(k.closed) })

	return nil
}

func (k *fakeKubelet) CloseChan() <-chan bool             { return k.closed }
func (k *fakeKubelet) SetIdleTimeout(time.Duration)       {}
func (k *fakeKubelet) RemoveStreams(...httpstream.Stream) {}

// fakeErrorStream is the client end of an error stream: Close only ends the unused write side.
type fakeErrorStream struct {
	*io.PipeReader
	w *io.PipeWriter
}

func (s *fakeErrorStream) Write([]byte) (int, error) { return 0, errors.New("write to error stream") }
func (s *fakeErrorStream) Close() error              { return nil }
func (s *fakeErrorStream) Reset() error              { return s.PipeReader.Close() }
func (s *fakeErrorStream) Headers() http.Header      { return nil }
func (s *fakeErrorStream) Identifier() uint32        { return 0 }

type fakeDataStream struct {
	net.Conn
}

func (s *fakeDataStream) Reset() error         { return s.Conn.Close() }
func (s *fakeDataStream) Headers() http.Header { return nil }
func (s *fakeDataStream) Identifier() uint32   { return 0 }

// newFakeTunnel returns a tunnel whose every dial yields a new fakeKubelet running serve.
func newFakeTunnel(t *testing.T, serve func(port string, conn net.Conn) error) (*debugTunnel, *[]*fakeKubelet) {
	t.Helper()

	var dialed []*fakeKubelet

	tunnel := &debugTunnel{dial: func() (httpstream.Connection, error) {
		kubelet := &fakeKubelet{serve: serve, pending: map[string]*fakeErrorStream{}, closed: make(chan bool)}
		dialed = append(dialed, kubelet)

		return kubelet, nil
	}}
	t.Cleanup(tunnel.Close)

	return tunnel, &dialed
}

// serveHTTP answers the single request on conn with handler, like the debug server behind the port.
func serveHTTP(handler http.HandlerFunc) func(string, net.Conn) error {
	return func(_ string, conn net.Conn) error {
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return err
		}

		rec := httptest.NewRecorder()
		handler(rec, req)

		return rec.Result().Write(conn)
	}
}

func TestDebugTunnelGetReturnsBody(t *testing.T) {
	tunnel, dialed := newFakeTunnel(t, serveHTTP(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/queue/list.text" || r.URL.Query().Get("showEmpty") != "true" {
			http.NotFound(w, r)

			return
		}

		fmt.Fprint(w, "Queue 'main': length 0, status: 'waiting for task 1s'\n")
	}))

	for range 2 {
		body, err := tunnel.Get(context.Background(), "/queue/list.text?showEmpty=true")
		require.NoError(t, err)
		require.Equal(t, "Queue 'main': length 0, status: 'waiting for task 1s'\n", body)
	}

	require.Len(t, *dialed, 1, "a healthy connection is reused")

	var streams []string
	for _, headers := range (*dialed)[0].created {
		require.Equal(t, "9652", headers.Get(corev1.PortHeader))

		streams = append(streams, headers.Get(corev1.StreamType)+"/"+headers.Get(corev1.PortForwardRequestIDHeader))
	}

	require.Equal(t, []string{"error/1", "data/1", "error/2", "data/2"}, streams)
}

func TestDebugTunnelGetReportsHTTPError(t *testing.T) {
	tunnel, _ := newFakeTunnel(t, serveHTTP(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "404 page not found", http.StatusNotFound)
	}))

	_, err := tunnel.Get(context.Background(), "/queue/nope.text")
	require.EqualError(t, err, "GET /queue/nope.text: 404 Not Found: 404 page not found")
}

func TestDebugTunnelGetReportsKubeletReasonAndRedials(t *testing.T) {
	refused := true

	tunnel, dialed := newFakeTunnel(t, func(port string, conn net.Conn) error {
		if refused {
			return fmt.Errorf("failed to connect to localhost:%s inside namespace \"host\": connection refused", port)
		}

		return serveHTTP(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "ok") })(port, conn)
	})

	_, err := tunnel.Get(context.Background(), "/queue/main.text")
	require.EqualError(t, err, `error forwarding port 9652 to pod d8-system/deckhouse-0: failed to connect to localhost:9652 inside namespace "host": connection refused`)

	refused = false

	body, err := tunnel.Get(context.Background(), "/queue/main.text")
	require.NoError(t, err)
	require.Equal(t, "ok", body)
	require.Len(t, *dialed, 2, "a failed request drops the connection")
}

func TestDebugTunnelGetKeepsLocalErrorWithoutKubeletReason(t *testing.T) {
	tunnel, _ := newFakeTunnel(t, func(_ string, conn net.Conn) error {
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			return err
		}

		_, err := io.WriteString(conn, "not http\r\n\r\n")

		return err
	})

	_, err := tunnel.Get(context.Background(), "/queue/main.text")
	require.ErrorContains(t, err, "read response: malformed HTTP")
}

func TestDebugTunnelGetRedialsClosedConnection(t *testing.T) {
	tunnel, dialed := newFakeTunnel(t, serveHTTP(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "ok") }))

	_, err := tunnel.Get(context.Background(), "/queue/main.text")
	require.NoError(t, err)

	// The API server or the kubelet hangs up between two watch ticks.
	require.NoError(t, (*dialed)[0].Close())

	_, err = tunnel.Get(context.Background(), "/queue/main.text")
	require.NoError(t, err)
	require.Len(t, *dialed, 2)
}

func TestDebugTunnelGetStopsOnCancel(t *testing.T) {
	tunnel, _ := newFakeTunnel(t, func(_ string, conn net.Conn) error {
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			return err
		}

		// Never answer; return once the client resets the stream.
		_, err := io.Copy(io.Discard, conn)

		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := tunnel.Get(ctx, "/queue/main.text")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
