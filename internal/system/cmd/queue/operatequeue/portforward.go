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
	"net/http"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// remoteErrorWait bounds the wait for the kubelet's explanation after a request fails. The kubelet
// reports a refused connection at once; the CRI gives a half-closed forward one second to drain.
const remoteErrorWait = 2 * time.Second

// debugTunnel sends HTTP requests to the controller's debug server through the pods/portforward
// subresource of the Deckhouse leader pod. The server listens on loopback only, so pods/proxy
// cannot reach it, while port-forwarding dials localhost inside the pod's network namespace.
type debugTunnel struct {
	dial func() (httpstream.Connection, error)

	conn      httpstream.Connection
	requestID int
}

func newDebugTunnel(config *rest.Config, kubeCl kubernetes.Interface) *debugTunnel {
	return &debugTunnel{
		dial: func() (httpstream.Connection, error) {
			return dialLeaderPod(config, kubeCl)
		},
	}
}

// Get returns the body of a GET request for path. The connection is opened on first use and
// dropped on any failure, so the next call looks up the leader pod again: a watch keeps working
// across leader changes and pod restarts.
func (t *debugTunnel) Get(ctx context.Context, path string) (string, error) {
	if t.conn != nil {
		select {
		case <-t.conn.CloseChan():
			t.Close()
		default:
		}
	}

	if t.conn == nil {
		conn, err := t.dial()
		if err != nil {
			return "", err
		}

		t.conn = conn
	}

	body, err := t.get(ctx, path)
	if err != nil {
		t.Close()

		return "", err
	}

	return body, nil
}

// Close releases the port-forward connection.
func (t *debugTunnel) Close() {
	if t.conn == nil {
		return
	}

	_ = t.conn.Close()
	t.conn = nil
}

// get sends one request over a fresh pair of port-forward streams that share a request ID.
// The data stream carries the HTTP exchange; the error stream carries the kubelet's reason
// when it cannot connect to the port inside the pod.
func (t *debugTunnel) get(ctx context.Context, path string) (string, error) {
	t.requestID++

	headers := http.Header{}
	headers.Set(corev1.StreamType, corev1.StreamTypeError)
	headers.Set(corev1.PortHeader, strconv.Itoa(debugServerPort))
	headers.Set(corev1.PortForwardRequestIDHeader, strconv.Itoa(t.requestID))

	errorStream, err := t.conn.CreateStream(headers)
	if err != nil {
		return "", fmt.Errorf("create port-forward error stream: %w", err)
	}
	defer t.conn.RemoveStreams(errorStream)

	// The error stream is only read from.
	_ = errorStream.Close()

	remoteErr := make(chan error, 1)

	go func() {
		message, err := io.ReadAll(errorStream)

		switch {
		case err != nil:
			remoteErr <- fmt.Errorf("read port-forward error stream: %w", err)
		case len(message) > 0:
			remoteErr <- errors.New(string(message))
		default:
			remoteErr <- nil
		}
	}()

	headers.Set(corev1.StreamType, corev1.StreamTypeData)

	dataStream, err := t.conn.CreateStream(headers)
	if err != nil {
		return "", fmt.Errorf("create port-forward data stream: %w", err)
	}
	defer t.conn.RemoveStreams(dataStream)

	// Resetting the stream unblocks the exchange when ctx is cancelled and, once it is over, lets
	// the kubelet drop the forward at once instead of waiting for this side to close.
	defer func() { _ = dataStream.Reset() }()

	stopReset := context.AfterFunc(ctx, func() { _ = dataStream.Reset() })
	defer stopReset()

	body, err := roundTrip(ctx, dataStream, path)
	if err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("GET %s: %w", path, ctx.Err())
		}

		select {
		case reason := <-remoteErr:
			if reason != nil {
				return "", reason
			}
		case <-time.After(remoteErrorWait):
		case <-ctx.Done():
		}

		return "", err
	}

	return body, nil
}

// roundTrip performs a single GET over stream, which acts as one TCP connection to the server.
func roundTrip(ctx context.Context, stream io.ReadWriter, path string) (string, error) {
	url := fmt.Sprintf("http://%s:%d%s", debugServerHost, debugServerPort, path)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}

	// One request per stream: the server closes its end after answering.
	req.Close = true

	if err := req.Write(stream); err != nil {
		return "", fmt.Errorf("send request: %w", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(stream), req)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: %s: %s", path, resp.Status, strings.TrimSpace(string(body)))
	}

	return string(body), nil
}

// dialLeaderPod opens a port-forward connection to the Deckhouse leader pod. Like kubectl, it
// tunnels over WebSockets and falls back to SPDY when the API server cannot upgrade to them.
func dialLeaderPod(config *rest.Config, kubeCl kubernetes.Interface) (httpstream.Connection, error) {
	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	if err != nil {
		return nil, err
	}

	portForwardURL := kubeCl.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("portforward").
		URL()

	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, fmt.Errorf("create SPDY transport: %w", err)
	}

	websocketDialer, err := portforward.NewSPDYOverWebsocketDialer(portForwardURL, config)
	if err != nil {
		return nil, fmt.Errorf("create WebSocket dialer: %w", err)
	}

	dialer := portforward.NewFallbackDialer(
		websocketDialer,
		spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, portForwardURL),
		func(err error) bool {
			return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
		},
	)

	conn, protocol, err := dialer.Dial(portforward.PortForwardProtocolV1Name)
	if err != nil {
		return nil, fmt.Errorf("port-forward to pod %s/%s: %w", namespace, podName, err)
	}

	if protocol != portforward.PortForwardProtocolV1Name {
		_ = conn.Close()

		return nil, fmt.Errorf("port-forward to pod %s/%s: server negotiated protocol %q, want %q",
			namespace, podName, protocol, portforward.PortForwardProtocolV1Name)
	}

	return conn, nil
}
