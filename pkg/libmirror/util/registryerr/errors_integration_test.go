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

package registryerr

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// headImage performs remote.Head against the given host with insecure (plain HTTP) mode.
func headImage(ctx context.Context, host string) error {
	ref, err := name.ParseReference(host+"/test:latest", name.Insecure)
	if err != nil {
		return fmt.Errorf("parse reference: %w", err)
	}
	_, err = remote.Head(ref, remote.WithContext(ctx))
	return err
}

// newRegistryErrorHandler returns an http.Handler that responds with
// a Docker V2 API error in JSON format.
func newRegistryErrorHandler(statusCode int, code, message string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(struct {
			Errors []struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"errors"`
		}{
			Errors: []struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}{{code, message}},
		})
	})
}

// classifyFromServer starts an httptest server with the given handler,
// makes a real remote.Head call, and returns the Classify result.
func classifyFromServer(t *testing.T, handler http.Handler) *Diagnostic {
	t.Helper()
	server := httptest.NewServer(handler)
	defer server.Close()

	err := headImage(context.Background(), strings.TrimPrefix(server.URL, "http://"))
	require.Error(t, err)
	return Classify(err)
}

// --- HTTP Status Code Tests ---
// Verify that real go-containerregistry errors from HTTP responses
// are correctly classified using the Category constants.

func TestIntegration_Auth401(t *testing.T) {
	diag := classifyFromServer(t, newRegistryErrorHandler(
		http.StatusUnauthorized, "UNAUTHORIZED", "authentication required",
	))
	require.NotNil(t, diag)
	assert.Equal(t, CategoryAuth401, diag.Category)
}

func TestIntegration_Auth403(t *testing.T) {
	diag := classifyFromServer(t, newRegistryErrorHandler(
		http.StatusForbidden, "DENIED", "access denied",
	))
	require.NotNil(t, diag)
	assert.Equal(t, CategoryAuth403, diag.Category)
}

func TestIntegration_RateLimit429(t *testing.T) {
	diag := classifyFromServer(t, newRegistryErrorHandler(
		http.StatusTooManyRequests, "TOOMANYREQUESTS", "rate limit exceeded",
	))
	require.NotNil(t, diag)
	assert.Equal(t, CategoryRateLimit, diag.Category)
}

func TestIntegration_ServerErrors(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		errCode    string
	}{
		{"500", http.StatusInternalServerError, "UNKNOWN"},
		{"502", http.StatusBadGateway, ""},
		{"503", http.StatusServiceUnavailable, "UNAVAILABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.errCode != "" {
					newRegistryErrorHandler(tt.statusCode, tt.errCode, "server error").ServeHTTP(w, nil)
				} else {
					w.WriteHeader(tt.statusCode)
				}
			})

			diag := classifyFromServer(t, handler)
			require.NotNil(t, diag)
			// Category is dynamic: "Registry server error (HTTP 500)"
			assert.Contains(t, diag.Category, CategoryServerError)
			assert.Contains(t, diag.Category, tt.name)
		})
	}
}

// --- TLS Error Test ---

func TestIntegration_TLSCertificateError(t *testing.T) {
	// httptest.NewTLSServer uses a self-signed cert not in the system trust store.
	// Connecting without TLS skip produces an x509 certificate error.
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "https://")
	ref, err := name.ParseReference(host+"/test:latest", name.StrictValidation)
	require.NoError(t, err)

	_, err = remote.Head(ref)
	require.Error(t, err)

	diag := Classify(err)
	require.NotNil(t, diag, "expected TLS error to be classified, got raw: %v", err)
	assert.Equal(t, CategoryTLS, diag.Category)
}

// --- EOF Error Test ---

func TestIntegration_EOF(t *testing.T) {
	// Server accepts the TCP connection then closes it immediately,
	// producing an EOF on the client side.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("server does not support hijacking")
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Fatalf("hijack failed: %v", err)
		}
		conn.Close()
	}))
	defer server.Close()

	err := headImage(context.Background(), strings.TrimPrefix(server.URL, "http://"))
	require.Error(t, err)

	diag := Classify(err)
	require.NotNil(t, diag, "expected EOF error to be classified, got raw: %v", err)
	assert.Equal(t, CategoryEOF, diag.Category)
}

// --- Connection Refused Test ---

func TestIntegration_ConnectionRefused(t *testing.T) {
	// Bind a port, then close the listener so nothing accepts connections.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = headImage(ctx, addr)
	require.Error(t, err)

	diag := Classify(err)
	require.NotNil(t, diag, "expected network error to be classified, got raw: %v", err)
	// Category is dynamic: "Network connection failed to 127.0.0.1:XXXXX"
	assert.Contains(t, diag.Category, CategoryNetwork)
}

// --- Timeout Test ---

func TestIntegration_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(10 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := headImage(ctx, strings.TrimPrefix(server.URL, "http://"))
	require.Error(t, err)

	diag := Classify(err)
	require.NotNil(t, diag, "expected timeout error to be classified, got raw: %v", err)
	assert.Equal(t, CategoryTimeout, diag.Category)
}

// --- DNS Error Test ---

func TestIntegration_DNSResolutionFailure(t *testing.T) {
	// .invalid TLD is reserved by RFC 2606 and guaranteed to never resolve.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := headImage(ctx, "nonexistent.invalid:443")
	require.Error(t, err)

	diag := Classify(err)
	require.NotNil(t, diag, "expected DNS error to be classified, got raw: %v", err)
	// Category is dynamic: "DNS resolution failed for 'nonexistent.invalid'"
	assert.Contains(t, diag.Category, CategoryDNS)
}
