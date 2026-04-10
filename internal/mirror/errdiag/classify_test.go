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

package errdiag

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"syscall"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassify_Nil(t *testing.T) {
	assert.Nil(t, Classify(nil))
}

func TestClassify_Unclassified(t *testing.T) {
	assert.Nil(t, Classify(errors.New("some random error")))
}

func TestClassify_EOF(t *testing.T) {
	diag := Classify(io.EOF)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryEOF, diag.Category)
}

func TestClassify_UnexpectedEOF(t *testing.T) {
	diag := Classify(io.ErrUnexpectedEOF)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryEOF, diag.Category)
}

func TestClassify_WrappedEOF(t *testing.T) {
	err := fmt.Errorf("pull from registry: %w", fmt.Errorf("get manifest: %w", io.EOF))
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryEOF, diag.Category)
}

func TestClassify_TLS_UnknownAuthority(t *testing.T) {
	err := fmt.Errorf("registry: %w", x509.UnknownAuthorityError{})
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryTLS, diag.Category)
}

func TestClassify_TLS_CertificateInvalid(t *testing.T) {
	err := fmt.Errorf("registry: %w", x509.CertificateInvalidError{})
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryTLS, diag.Category)
}

func TestClassify_TLS_Hostname(t *testing.T) {
	err := fmt.Errorf("registry: %w", x509.HostnameError{Host: "example.com"})
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryTLS, diag.Category)
}

func TestClassify_Auth_401(t *testing.T) {
	diag := Classify(&transport.Error{StatusCode: http.StatusUnauthorized})
	require.NotNil(t, diag)
	assert.Equal(t, CategoryAuth401, diag.Category)
}

func TestClassify_Auth_403(t *testing.T) {
	diag := Classify(&transport.Error{StatusCode: http.StatusForbidden})
	require.NotNil(t, diag)
	assert.Equal(t, CategoryAuth403, diag.Category)
}

func TestClassify_Auth_DiagnosticCode(t *testing.T) {
	err := &transport.Error{
		StatusCode: http.StatusOK,
		Errors:     []transport.Diagnostic{{Code: transport.UnauthorizedErrorCode}},
	}
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryAuth, diag.Category)
}

func TestClassify_RateLimit_429(t *testing.T) {
	diag := Classify(&transport.Error{StatusCode: http.StatusTooManyRequests})
	require.NotNil(t, diag)
	assert.Equal(t, CategoryRateLimit, diag.Category)
}

func TestClassify_RateLimit_DiagnosticCode(t *testing.T) {
	err := &transport.Error{
		StatusCode: http.StatusOK,
		Errors:     []transport.Diagnostic{{Code: transport.TooManyRequestsErrorCode}},
	}
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryRateLimit, diag.Category)
}

func TestClassify_ServerErrors(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
	}{
		{"500", http.StatusInternalServerError},
		{"502", http.StatusBadGateway},
		{"503", http.StatusServiceUnavailable},
		{"504", http.StatusGatewayTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diag := Classify(&transport.Error{StatusCode: tt.statusCode})
			require.NotNil(t, diag)
			assert.Contains(t, diag.Category, CategoryServerError)
			assert.Contains(t, diag.Category, tt.name)
		})
	}
}

func TestClassify_ServerError_Unavailable(t *testing.T) {
	err := &transport.Error{
		StatusCode: http.StatusOK,
		Errors:     []transport.Diagnostic{{Code: transport.UnavailableErrorCode}},
	}
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Contains(t, diag.Category, CategoryServerError)
}

func TestClassify_DNS(t *testing.T) {
	err := &net.DNSError{Name: "registry.example.com", Err: "no such host"}
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Contains(t, diag.Category, CategoryDNS)
	assert.Contains(t, diag.Category, "registry.example.com")
}

func TestClassify_Timeout_Context(t *testing.T) {
	err := fmt.Errorf("validate access: %w", context.DeadlineExceeded)
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryTimeout, diag.Category)
}

func TestClassify_Timeout_OS(t *testing.T) {
	err := fmt.Errorf("validate access: %w", os.ErrDeadlineExceeded)
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryTimeout, diag.Category)
}

func TestClassify_Network_ConnRefused(t *testing.T) {
	err := &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443},
		Err:  &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
	}
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Contains(t, diag.Category, CategoryNetwork)
	assert.Contains(t, diag.Category, "127.0.0.1:443")
}

func TestClassify_Network_ConnReset(t *testing.T) {
	err := &net.OpError{
		Op: "read", Net: "tcp",
		Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET},
	}
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryNetwork, diag.Category)
}

func TestClassify_ImageNotFound(t *testing.T) {
	diag := Classify(errors.New("MANIFEST_UNKNOWN: manifest unknown"))
	require.NotNil(t, diag)
	assert.Equal(t, CategoryImageNotFound, diag.Category)
}

func TestClassify_RepoNotFound(t *testing.T) {
	diag := Classify(errors.New("NAME_UNKNOWN: repository not found"))
	require.NotNil(t, diag)
	assert.Equal(t, CategoryRepoNotFound, diag.Category)
}

func TestClassify_UnsupportedOCIMediaType(t *testing.T) {
	diag := Classify(errors.New("MANIFEST_INVALID: media type vnd.aquasec.trivy not allowed"))
	require.NotNil(t, diag)
	assert.Equal(t, CategoryUnsupportedOCI, diag.Category)
}

func TestClassify_DeepWrapping(t *testing.T) {
	inner := x509.UnknownAuthorityError{}
	err := fmt.Errorf("l1: %w", fmt.Errorf("l2: %w", fmt.Errorf("l3: %w", fmt.Errorf("l4: %w", fmt.Errorf("l5: %w", inner)))))
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryTLS, diag.Category)
}

func TestClassify_Unwrap(t *testing.T) {
	originalErr := io.EOF
	diag := Classify(fmt.Errorf("wrap: %w", originalErr))
	require.NotNil(t, diag)
	assert.True(t, errors.Is(diag, originalErr))
}

func TestClassify_Priority_DNSOverNetwork(t *testing.T) {
	err := &net.DNSError{Name: "example.com", Err: "no such host"}
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Contains(t, diag.Category, CategoryDNS)
	assert.NotContains(t, diag.Category, CategoryNetwork)
}

func TestClassify_Priority_TimeoutOverNetwork(t *testing.T) {
	diag := Classify(context.DeadlineExceeded)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryTimeout, diag.Category)
}

func TestClassify_WrappedAuth(t *testing.T) {
	inner := &transport.Error{StatusCode: http.StatusUnauthorized}
	err := fmt.Errorf("validate access: %w", inner)
	diag := Classify(err)
	require.NotNil(t, diag)
	assert.Equal(t, CategoryAuth401, diag.Category)
}

func TestClassify_AlreadyClassified(t *testing.T) {
	first := Classify(io.EOF)
	require.NotNil(t, first)

	second := Classify(first)
	assert.Nil(t, second, "must not wrap an already classified error")
}

func TestIsUnsupportedOCIMediaType(t *testing.T) {
	assert.True(t, isUnsupportedOCIMediaType(errors.New("MANIFEST_INVALID: vnd.aquasec.trivy")))
	assert.True(t, isUnsupportedOCIMediaType(errors.New("MANIFEST_INVALID: application/octet-stream")))
	assert.True(t, isUnsupportedOCIMediaType(errors.New("MANIFEST_INVALID: deckhouse.io.bdu.layer")))
	assert.True(t, isUnsupportedOCIMediaType(errors.New("MANIFEST_INVALID: vnd.cncf.openpolicyagent")))
	assert.False(t, isUnsupportedOCIMediaType(errors.New("MANIFEST_INVALID: other")))
	assert.False(t, isUnsupportedOCIMediaType(errors.New("some error without manifest invalid")))
	assert.False(t, isUnsupportedOCIMediaType(nil))
}
