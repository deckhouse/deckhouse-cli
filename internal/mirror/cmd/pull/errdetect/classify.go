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

// Package errdetect classifies registry errors for d8 mirror pull
// with pull-specific causes and solutions.
package errdetect

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

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"github.com/deckhouse/deckhouse-cli/pkg/diagnostic"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/errmatch"
)

const (
	categoryEOF           = "Connection terminated unexpectedly (EOF)"
	categoryTLS           = "TLS/certificate verification failed"
	categoryAuth          = "Authentication failed"
	categoryAuth401       = "Authentication failed (HTTP 401 Unauthorized)"
	categoryAuth403       = "Access denied (HTTP 403 Forbidden)"
	categoryRateLimit     = "Rate limited by registry (HTTP 429 Too Many Requests)"
	categoryServerError   = "Registry server error"
	categoryDNS           = "DNS resolution failed"
	categoryTimeout       = "Operation timed out"
	categoryNetwork       = "Network connection failed"
	categoryImageNotFound = "Image not found in registry"
	categoryRepoNotFound  = "Repository not found in registry"
)

// Diagnose analyzes an error and returns a *diagnostic.HelpfulError
// with pull-specific causes and solutions, or nil if the error is not recognized.
func Diagnose(err error) *diagnostic.HelpfulError {
	if err == nil {
		return nil
	}

	var helpErr *diagnostic.HelpfulError
	if errors.As(err, &helpErr) {
		return nil
	}

	switch {
	case isEOF(err):
		return &diagnostic.HelpfulError{
			Category:    categoryEOF,
			OriginalErr: err,
			Causes: []string{
				"Corporate proxy or middleware intercepting and terminating HTTPS connections",
				"Source registry closed the connection unexpectedly",
				"Network device (firewall, load balancer) dropping packets",
			},
			Solutions: []string{
				"Check if a corporate proxy is intercepting HTTPS traffic",
				"If using a proxy, ensure it is configured to pass through registry traffic",
				"Try connecting directly without proxy: unset HTTP_PROXY HTTPS_PROXY",
			},
		}

	case isCertificateError(err):
		return &diagnostic.HelpfulError{
			Category:    categoryTLS,
			OriginalErr: err,
			Causes: []string{
				"Self-signed certificate on the source registry",
				"Certificate expired or not yet valid",
				"Corporate proxy or middleware intercepting HTTPS connections",
			},
			Solutions: []string{
				"Use --tls-skip-verify flag to skip TLS verification (not recommended for production)",
				"Add the source registry's CA certificate to your system trust store",
				"Verify system clock is correct (expired certificates can be caused by wrong time)",
			},
		}

	case isAuthenticationError(err):
		category := categoryAuth
		if code := authStatusCode(err); code == http.StatusUnauthorized {
			category = categoryAuth401
		} else if code == http.StatusForbidden {
			category = categoryAuth403
		}

		return &diagnostic.HelpfulError{
			Category:    category,
			OriginalErr: err,
			Causes: []string{
				"License key is invalid, expired, or not provided",
				"Source registry credentials are incorrect",
				"Insufficient permissions for the requested images",
			},
			Solutions: []string{
				"Verify your license key and pass it with --license flag",
				"For custom source registries, use --source-login and --source-password",
				"Contact registry administrator to verify access rights",
			},
		}

	case isRateLimitError(err):
		return &diagnostic.HelpfulError{
			Category:    categoryRateLimit,
			OriginalErr: err,
			Causes: []string{
				"Too many requests to the source registry in a short time",
				"Registry-side rate limiting policy",
			},
			Solutions: []string{
				"Wait a few minutes and retry the operation",
				"Contact registry administrator to increase rate limits",
			},
		}

	case isServerError(err):
		category := categoryServerError
		if code := serverStatusCode(err); code != 0 {
			category = fmt.Sprintf("%s (HTTP %d)", categoryServerError, code)
		}

		return &diagnostic.HelpfulError{
			Category:    category,
			OriginalErr: err,
			Causes: []string{
				"Source registry is experiencing internal errors",
				"Backend storage is temporarily unavailable",
				"Registry is overloaded or being maintained",
			},
			Solutions: []string{
				"Wait a few minutes and retry the operation",
				"Check source registry status and health",
				"Contact registry administrator if the problem persists",
			},
		}

	case isDNSError(err):
		category := categoryDNS
		if name := dnsHostname(err); name != "" {
			category = fmt.Sprintf("%s for '%s'", categoryDNS, name)
		}

		return &diagnostic.HelpfulError{
			Category:    category,
			OriginalErr: err,
			Causes: []string{
				"Source registry hostname cannot be resolved by DNS",
				"DNS server is unreachable or not responding",
				"Incorrect source registry URL or typo in hostname",
			},
			Solutions: []string{
				"Verify the --source registry URL is spelled correctly",
				"Check your DNS server configuration",
				"Try using the registry's IP address instead of hostname",
			},
		}

	case isTimeoutError(err):
		return &diagnostic.HelpfulError{
			Category:    categoryTimeout,
			OriginalErr: err,
			Causes: []string{
				"Source registry took too long to respond",
				"Network latency is too high",
				"Firewall silently dropping packets (no RST, no ICMP)",
			},
			Solutions: []string{
				"Check network connectivity to the source registry",
				"Verify firewall rules allow outbound HTTPS (port 443)",
			},
		}

	case isNetworkError(err):
		category := categoryNetwork
		if addr := networkAddr(err); addr != "" {
			category = fmt.Sprintf("%s to %s", categoryNetwork, addr)
		}

		return &diagnostic.HelpfulError{
			Category:    category,
			OriginalErr: err,
			Causes: []string{
				"No network connection to the source registry",
				"Firewall or security group blocking the connection",
				"Source registry is down or unreachable",
			},
			Solutions: []string{
				"Check your network connection and internet access",
				"Verify firewall rules allow outbound HTTPS (port 443)",
				"Test connectivity with: curl -v https://<source-registry>",
			},
		}

	case errmatch.IsImageNotFound(err):
		return &diagnostic.HelpfulError{
			Category:    categoryImageNotFound,
			OriginalErr: err,
			Causes: []string{
				"Image tag doesn't exist in the source registry",
				"Incorrect image name or tag specified",
			},
			Solutions: []string{
				"Verify the source registry path with --source flag",
				"Check if the requested Deckhouse version exists",
			},
		}

	case errmatch.IsRepoNotFound(err):
		return &diagnostic.HelpfulError{
			Category:    categoryRepoNotFound,
			OriginalErr: err,
			Causes: []string{
				"Repository doesn't exist in the source registry",
				"Incorrect source registry path",
			},
			Solutions: []string{
				"Verify the --source registry path is correct",
				"Ensure you have permission to access this repository",
			},
		}
	}

	return nil
}

// --- detection functions ---

func isEOF(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func isCertificateError(err error) bool {
	var (
		unknownAuthErr x509.UnknownAuthorityError
		certInvalidErr x509.CertificateInvalidError
		hostnameErr    x509.HostnameError
		systemRootsErr x509.SystemRootsError
		constraintErr  x509.ConstraintViolationError
		insecureAlgErr x509.InsecureAlgorithmError
	)

	return errors.As(err, &unknownAuthErr) ||
		errors.As(err, &certInvalidErr) ||
		errors.As(err, &hostnameErr) ||
		errors.As(err, &systemRootsErr) ||
		errors.As(err, &constraintErr) ||
		errors.As(err, &insecureAlgErr)
}

func isAuthenticationError(err error) bool {
	var transportErr *transport.Error
	if !errors.As(err, &transportErr) {
		return false
	}

	if transportErr.StatusCode == http.StatusUnauthorized || transportErr.StatusCode == http.StatusForbidden {
		return true
	}

	for _, diag := range transportErr.Errors {
		if diag.Code == transport.UnauthorizedErrorCode || diag.Code == transport.DeniedErrorCode {
			return true
		}
	}

	return false
}

func authStatusCode(err error) int {
	var transportErr *transport.Error
	if errors.As(err, &transportErr) {
		return transportErr.StatusCode
	}
	return 0
}

func isRateLimitError(err error) bool {
	var transportErr *transport.Error
	if !errors.As(err, &transportErr) {
		return false
	}

	if transportErr.StatusCode == http.StatusTooManyRequests {
		return true
	}

	for _, diag := range transportErr.Errors {
		if diag.Code == transport.TooManyRequestsErrorCode {
			return true
		}
	}

	return false
}

func isServerError(err error) bool {
	var transportErr *transport.Error
	if !errors.As(err, &transportErr) {
		return false
	}

	switch transportErr.StatusCode {
	case http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	}

	for _, diag := range transportErr.Errors {
		if diag.Code == transport.UnavailableErrorCode {
			return true
		}
	}

	return false
}

func serverStatusCode(err error) int {
	var transportErr *transport.Error
	if errors.As(err, &transportErr) {
		return transportErr.StatusCode
	}
	return 0
}

func isDNSError(err error) bool {
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr)
}

func dnsHostname(err error) string {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.Name
	}
	return ""
}

func isTimeoutError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)
}

func isNetworkError(err error) bool {
	if isDNSError(err) || isTimeoutError(err) {
		return false
	}

	var (
		netErr     net.Error
		opErr      *net.OpError
		syscallErr syscall.Errno
	)

	if errors.As(err, &opErr) {
		return true
	}

	if errors.As(err, &netErr) {
		return true
	}

	if errors.As(err, &syscallErr) {
		return syscallErr == syscall.ECONNREFUSED ||
			syscallErr == syscall.ECONNRESET ||
			syscallErr == syscall.ETIMEDOUT ||
			syscallErr == syscall.ENETUNREACH ||
			syscallErr == syscall.EHOSTUNREACH
	}

	return false
}

func networkAddr(err error) string {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Addr != nil {
		return opErr.Addr.String()
	}
	return ""
}
