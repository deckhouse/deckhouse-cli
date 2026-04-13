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

	"github.com/deckhouse/deckhouse-cli/internal/mirror/errmatch"
	"github.com/deckhouse/deckhouse-cli/pkg/diagnostic"
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
	categoryDiskFull      = "Disk space exhausted"
	categoryPermission    = "Permission denied"
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
			Suggestions: []diagnostic.Suggestion{
				{
					Cause: "Large layer transfer interrupted by an intermediate timeout",
					Solutions: []string{
						"Increase the timeout with D8_MIRROR_TIMEOUT env variable, e.g.: export D8_MIRROR_TIMEOUT=4h",
						"Pull supports resuming — simply re-run the same command to continue from where it stopped",
					},
				},
				{
					Cause: "Corporate proxy or middleware intercepting and terminating HTTPS connections",
					Solutions: []string{
						"Check if a corporate proxy is intercepting HTTPS traffic",
						"If using a proxy, ensure it is configured to pass through registry traffic",
						"Try connecting directly without proxy: unset HTTP_PROXY HTTPS_PROXY",
					},
				},
				{Cause: "Source registry closed the connection unexpectedly"},
				{Cause: "Network device (firewall, load balancer) dropping packets"},
			},
		}

	case isCertificateError(err):
		return &diagnostic.HelpfulError{
			Category:    categoryTLS,
			OriginalErr: err,
			Suggestions: []diagnostic.Suggestion{
				{
					Cause: "Self-signed or private CA certificate on the source registry",
					Solutions: []string{
						"Add the source registry's CA certificate to your system trust store",
						"Use --tls-skip-verify flag to skip TLS verification (not recommended for production)",
					},
				},
				{
					Cause: "Certificate expired or not yet valid",
					Solutions: []string{
						"Verify system clock is correct (wrong time is a common cause of certificate errors)",
						"Renew the registry certificate if it has expired",
					},
				},
				{
					Cause:     "Source registry is serving plain HTTP, not HTTPS",
					Solutions: []string{"Use --insecure flag if the source registry uses HTTP instead of HTTPS"},
				},
				{
					Cause: "Corporate proxy or middleware intercepting and replacing TLS certificates",
					Solutions: []string{
						"Add the proxy's CA certificate to your system trust store",
						"Check if a corporate proxy is intercepting HTTPS traffic",
					},
				},
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
			Suggestions: []diagnostic.Suggestion{
				{
					Cause:     "License key is invalid, expired, or not provided",
					Solutions: []string{"Verify your license key and pass it with --license flag"},
				},
				{
					Cause:     "Source registry credentials are incorrect",
					Solutions: []string{"For custom source registries, use --source-login and --source-password"},
				},
				{
					Cause:     "Insufficient permissions for the requested images",
					Solutions: []string{"Contact registry administrator to verify access rights"},
				},
			},
		}

	case isRateLimitError(err):
		return &diagnostic.HelpfulError{
			Category:    categoryRateLimit,
			OriginalErr: err,
			Suggestions: []diagnostic.Suggestion{
				{
					Cause:     "Too many requests to the source registry in a short time",
					Solutions: []string{"Wait a few minutes and retry the operation"},
				},
				{
					Cause:     "Registry-side rate limiting policy",
					Solutions: []string{"Contact registry administrator to increase rate limits"},
				},
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
			Suggestions: []diagnostic.Suggestion{
				{
					Cause:     "Source registry is experiencing internal errors",
					Solutions: []string{"Wait a few minutes and retry the operation"},
				},
				{
					Cause:     "Backend storage is temporarily unavailable",
					Solutions: []string{"Check source registry status and health"},
				},
				{
					Cause:     "Registry is overloaded or being maintained",
					Solutions: []string{"Contact registry administrator if the problem persists"},
				},
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
			Suggestions: []diagnostic.Suggestion{
				{
					Cause:     "Incorrect source registry URL or typo in hostname",
					Solutions: []string{"Verify the --source registry URL is spelled correctly"},
				},
				{
					Cause:     "DNS server is unreachable or not responding",
					Solutions: []string{"Check your DNS server configuration"},
				},
				{
					Cause:     "Source registry hostname cannot be resolved by DNS",
					Solutions: []string{"Try using the registry's IP address instead of hostname"},
				},
			},
		}

	case isTimeoutError(err):
		return &diagnostic.HelpfulError{
			Category:    categoryTimeout,
			OriginalErr: err,
			Suggestions: []diagnostic.Suggestion{
				{
					Cause: "Large image layers require more time to transfer than the default timeout",
					Solutions: []string{
						"Increase the timeout with D8_MIRROR_TIMEOUT env variable, e.g.: export D8_MIRROR_TIMEOUT=4h",
						"Pull supports resuming — re-run the same command to continue from where it stopped",
					},
				},
				{
					Cause:     "Firewall silently dropping packets (no RST, no ICMP)",
					Solutions: []string{"Verify firewall rules allow outbound HTTPS (port 443) to the source registry"},
				},
				{
					Cause:     "Source registry took too long to respond",
					Solutions: []string{"Check network connectivity and latency to the source registry"},
				},
			},
		}

	case isDiskFullError(err):
		return &diagnostic.HelpfulError{
			Category:    categoryDiskFull,
			OriginalErr: err,
			Suggestions: []diagnostic.Suggestion{
				{
					Cause: "Not enough free disk space for the bundle output",
					Solutions: []string{
						"Free up disk space on the output partition",
						"Use --images-bundle-chunk-size to split the bundle into smaller chunks",
					},
				},
				{
					Cause: "Temporary working directory is on a partition with insufficient space",
					Solutions: []string{
						"Mirror operations require free space approximately equal to the full bundle size",
						"Use --tmp-dir to point to a partition with more free space",
					},
				},
			},
		}

	case isPermissionError(err):
		return &diagnostic.HelpfulError{
			Category:    categoryPermission,
			OriginalErr: err,
			Suggestions: []diagnostic.Suggestion{
				{
					Cause: "Output directory is not writable by the current user",
					Solutions: []string{
						"Check write permissions on the output path",
						"Run with a user that has write access, or change directory permissions",
					},
				},
				{
					Cause:     "Temporary directory is not writable",
					Solutions: []string{"Use --tmp-dir to specify a writable temporary directory"},
				},
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
			Suggestions: []diagnostic.Suggestion{
				{
					Cause:     "Source registry is serving plain HTTP, not HTTPS",
					Solutions: []string{"Use --insecure flag if the source registry uses HTTP instead of HTTPS"},
				},
				{
					Cause: "Firewall or security group blocking outbound connections",
					Solutions: []string{
						"Verify firewall rules allow outbound HTTPS (port 443) to the source registry",
						"Test connectivity with: curl -v https://<source-registry>",
					},
				},
				{
					Cause:     "Source registry is down or temporarily unreachable",
					Solutions: []string{"Check your network connection and the source registry status"},
				},
			},
		}

	case errmatch.IsImageNotFound(err):
		return &diagnostic.HelpfulError{
			Category:    categoryImageNotFound,
			OriginalErr: err,
			Suggestions: []diagnostic.Suggestion{
				{
					Cause: "Requested Deckhouse version or tag does not exist in the source registry",
					Solutions: []string{
						"Check --deckhouse-tag or --since-version value for typos or non-existent versions",
						"Browse available release versions in the source registry",
					},
				},
				{
					Cause:     "License key does not have access to the requested edition or version",
					Solutions: []string{"Verify the --license key grants access to the requested Deckhouse edition"},
				},
			},
		}

	case errmatch.IsRepoNotFound(err):
		return &diagnostic.HelpfulError{
			Category:    categoryRepoNotFound,
			OriginalErr: err,
			Suggestions: []diagnostic.Suggestion{
				{
					Cause: "Source registry path is incorrect or the repository does not exist",
					Solutions: []string{
						"Double-check the --source flag value for typos or extra path segments",
						"Default source is registry.deckhouse.ru/deckhouse/ee",
					},
				},
				{
					Cause: "Account does not have read access to the repository",
					Solutions: []string{
						"Verify the --license key or --source-login credentials have read permissions",
						"Contact registry administrator to confirm your access rights",
					},
				},
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

func isDiskFullError(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}

func isPermissionError(err error) bool {
	return errors.Is(err, os.ErrPermission) ||
		errors.Is(err, syscall.EACCES) ||
		errors.Is(err, syscall.EPERM)
}
