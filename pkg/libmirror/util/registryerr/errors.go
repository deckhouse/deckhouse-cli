/*
Copyright 2024 Flant JSC

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
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

const TrivyMediaTypesWarning = `` +
	"It looks like you are using Project Quay registry and it is not configured correctly for hosting Deckhouse.\n" +
	"See the docs at https://deckhouse.io/products/kubernetes-platform/documentation/v1/supported_versions.html#container-registry for more details.\n\n" +
	"TL;DR: You should retry push after allowing some additional types of OCI artifacts in your config.yaml as follows:\n" +
	`FEATURE_GENERAL_OCI_SUPPORT: true
ALLOWED_OCI_ARTIFACT_TYPES:
  "application/octet-stream":
    - "application/deckhouse.io.bdu.layer.v1.tar+gzip"
    - "application/vnd.cncf.openpolicyagent.layer.v1.tar+gzip"
  "application/vnd.aquasec.trivy.config.v1+json":
    - "application/vnd.aquasec.trivy.javadb.layer.v1.tar+gzip"
    - "application/vnd.aquasec.trivy.db.layer.v1.tar+gzip"`

func IsImageNotFound(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "MANIFEST_UNKNOWN") || strings.Contains(errMsg, "404 Not Found")
}

func IsRepoNotFound(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "NAME_UNKNOWN")
}

func IsTrivyMediaTypeNotAllowed(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "MANIFEST_INVALID") &&
		(strings.Contains(errMsg, "vnd.aquasec.trivy") || strings.Contains(errMsg, "application/octet-stream"))
}

// Error category names displayed to the user after "error: ".
// Some categories are extended with dynamic details (hostname, port, HTTP code)
// at classification time via fmt.Sprintf.
const (
	CategoryEOF            = "Connection terminated unexpectedly (EOF)"
	CategoryTLS            = "TLS/certificate verification failed"
	CategoryAuth           = "Authentication failed"
	CategoryAuth401        = "Authentication failed (HTTP 401 Unauthorized)"
	CategoryAuth403        = "Access denied (HTTP 403 Forbidden)"
	CategoryRateLimit      = "Rate limited by registry (HTTP 429 Too Many Requests)"
	CategoryServerError    = "Registry server error"
	CategoryDNS            = "DNS resolution failed"
	CategoryTimeout        = "Operation timed out"
	CategoryNetwork        = "Network connection failed"
	CategoryImageNotFound  = "Image not found in registry"
	CategoryRepoNotFound   = "Repository not found in registry"
	CategoryUnsupportedOCI = "Unsupported OCI artifact type"
)

// Classify analyzes an error and returns a Diagnostic if
// the error can be classified into a known category, or nil otherwise.
// Detection order matters: more specific checks come before general ones.
func Classify(err error) *Diagnostic {
	if err == nil {
		return nil
	}

	switch {
	case isEOFError(err):
		return &Diagnostic{
			Category:    CategoryEOF,
			OriginalErr: err,
			Causes: []string{
				"Corporate proxy or middleware intercepting and terminating HTTPS connections",
				"Registry server closed the connection unexpectedly",
				"Network device (firewall, load balancer) dropping packets",
			},
			Solutions: []string{
				"Check if a corporate proxy is intercepting HTTPS traffic",
				"If using a proxy, ensure it is configured to pass through registry traffic",
				"Use --tls-skip-verify flag if a proxy is replacing TLS certificates",
				"Try connecting directly without proxy: unset HTTP_PROXY HTTPS_PROXY",
			},
		}

	case isCertificateError(err):
		return &Diagnostic{
			Category:    CategoryTLS,
			OriginalErr: err,
			Causes: []string{
				"Self-signed certificate without proper trust chain",
				"Certificate expired or not yet valid",
				"Hostname mismatch between certificate and registry URL",
				"Corporate proxy or middleware intercepting HTTPS connections",
			},
			Solutions: []string{
				"Use --tls-skip-verify flag to skip TLS verification (not recommended for production)",
				"Add the registry's CA certificate to your system trust store",
				"Verify the registry URL hostname matches the certificate",
				"Verify system clock is correct (expired certificates can be caused by wrong time)",
			},
		}

	case isAuthenticationError(err):
		var transportErr *transport.Error
		category := CategoryAuth
		if errors.As(err, &transportErr) {
			switch transportErr.StatusCode {
			case http.StatusUnauthorized:
				category = CategoryAuth401
			case http.StatusForbidden:
				category = CategoryAuth403
			}
		}

		return &Diagnostic{
			Category:    category,
			OriginalErr: err,
			Causes: []string{
				"Invalid or expired credentials",
				"License key or registry credentials are incorrect or not provided",
				"Insufficient permissions for the requested operation",
			},
			Solutions: []string{
				"For pull: verify your license key and pass it with --license flag",
				"For push: verify --registry-login and --registry-password are correct",
				"Contact registry administrator to verify access rights",
			},
		}

	case isRateLimitError(err):
		return &Diagnostic{
			Category:    CategoryRateLimit,
			OriginalErr: err,
			Causes: []string{
				"Too many requests to the registry in a short time",
				"Registry-side rate limiting policy",
			},
			Solutions: []string{
				"Wait a few minutes and retry the operation",
				"Contact registry administrator to increase rate limits",
			},
		}

	case isServerError(err):
		var transportErr *transport.Error
		category := CategoryServerError
		if errors.As(err, &transportErr) {
			category = fmt.Sprintf("%s (HTTP %d)", CategoryServerError, transportErr.StatusCode)
		}

		return &Diagnostic{
			Category:    category,
			OriginalErr: err,
			Causes: []string{
				"Registry server is experiencing internal errors",
				"Backend storage is temporarily unavailable",
				"Registry is overloaded or being maintained",
			},
			Solutions: []string{
				"Wait a few minutes and retry the operation",
				"Check registry server status and health",
				"Contact registry administrator if the problem persists",
			},
		}

	case isDNSError(err):
		var dnsErr *net.DNSError
		category := CategoryDNS
		if errors.As(err, &dnsErr) && dnsErr.Name != "" {
			category = fmt.Sprintf("%s for '%s'", CategoryDNS, dnsErr.Name)
		}

		return &Diagnostic{
			Category:    category,
			OriginalErr: err,
			Causes: []string{
				"Registry hostname cannot be resolved by DNS",
				"DNS server is unreachable or not responding",
				"Incorrect registry URL or typo in hostname",
			},
			Solutions: []string{
				"Verify the registry URL is spelled correctly",
				"Check your DNS server configuration",
				"Try using the registry's IP address instead of hostname",
			},
		}

	case isTimeoutError(err):
		return &Diagnostic{
			Category:    CategoryTimeout,
			OriginalErr: err,
			Causes: []string{
				"Registry server took too long to respond",
				"Network latency is too high",
				"Firewall silently dropping packets (no RST, no ICMP)",
			},
			Solutions: []string{
				"Check network connectivity to the registry",
				"Try increasing the timeout with --timeout flag",
				"Verify firewall rules allow outbound HTTPS (port 443)",
			},
		}

	case isNetworkError(err):
		var opErr *net.OpError
		category := CategoryNetwork
		if errors.As(err, &opErr) && opErr.Addr != nil {
			category = fmt.Sprintf("%s to %s", CategoryNetwork, opErr.Addr.String())
		}

		return &Diagnostic{
			Category:    category,
			OriginalErr: err,
			Causes: []string{
				"Network connectivity issues or no internet connection",
				"Firewall or security group blocking the connection",
				"Registry server is down or unreachable",
			},
			Solutions: []string{
				"Check your network connection and internet access",
				"Verify firewall rules allow outbound HTTPS (port 443)",
				"Test connectivity with: curl -v https://<registry>",
			},
		}

	case IsImageNotFound(err):
		return &Diagnostic{
			Category:    CategoryImageNotFound,
			OriginalErr: err,
			Causes: []string{
				"Image tag doesn't exist in the registry",
				"Incorrect image name or tag specified",
			},
			Solutions: []string{
				"Verify the image name and tag are correct",
				"Check if you have permission to access this image",
			},
		}

	case IsRepoNotFound(err):
		return &Diagnostic{
			Category:    CategoryRepoNotFound,
			OriginalErr: err,
			Causes: []string{
				"Repository doesn't exist in the registry",
				"Incorrect repository path or name",
			},
			Solutions: []string{
				"Verify the repository path is correct",
				"Ensure you have permission to access this repository",
			},
		}

	case IsTrivyMediaTypeNotAllowed(err):
		return &Diagnostic{
			Category:    CategoryUnsupportedOCI,
			OriginalErr: err,
			Causes: []string{
				"Registry doesn't support required media types for Trivy security databases",
				"Project Quay registry not configured for Deckhouse artifacts",
			},
			Solutions: []string{
				"Configure registry to allow custom OCI artifact types",
				"See: https://deckhouse.io/products/kubernetes-platform/documentation/v1/supported_versions.html#container-registry",
			},
		}
	}

	return nil
}

func isEOFError(err error) bool {
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

func isDNSError(err error) bool {
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr)
}

func isTimeoutError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)
}

func isNetworkError(err error) bool {
	// DNS and timeout are checked before this, so we skip them here
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
