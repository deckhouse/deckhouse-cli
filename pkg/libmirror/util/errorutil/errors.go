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

package errorutil

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"syscall"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

const CustomTrivyMediaTypesWarning = `` +
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

func IsImageNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "MANIFEST_UNKNOWN") || strings.Contains(errMsg, "404 Not Found")
}

func IsRepoNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "NAME_UNKNOWN")
}

func IsTrivyMediaTypeNotAllowedError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "MANIFEST_INVALID") &&
		(strings.Contains(errMsg, "vnd.aquasec.trivy") || strings.Contains(errMsg, "application/octet-stream"))
}

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

type errorCategory struct {
	name      string
	causes    []string
	solutions []string
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

func isNetworkError(err error) bool {
	var (
		netErr     net.Error
		opErr      *net.OpError
		syscallErr syscall.Errno
	)

	if errors.As(err, &netErr) {
		return true
	}

	if errors.As(err, &opErr) {
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

func isDNSError(err error) bool {
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr)
}

func formatError(category errorCategory, err error) string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString(colorBold)
	b.WriteString(colorRed)
	b.WriteString("error")
	b.WriteString(colorReset)
	b.WriteString(colorBold)
	b.WriteString(": ")
	b.WriteString(category.name)
	b.WriteString(colorReset)
	b.WriteString("\n")

	b.WriteString(colorCyan)
	b.WriteString("  ╰─▶ ")
	b.WriteString(colorReset)
	b.WriteString(err.Error())
	b.WriteString("\n\n")

	if len(category.causes) > 0 {
		b.WriteString(colorYellow)
		b.WriteString("  Possible causes:\n")
		b.WriteString(colorReset)
		for _, cause := range category.causes {
			b.WriteString("    • ")
			b.WriteString(cause)
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}

	if len(category.solutions) > 0 {
		b.WriteString(colorCyan)
		b.WriteString("  How to fix:\n")
		b.WriteString(colorReset)
		for _, solution := range category.solutions {
			b.WriteString("    • ")
			b.WriteString(solution)
			b.WriteString("\n")
		}
	}

	return b.String()
}

func FormatRegistryError(err error) string {
	if err == nil {
		return ""
	}

	var category errorCategory

	switch {
	case isCertificateError(err):
		category = errorCategory{
			name: "TLS/certificate verification failed",
			causes: []string{
				"Self-signed certificate without proper trust chain",
				"Certificate expired or not yet valid",
				"Hostname mismatch between certificate and registry URL",
				"Corporate proxy or middleware intercepting HTTPS connections",
			},
			solutions: []string{
				"Use --insecure flag to skip TLS verification (not recommended for production)",
				"Add the registry's CA certificate to your system trust store",
				"Verify the registry URL hostname matches the certificate",
			},
		}

	case isAuthenticationError(err):
		var transportErr *transport.Error
		name := "Authentication failed"
		if errors.As(err, &transportErr) {
			switch transportErr.StatusCode {
			case http.StatusUnauthorized:
				name = "Authentication failed (HTTP 401 Unauthorized)"
			case http.StatusForbidden:
				name = "Access denied (HTTP 403 Forbidden)"
			}
		}

		category = errorCategory{
			name: name,
			causes: []string{
				"Invalid or expired credentials",
				"License key is incorrect, expired, or not provided",
				"Insufficient permissions for the requested operation",
			},
			solutions: []string{
				"Verify your license key is correct and not expired",
				"Ensure --license flag is specified with a valid key",
				"Contact registry administrator to verify access rights",
			},
		}

	case isDNSError(err):
		var dnsErr *net.DNSError
		name := "DNS resolution failed"
		if errors.As(err, &dnsErr) && dnsErr.Name != "" {
			name = fmt.Sprintf("DNS resolution failed for '%s'", dnsErr.Name)
		}

		category = errorCategory{
			name: name,
			causes: []string{
				"Registry hostname cannot be resolved by DNS",
				"DNS server is unreachable or not responding",
				"Incorrect registry URL or typo in hostname",
			},
			solutions: []string{
				"Verify the registry URL is spelled correctly",
				"Check your DNS server configuration",
				"Try using the registry's IP address instead of hostname",
			},
		}

	case isNetworkError(err):
		var opErr *net.OpError
		name := "Network connection failed"
		if errors.As(err, &opErr) {
			if opErr.Addr != nil {
				name = fmt.Sprintf("Network connection failed to %s", opErr.Addr.String())
			}
		}

		category = errorCategory{
			name: name,
			causes: []string{
				"Network connectivity issues or no internet connection",
				"Firewall or security group blocking the connection",
				"Registry server is down or unreachable",
			},
			solutions: []string{
				"Check your network connection and internet access",
				"Verify firewall rules allow outbound HTTPS (port 443)",
				"Test connectivity with: curl -v https://<registry>",
			},
		}

	case IsImageNotFoundError(err):
		category = errorCategory{
			name: "Image not found in registry",
			causes: []string{
				"Image tag doesn't exist in the registry",
				"Incorrect image name or tag specified",
			},
			solutions: []string{
				"Verify the image name and tag are correct",
				"Check if you have permission to access this image",
			},
		}

	case IsRepoNotFoundError(err):
		category = errorCategory{
			name: "Repository not found in registry",
			causes: []string{
				"Repository doesn't exist in the registry",
				"Incorrect repository path or name",
			},
			solutions: []string{
				"Verify the repository path is correct",
				"Ensure you have permission to access this repository",
			},
		}

	case IsTrivyMediaTypeNotAllowedError(err):
		category = errorCategory{
			name: "Unsupported OCI artifact type",
			causes: []string{
				"Registry doesn't support required media types for Trivy security databases",
				"Project Quay registry not configured for Deckhouse artifacts",
			},
			solutions: []string{
				"Configure registry to allow custom OCI artifact types",
				"See: https://deckhouse.io/products/kubernetes-platform/documentation/v1/supported_versions.html#container-registry",
			},
		}

	default:
		return err.Error()
	}

	return formatError(category, err)
}
