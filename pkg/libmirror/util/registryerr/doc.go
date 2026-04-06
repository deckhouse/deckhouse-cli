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

// Package registryerr classifies container registry errors and provides
// user-friendly diagnostics for d8 mirror operations.
//
// When users encounter registry errors during "d8 mirror pull/push", raw Go
// errors like "EOF" or "x509: certificate signed by unknown authority" are
// often not actionable. This package analyzes such errors, determines the
// root cause category, and produces a formatted diagnostic with possible
// causes and concrete solutions.
//
// # Usage
//
//	if diag := registryerr.Classify(err); diag != nil {
//	    return diag // implements error interface
//	}
//
// The returned [Diagnostic] implements the error interface with two representations:
//   - [Diagnostic.Error] returns plain text suitable for logging
//   - [Diagnostic.Format] returns colored terminal output (when stderr is a TTY)
//
// # Output example
//
// [Diagnostic.Format] produces output like this:
//
//	error: TLS/certificate verification failed
//	  ╰─▶ Get "https://registry.example.com/v2/": x509: certificate signed by unknown authority
//
//	  Possible causes:
//	    * Self-signed certificate without proper trust chain
//	    * Certificate expired or not yet valid
//	    * Corporate proxy or middleware intercepting HTTPS connections
//
//	  How to fix:
//	    * Use --tls-skip-verify flag to skip TLS verification (not recommended for production)
//	    * Add the registry's CA certificate to your system trust store
//
// # Classification
//
// [Classify] uses Go's [errors.Is] and [errors.As] to detect error types from
// the standard library (crypto/x509, net, io) and go-containerregistry
// (transport.Error). Detection order matters - more specific checks run first:
//
//  1. EOF                - io.EOF, io.ErrUnexpectedEOF (proxy/middleware termination)
//  2. TLS/Certificate    - x509.UnknownAuthorityError, HostnameError, etc.
//  3. Authentication     - transport.Error with HTTP 401/403
//  4. Rate limiting      - transport.Error with HTTP 429
//  5. Server errors      - transport.Error with HTTP 500/502/503/504
//  6. DNS                - net.DNSError
//  7. Timeout            - context.DeadlineExceeded
//  8. Network            - net.OpError, syscall.ECONNREFUSED, etc.
//  9. Image not found    - error message contains "MANIFEST_UNKNOWN"
//  10. Repo not found    - error message contains "NAME_UNKNOWN"
//  11. Unsupported OCI   - Trivy media type rejection (Project Quay)
//
// DNS is checked before Network because [net.DNSError] satisfies [net.Error].
// Timeout is checked before Network for the same reason.
//
// Error type checkers ([IsImageNotFound], [IsRepoNotFound], [IsTrivyMediaTypeNotAllowed])
// are used for flow control in mirror operations (e.g., skipping optional images).
package registryerr
