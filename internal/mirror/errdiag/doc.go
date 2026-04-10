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

// Package errdiag classifies container registry errors and returns
// [diagnostic.HelpfulError] with user-friendly causes and solutions.
//
// # Usage
//
//	if diag := errdiag.Classify(err); diag != nil {
//	    return diag // implements error interface via *diagnostic.HelpfulError
//	}
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
//  11. Unsupported OCI   - unsupported OCI media types (e.g. Project Quay)
//
// DNS is checked before Network because [net.DNSError] satisfies [net.Error].
// Timeout is checked before Network for the same reason.
//
// Error matchers for flow control (e.g., skipping optional images) are in
// [github.com/deckhouse/deckhouse-cli/pkg/registry/errmatch].
package errdiag
