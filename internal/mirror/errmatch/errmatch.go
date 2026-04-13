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

// Package errmatch provides error matchers for container registry responses.
// These are used for flow control in mirror operations (e.g., skipping optional images).
package errmatch

import (
	"errors"
	"strings"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

// IsImageNotFound returns true if the error indicates that the requested image
// tag or manifest does not exist in the registry.
func IsImageNotFound(err error) bool {
	if err == nil {
		return false
	}

	// Typed check: works for GET responses where registry returns JSON with error codes.
	if hasDiagnosticCode(err, transport.ManifestUnknownErrorCode) {
		return true
	}

	// String fallback: HEAD responses have no body per HTTP spec, so transport.Error.Errors
	// is empty. Also covers registries that return plain text instead of structured JSON.
	errMsg := err.Error()
	return strings.Contains(errMsg, "MANIFEST_UNKNOWN") || strings.Contains(errMsg, "404 Not Found")
}

// IsRepoNotFound returns true if the error indicates that the requested
// repository does not exist in the registry.
func IsRepoNotFound(err error) bool {
	if err == nil {
		return false
	}

	// Typed check: works for GET responses with structured JSON error codes.
	if hasDiagnosticCode(err, transport.NameUnknownErrorCode) {
		return true
	}

	// String fallback: same as IsImageNotFound - covers HEAD responses and plain text errors.
	return strings.Contains(err.Error(), "NAME_UNKNOWN")
}

// hasDiagnosticCode checks if err is a *transport.Error containing
// a Diagnostic with the given error code.
func hasDiagnosticCode(err error, code transport.ErrorCode) bool {
	var transportErr *transport.Error
	if !errors.As(err, &transportErr) {
		return false
	}

	for _, diag := range transportErr.Errors {
		if diag.Code == code {
			return true
		}
	}

	return false
}
