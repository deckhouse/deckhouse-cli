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

package rpp

import "errors"

// Sentinel errors let callers branch on the outcome via errors.Is instead of
// inspecting HTTP status codes themselves.
var (
	// ErrInvalidImage means the requested image or tag is malformed or outside
	// the proxy allow-list (e.g. a plugin name containing a slash).
	ErrInvalidImage = errors.New("invalid image reference")

	// ErrInvalidEndpoint means the proxy endpoint URL is empty or not an https URL with a host.
	ErrInvalidEndpoint = errors.New("invalid proxy endpoint")

	// ErrEndpointDiscovery means the proxy endpoint could not be discovered through
	// the Kubernetes API (the kubeconfig 'server:'): the API was unreachable, its
	// certificate was invalid, the identity was rejected, or no usable proxy was
	// found. It is the kube-API leg, not the proxy itself - bypass it with an
	// explicit endpoint (--rpp-endpoint / D8_RPP_ENDPOINT).
	ErrEndpointDiscovery = errors.New("registry-packages-proxy endpoint discovery failed")

	// ErrInvalidCA means the supplied CA bundle contained no usable certificates.
	ErrInvalidCA = errors.New("invalid CA bundle")

	// ErrUnsupportedConfig means the requested client configuration is
	// contradictory or unsupported (e.g. insecure TLS together with a CA bundle,
	// or a rest.Config carrying a custom transport that would bypass CA verification).
	ErrUnsupportedConfig = errors.New("unsupported client configuration")

	// ErrNotFound means the proxy has no such image or tag (HTTP 404).
	ErrNotFound = errors.New("image or tag not found")

	// ErrUnauthorized means authentication failed: the kubeconfig credentials
	// were missing, invalid or expired (HTTP 401).
	ErrUnauthorized = errors.New("unauthorized")

	// ErrForbidden means the caller is authenticated but not allowed to download.
	// In practice the cli-download ClusterRole is not bound to the subject (HTTP 403).
	ErrForbidden = errors.New("forbidden")

	// ErrUpstream means the proxy failed while talking to the backing registry
	// (HTTP 5xx).
	ErrUpstream = errors.New("registry proxy upstream error")

	// ErrFileNotFound means a requested entry was absent from the downloaded image
	// archive.
	ErrFileNotFound = errors.New("file not found in image")
)
