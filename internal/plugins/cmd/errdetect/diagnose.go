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

// Package errdetect maps registry-packages-proxy failures from `d8 plugins` to
// HelpfulErrors with plugin-specific guidance.
package errdetect

import (
	"errors"

	"github.com/deckhouse/deckhouse-cli/internal/rpp"
	"github.com/deckhouse/deckhouse-cli/pkg/diagnostic"
)

// Diagnose returns a HelpfulError for a recognized proxy failure, or nil for a nil,
// unrecognized, or already-diagnosed error - so the caller keeps the original.
func Diagnose(err error) *diagnostic.HelpfulError {
	var he *diagnostic.HelpfulError
	if err == nil || errors.As(err, &he) {
		return nil
	}

	switch {
	case errors.Is(err, rpp.ErrUnauthorized):
		return help(err, "registry-packages-proxy: unauthorized (401)",
			"no accepted Bearer token (a client-certificate kubeconfig is not enough)",
			"use a kubeconfig with an OIDC token (Kubeconfig Generator or 'd8 login')")
	case errors.Is(err, rpp.ErrForbidden):
		// Plugins live under deckhouse-cli/plugins/<name>, so they are served from
		// /v1/images/ and authorized by cli-download. The packages-download role
		// covers /v1/packages/ and grants nothing here.
		return help(err, "registry-packages-proxy: forbidden (403)",
			"the identity may not download plugins",
			"bind the ClusterRole 'd8:registry-packages-proxy:cli-download' to the user/group",
			"authorization is cached ~5 min - after binding, retry with a fresh token")
	case errors.Is(err, rpp.ErrNotFound):
		return help(err, "registry-packages-proxy: plugin or version not found (404)",
			"this plugin or version is not published",
			"check the name and version with 'd8 plugins versions <name>'",
			"confirm it is published under 'deckhouse-cli/plugins/<name>'")
	case errors.Is(err, rpp.ErrUpstream):
		return help(err, "registry-packages-proxy: upstream error (5xx)",
			"the proxy could not reach the backing registry",
			"retry shortly, or check the registry-packages-proxy pods in d8-cloud-instance-manager")
	case errors.Is(err, rpp.ErrEndpointDiscovery):
		return help(err, "registry-packages-proxy: no usable endpoint found",
			"either the cluster could not be asked where the proxy is, or none of the endpoints it offered answered",
			"the message above names what was tried: a Kubernetes API failure means the kubeconfig 'server:' is unreachable or its certificate is invalid",
			"a per-endpoint failure means the proxy was not reachable there - master addresses need access to port 4219, the public host needs a valid certificate",
			"skip discovery: pass --rpp-endpoint <url> (or set D8_RPP_ENDPOINT), adding --rpp-ca-file <ca.pem> when its CA is not publicly trusted")
	default:
		return nil
	}
}

func help(err error, category, cause string, solutions ...string) *diagnostic.HelpfulError {
	return &diagnostic.HelpfulError{
		Category:    category,
		OriginalErr: err,
		Suggestions: []diagnostic.Suggestion{{Cause: cause, Solutions: solutions}},
	}
}
