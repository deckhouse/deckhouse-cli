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

// Package errdetect maps registry-packages-proxy failures from `d8 cli` to
// HelpfulErrors with CLI-specific guidance.
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
		return help(err, "registry-packages-proxy: forbidden (403)",
			"the identity may not download the CLI",
			"bind the ClusterRole 'd8:registry-packages-proxy:cli-download' to the user/group",
			"authorization is cached ~5 min - after binding, retry with a fresh token")
	case errors.Is(err, rpp.ErrNotFound):
		return help(err, "registry-packages-proxy: version not found (404)",
			"this deckhouse-cli version is not published",
			"list available versions with 'd8 cli versions'")
	case errors.Is(err, rpp.ErrUpstream):
		return help(err, "registry-packages-proxy: upstream error (5xx)",
			"the proxy could not reach the backing registry",
			"retry shortly, or check the registry-packages-proxy pods in d8-cloud-instance-manager")
	case errors.Is(err, rpp.ErrEndpointDiscovery):
		return help(err, "registry-packages-proxy: endpoint discovery via the Kubernetes API failed",
			"discovery reaches the proxy through your kubeconfig's API server, which was unreachable or presented an invalid certificate",
			"this is the Kubernetes API endpoint (kubeconfig 'server:'), not the proxy - confirm it is reachable and its TLS certificate is valid for that host",
			"skip discovery: pass --rpp-endpoint https://registry-packages-proxy.<publicDomain> (or set D8_RPP_ENDPOINT)",
			"on a master node, point the kubeconfig at the local API (https://127.0.0.1:6445, CA /etc/kubernetes/pki/ca.crt) with an OIDC token")
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
