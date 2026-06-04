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

import (
	"context"
	"log/slog"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
)

// NewClusterClient builds a Client for the proxy reachable from the given cluster
// connection. The endpoint is used as-is when set, otherwise it is discovered:
// the public Ingress is preferred (valid TLS, reachable from a workstation) with a
// fallback to in-cluster pod IPs. caFile / insecure select TLS verification
// (mutually exclusive; New reports the contradiction). It centralizes the wiring
// shared by every command that talks to the proxy.
func NewClusterClient(
	ctx context.Context,
	kube kubernetes.Interface,
	restConfig *rest.Config,
	logger *dkplog.Logger,
	endpoint, caFile string,
	insecure bool,
) (*Client, error) {
	if endpoint == "" {
		discovered, source, err := chooseDiscoveredEndpoint(ctx, kube)
		if err != nil {
			return nil, err
		}

		logger.Debug("discovered registry-packages-proxy endpoint",
			slog.String("endpoint", discovered), slog.String("discovered_via", source))

		if source == "pod" {
			// The pod fallback is a master-node IP: unreachable from outside the
			// cluster network, and its certificate carries no IP SANs - say so
			// before the connection fails with an opaque TLS/timeout error.
			logger.Debug("no registry-packages-proxy Ingress found; the pod endpoint is reachable " +
				"only from the cluster network and needs --rpp-insecure-skip-tls-verify (or pass --rpp-endpoint)")
		}

		endpoint = discovered
	}

	var opts []Option

	if insecure {
		opts = append(opts, WithInsecureSkipTLSVerify())
	}

	if caFile != "" {
		opts = append(opts, WithCAFile(caFile))
	}

	return New(endpoint, restConfig, logger, opts...)
}
