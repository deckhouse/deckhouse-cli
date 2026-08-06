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
	"fmt"
	"log/slog"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
)

// NewClusterClient builds a Client for the proxy reachable from the given cluster
// connection. The endpoint is used as-is when set, otherwise the candidates the
// cluster offers are tried in order (see discoverCandidates).
// caFile / insecure select TLS verification (mutually exclusive; New reports the
// contradiction). Either one applies to every candidate and replaces the CA the
// cluster published.
func NewClusterClient(
	ctx context.Context,
	kube kubernetes.Interface,
	restConfig *rest.Config,
	logger *dkplog.Logger,
	endpoint, caFile string,
	insecure bool,
) (*Client, error) {
	if endpoint != "" {
		return New(endpoint, restConfig, logger, flagOptions(caFile, insecure)...)
	}

	candidates, err := discoverCandidates(ctx, kube)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEndpointDiscovery, err)
	}

	rejected := make([]string, 0, len(candidates))

	for _, c := range candidates {
		client, err := New(c.endpoint, restConfig, logger, candidateOptions(c, caFile, insecure)...)
		if err != nil {
			rejected = append(rejected, fmt.Sprintf("%s %s: %s", c.source, c.endpoint, err))

			continue
		}

		if err := client.reachable(ctx); err != nil {
			logger.Debug("registry-packages-proxy candidate did not answer",
				slog.String("endpoint", c.endpoint), slog.String("candidate_source", c.source), dkplog.Err(err))

			rejected = append(rejected, fmt.Sprintf("%s %s: %s", c.source, c.endpoint, err))

			continue
		}

		logger.Debug("discovered registry-packages-proxy endpoint",
			slog.String("endpoint", c.endpoint), slog.String("discovered_via", c.source))

		return client, nil
	}

	return nil, fmt.Errorf("%w: no endpoint answered: %s", ErrEndpointDiscovery, strings.Join(rejected, "; "))
}

// flagOptions turns the TLS flags into client options.
func flagOptions(caFile string, insecure bool) []Option {
	var opts []Option

	if insecure {
		opts = append(opts, WithInsecureSkipTLSVerify())
	}

	if caFile != "" {
		opts = append(opts, WithCAFile(caFile))
	}

	return opts
}

// candidateOptions keeps the flags authoritative: a CA file or insecure given by
// the caller replaces the CA the cluster published, and the two never combine
// (New rejects that).
func candidateOptions(c candidate, caFile string, insecure bool) []Option {
	if insecure || caFile != "" {
		return flagOptions(caFile, insecure)
	}

	if len(c.caPEM) > 0 {
		return []Option{WithCAData(c.caPEM)}
	}

	return nil
}
