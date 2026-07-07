/*
Copyright 2025 Flant JSC

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

package plugins

import (
	"context"
	"fmt"

	d8flags "github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal/rpp"
	rppflags "github.com/deckhouse/deckhouse-cli/internal/rpp/flags"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// InitPluginServices wires m.service to the in-cluster registry-packages-proxy,
// reaching the proxy by the user's kubeconfig identity. ctx bounds endpoint
// discovery, so a Ctrl-C during command startup is honored. The proxy is the only
// plugin source (ADR: deckhouse-cli reaches the registry exclusively through it).
func (m *Manager) InitPluginServices(ctx context.Context) error {
	// legacy --source bypass (temporary): pull straight from a registry, skipping
	// the proxy and the cluster. See internal/plugins/source_legacy.go.
	if d8flags.SourceRegistryRepo != "" {
		return m.initLegacyRegistrySource()
	}

	restConfig, kubeCl, err := utilk8s.SetupK8sClientSet(d8flags.Kubeconfig, d8flags.KubeContext)
	if err != nil {
		return fmt.Errorf("set up kubernetes client: %w", err)
	}

	client, err := rpp.NewClusterClient(
		ctx, kubeCl, restConfig, m.logger.Named("registry-packages-proxy"),
		rppflags.Endpoint, rppflags.CAFile, rppflags.InsecureSkipTLSVerify,
	)
	if err != nil {
		return fmt.Errorf("build registry-packages-proxy client: %w", err)
	}

	m.service = newRppPluginSource(client, m.logger)

	return nil
}
