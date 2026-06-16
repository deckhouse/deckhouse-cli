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

package plugins

import (
	"context"
	"fmt"
	"log/slog"

	"sigs.k8s.io/yaml"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/rpp"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

const (
	// pluginBinaryEntryName is the file inside a plugin image that holds the
	// executable.
	pluginBinaryEntryName = "plugin"

	// pluginContractEntryName is the file inside a plugin image that holds the
	// contract. The proxy serves the image as a tar (not via OCI annotations), so
	// the contract travels as a file. Confirmed against the plugin build CI
	// (d8-package-plugin: a scratch image with /plugin and /contract.yaml). A
	// missing contract is still tolerated for older / contract-less images.
	pluginContractEntryName = "contract.yaml"

	// maxContractBytes caps the contract read so a malformed image cannot exhaust
	// memory; real contracts are a few KiB.
	maxContractBytes = 1 << 20
)

// rppPluginSource adapts the registry-packages-proxy client to pluginSource.
type rppPluginSource struct {
	client *rpp.Client
	logger *dkplog.Logger
}

func newRppPluginSource(client *rpp.Client, logger *dkplog.Logger) *rppPluginSource {
	return &rppPluginSource{client: client, logger: logger}
}

var _ pluginSource = (*rppPluginSource)(nil)

func (s *rppPluginSource) ListPluginTags(ctx context.Context, pluginName string) ([]string, error) {
	ref, err := rpp.PluginImage(pluginName)
	if err != nil {
		return nil, err
	}

	return s.client.ListTags(ctx, ref)
}

func (s *rppPluginSource) GetPluginContract(ctx context.Context, pluginName, tag string) (*internal.Plugin, error) {
	ref, err := rpp.PluginImage(pluginName)
	if err != nil {
		return nil, err
	}

	// The proxy serves no manifest/annotations, so reading the contract needs a
	// full image pull; an install therefore pulls the image twice (here and in
	// ExtractPlugin). Plugin images are small, so this is acceptable for now.
	body, err := s.client.PullImage(ctx, ref, tag)
	if err != nil {
		return nil, err
	}

	defer func() { _ = body.Close() }()

	raw, found, err := rpp.ReadFile(body, pluginContractEntryName, maxContractBytes)
	if err != nil {
		return nil, fmt.Errorf("read contract for plugin %q: %w", pluginName, err)
	}

	if !found {
		// Plugin images do not carry a contract yet, so surface just name+version:
		// install can proceed and the cluster/plugin requirement checks have nothing
		// to enforce.
		s.logger.Debug("plugin image has no contract file", slog.String("plugin", pluginName), slog.String("tag", tag))

		return &internal.Plugin{Name: pluginName, Version: tag}, nil
	}

	return contractFromBytes(raw, pluginName, tag)
}

func (s *rppPluginSource) ExtractPlugin(ctx context.Context, pluginName, tag, destination string) error {
	ref, err := rpp.PluginImage(pluginName)
	if err != nil {
		return err
	}

	body, err := s.client.PullImage(ctx, ref, tag)
	if err != nil {
		return err
	}

	defer func() { _ = body.Close() }()

	if err := rpp.ExtractFileToPath(body, pluginBinaryEntryName, destination, rpp.ExecutableMode, rpp.DefaultBinaryByteLimit); err != nil {
		return fmt.Errorf("extract %q binary: %w", pluginName, err)
	}

	return nil
}

// contractFromBytes decodes a contract file, backfilling identity from the request
// so a present-but-degenerate contract still yields a usable name/version.
func contractFromBytes(raw []byte, pluginName, tag string) (*internal.Plugin, error) {
	// The contract file is YAML (contract.yaml); the shared decoder expects JSON.
	jsonRaw, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("decode contract for plugin %q: %w", pluginName, err)
	}

	var dto service.PluginContract
	if err := service.UnmarshalContract(jsonRaw, &dto); err != nil {
		return nil, fmt.Errorf("decode contract for plugin %q: %w", pluginName, err)
	}

	plugin := service.ContractToDomain(&dto)

	if plugin.Name == "" {
		plugin.Name = pluginName
	}

	if plugin.Version == "" {
		plugin.Version = tag
	}

	return plugin, nil
}
