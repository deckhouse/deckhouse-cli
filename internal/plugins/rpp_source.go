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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"

	"sigs.k8s.io/yaml"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/rpp"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// pluginBinaryEntryName is the file inside a plugin image that holds the executable.
const pluginBinaryEntryName = "plugin"

// contractAnnotation is the manifest annotation key that carries the plugin
// contract as base64-encoded JSON.
const contractAnnotation = "contract"

// imageManifest is the subset of an OCI/Docker image manifest or index the CLI
// reads: the top-level annotations (where the plugin contract lives) and, for an
// index, the child descriptors so it can follow one when the index itself carries
// no contract.
type imageManifest struct {
	Annotations map[string]string    `json:"annotations,omitempty"`
	Manifests   []manifestDescriptor `json:"manifests,omitempty"`
}

type manifestDescriptor struct {
	Digest string `json:"digest"`
}

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

	// The contract is a manifest annotation, so a manifest fetch is enough - no
	// image/layer pull. The proxy returns the raw manifest; the contract key and its
	// base64 encoding are CLI knowledge, decoded here. The binary still needs a full
	// pull in ExtractPlugin.
	encoded, err := s.contractAnnotation(ctx, ref, tag)
	if err != nil {
		return nil, err
	}

	if encoded == "" {
		// No contract annotation anywhere: surface just name+version. Install can
		// proceed and the cluster/plugin requirement checks have nothing to enforce.
		s.logger.Debug("plugin image has no contract annotation", slog.String("plugin", pluginName), slog.String("tag", tag))

		return &internal.Plugin{Name: pluginName, Version: tag}, nil
	}

	contract, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		// A present but malformed annotation is a publishing bug, not a contract-less
		// plugin - surface it rather than silently degrading to name+version.
		return nil, fmt.Errorf("decode contract annotation for plugin %q: %w", pluginName, err)
	}

	return contractFromBytes(contract, pluginName, tag)
}

// contractAnnotation returns the base64 "contract" annotation for ref:tag, or ""
// when absent. A multi-platform tag points at an index; the contract is platform-
// independent, so it may sit on the index or on its (identical) child manifests.
// When the index carries none, the first child is followed - one extra manifest
// fetch, still no layer pull.
func (s *rppPluginSource) contractAnnotation(ctx context.Context, ref rpp.ImageRef, tag string) (string, error) {
	man, err := s.fetchManifest(ctx, ref, tag)
	if err != nil {
		return "", err
	}

	if encoded := man.Annotations[contractAnnotation]; encoded != "" {
		return encoded, nil
	}

	if len(man.Manifests) == 0 {
		return "", nil
	}

	child, err := s.fetchManifest(ctx, ref, man.Manifests[0].Digest)
	if err != nil {
		return "", err
	}

	return child.Annotations[contractAnnotation], nil
}

// fetchManifest fetches and decodes the raw manifest for ref addressed by refOrDigest
// (a tag or a child digest).
func (s *rppPluginSource) fetchManifest(ctx context.Context, ref rpp.ImageRef, refOrDigest string) (imageManifest, error) {
	raw, err := s.client.GetManifest(ctx, ref, refOrDigest)
	if err != nil {
		return imageManifest{}, fmt.Errorf("get manifest for plugin %q: %w", ref.String(), err)
	}

	var man imageManifest
	if err := json.Unmarshal(raw, &man); err != nil {
		return imageManifest{}, fmt.Errorf("decode manifest for plugin %q: %w", ref.String(), err)
	}

	return man, nil
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

// contractFromBytes decodes a contract, backfilling identity from the request so a
// present-but-degenerate contract still yields a usable name/version.
func contractFromBytes(raw []byte, pluginName, tag string) (*internal.Plugin, error) {
	// The decoded annotation is JSON; YAMLToJSON is a tolerant pass-through (valid
	// JSON is valid YAML) and the shared decoder expects JSON.
	jsonRaw, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("decode contract for plugin %q: %w", pluginName, err)
	}

	var dto service.PluginContract
	if err := service.UnmarshalContract(jsonRaw, &dto); err != nil {
		return nil, fmt.Errorf("decode contract for plugin %q: %w", pluginName, err)
	}

	plugin, err := service.ContractToDomain(&dto)
	if err != nil {
		return nil, fmt.Errorf("decode contract for plugin %q: %w", pluginName, err)
	}

	if plugin.Name == "" {
		plugin.Name = pluginName
	}

	if plugin.Version == "" {
		plugin.Version = tag
	}

	return plugin, nil
}
