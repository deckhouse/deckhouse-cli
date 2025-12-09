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

package service

import (
	"archive/tar"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"
	"github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal"
)

// PluginService provides high-level operations for plugin management
type PluginService struct {
	client registry.Client
	log    *log.Logger
}

// NewPluginService creates a new plugin service
func NewPluginService(client registry.Client, logger *log.Logger) *PluginService {
	return &PluginService{
		client: client,
		log:    logger,
	}
}

// GetPluginContract reads the plugin contract from image metadata annotation
func (s *PluginService) GetPluginContract(ctx context.Context, pluginName, tag string) (*internal.Plugin, error) {
	// Create a scoped client for this specific plugin
	// The base client already has the path like "deckhouse/ee/plugins"
	// We just need to add the plugin name
	s.log.Debug("Getting plugin contract", slog.String("plugin", pluginName), slog.String("tag", tag))

	pluginClient := s.client.WithSegment(pluginName)

	digestManifestResult, err := pluginClient.GetManifest(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}

	var manifest registry.Manifest

	// multiarch manifests are just list of manifests, we need to get the image manifest
	if digestManifestResult.GetMediaType().IsIndex() {
		indexManifest, err := digestManifestResult.GetIndexManifest()
		if err != nil {
			return nil, fmt.Errorf("failed to get index manifest: %w", err)
		}

		if len(indexManifest.GetManifests()) == 0 {
			return nil, fmt.Errorf("no manifests found in index manifest")
		}

		// hardcoded first
		digest := indexManifest.GetManifests()[0].GetDigest()
		if digest.String() == "" {
			return nil, fmt.Errorf("no digest found in manifest")
		}

		digestClient := pluginClient.WithSegment("meta")

		digestManifestResult, err = digestClient.GetManifest(ctx, "@"+digest.String())
		if err != nil {
			return nil, fmt.Errorf("failed to get manifest: %w", err)
		}

		manifest, err = digestManifestResult.GetManifest()
		if err != nil {
			return nil, fmt.Errorf("failed to get manifest: %w", err)
		}
	} else {
		manifest, err = digestManifestResult.GetManifest()
		if err != nil {
			return nil, fmt.Errorf("failed to get manifest: %w", err)
		}
	}

	if manifest.GetAnnotations() == nil {
		return &internal.Plugin{
			Name:    pluginName,
			Version: tag,
		}, nil
	}

	annotations := manifest.GetAnnotations()

	contractB64, ok := annotations["contract"]
	if !ok || contractB64 == "" {
		return nil, fmt.Errorf("plugin-contract annotation not found in image metadata")
	}

	s.log.Debug("Contract base64 retrieved successfully", slog.String("contractb64", contractB64))

	contractRaw, err := base64.StdEncoding.DecodeString(contractB64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode contract: %w", err)
	}

	s.log.Debug("Contract raw retrieved successfully", slog.String("contractraw", string(contractRaw)))

	contract := new(PluginContract)
	err = json.Unmarshal(contractRaw, contract)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal contract: %w", err)
	}

	s.log.Debug("Plugin contract parsed successfully", slog.String("plugin", pluginName), slog.String("tag", tag), slog.String("name", contract.Name), slog.String("version", contract.Version))

	// Convert to domain entity
	return ContractToDomain(contract), nil
}

// GetPluginContractFromFile reads the plugin contract from a file
func GetPluginContractFromFile(contractFilePath string) (*internal.Plugin, error) {
	contractBytes, err := os.ReadFile(contractFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read contract file: %w", err)
	}

	contract := new(PluginContract)
	err = json.Unmarshal(contractBytes, contract)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal contract: %w", err)
	}

	return ContractToDomain(contract), nil
}

// ExtractPlugin downloads the plugin image and extracts it to the specified location
func (s *PluginService) ExtractPlugin(ctx context.Context, pluginName, tag, destination string) error {
	// Create a scoped client for this specific plugin
	s.log.Debug("Extracting plugin", slog.String("plugin", pluginName), slog.String("tag", tag), slog.String("destination", destination))

	pluginClient := s.client.WithSegment(pluginName)

	platform := &v1.Platform{Architecture: runtime.GOARCH, OS: runtime.GOOS}
	img, err := pluginClient.GetImage(ctx, tag, client.WithPlatform{Platform: platform})
	if err != nil {
		return fmt.Errorf("failed to get image: %w", err)
	}

	err = s.extractPluginFromTar(img.Extract(), destination, pluginName)
	if err != nil {
		return fmt.Errorf("failed to extract tar: %w", err)
	}

	return nil
}

// extractPluginFromTar extracts the plugin binary from a tar archive to the destination directory
func (s *PluginService) extractPluginFromTar(r io.Reader, destination, pluginName string) error {
	s.log.Debug("Starting plugin extraction from tar archive",
		slog.String("destination", destination),
		slog.String("plugin", pluginName))

	tr := tar.NewReader(r)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		// only extract regular files named "plugin"
		if header.Name == "plugin" {
			outFile, err := os.OpenFile(destination, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create plugin file %s: %w", destination, err)
			}
			defer outFile.Close()

			if _, err := io.Copy(outFile, tr); err != nil {
				return fmt.Errorf("failed to write plugin content to %s: %w", destination, err)
			}

			break // plugin found and extracted, no need to continue
		}
	}

	s.log.Debug("Plugin extraction completed successfully",
		slog.String("destination", destination),
		slog.String("plugin", pluginName))

	return nil
}

// ContractToDomain converts PluginContract DTO to Plugin domain entity
func ContractToDomain(contract *PluginContract) *internal.Plugin {
	// Note: This is a pure conversion function, no logging needed as it's called from GetPluginContract
	plugin := &internal.Plugin{
		Name:        contract.Name,
		Version:     contract.Version,
		Description: contract.Description,
		Env:         make([]internal.EnvVar, 0, len(contract.Env)),
		Flags:       make([]internal.Flag, 0, len(contract.Flags)),
	}

	// Convert env vars
	for _, envDTO := range contract.Env {
		plugin.Env = append(plugin.Env, internal.EnvVar{
			Name: envDTO.Name,
		})
	}

	// Convert flags
	for _, flagDTO := range contract.Flags {
		plugin.Flags = append(plugin.Flags, internal.Flag{
			Name: flagDTO.Name,
		})
	}

	// Convert requirements
	plugin.Requirements = internal.Requirements{
		Kubernetes: internal.KubernetesRequirement{
			Constraint: contract.Requirements.Kubernetes.Constraint,
		},
		Modules: make([]internal.ModuleRequirement, 0, len(contract.Requirements.Modules)),
	}

	for _, modDTO := range contract.Requirements.Modules {
		plugin.Requirements.Modules = append(plugin.Requirements.Modules, internal.ModuleRequirement{
			Name:       modDTO.Name,
			Constraint: modDTO.Constraint,
		})
	}

	return plugin
}

// DomainToContract converts Plugin domain entity to PluginContract DTO
func DomainToContract(plugin *internal.Plugin) *PluginContract {
	contract := &PluginContract{
		Name:        plugin.Name,
		Version:     plugin.Version,
		Description: plugin.Description,
		Env:         make([]EnvVarDTO, 0, len(plugin.Env)),
		Flags:       make([]FlagDTO, 0, len(plugin.Flags)),
		Requirements: RequirementsDTO{
			Kubernetes: KubernetesRequirementDTO{
				Constraint: plugin.Requirements.Kubernetes.Constraint,
			},
			Modules: make([]ModuleRequirementDTO, 0, len(plugin.Requirements.Modules)),
		},
	}

	for _, env := range plugin.Env {
		contract.Env = append(contract.Env, EnvVarDTO{
			Name: env.Name,
		})
	}

	for _, flag := range plugin.Flags {
		contract.Flags = append(contract.Flags, FlagDTO{
			Name: flag.Name,
		})
	}

	for _, mod := range plugin.Requirements.Modules {
		contract.Requirements.Modules = append(contract.Requirements.Modules, ModuleRequirementDTO{
			Name:       mod.Name,
			Constraint: mod.Constraint,
		})
	}

	return contract
}

// ListPlugins lists all available plugin names from the registry
// Note: This requires the registry to support the catalog API and grant access to it.
// If the registry doesn't allow catalog access, this will return an error.
func (s *PluginService) ListPlugins(ctx context.Context) ([]string, error) {
	s.log.Debug("Listing all plugins")

	// The client is already scoped to "deckhouse/ee/modules"
	// ListRepositories will return the plugin names directly (tags under that path)
	pluginNames, err := s.client.ListRepositories(ctx)
	if err != nil {
		s.log.Warn("Failed to list repositories from registry. The registry may not allow catalog access or you may need special permissions.",
			slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to list repositories (registry may not allow catalog access): %w", err)
	}

	s.log.Debug("Plugins listed successfully", slog.Int("count", len(pluginNames)))

	return pluginNames, nil
}

// ListPluginTags lists all available tags for a specific plugin
func (s *PluginService) ListPluginTags(ctx context.Context, pluginName string) ([]string, error) {
	// Create a scoped client for this specific plugin
	s.log.Debug("Listing plugin tags", slog.String("plugin", pluginName))

	pluginClient := s.client.WithSegment(pluginName)

	tags, err := pluginClient.ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list tags for plugin %s: %w", pluginName, err)
	}

	s.log.Debug("Plugin tags listed successfully", slog.String("plugin", pluginName), slog.Int("count", len(tags)))

	return tags, nil
}
