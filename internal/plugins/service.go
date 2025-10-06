package plugins

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// PluginService provides high-level operations for plugin management
type PluginService struct {
	registry string
	client   *RegistryClient
}

// NewPluginService creates a new plugin service
func NewPluginService(registry, username, password string) *PluginService {
	client := NewRegistryClient(registry, username, password)
	return &PluginService{
		registry: registry,
		client:   client,
	}
}

// GetPluginContract reads the plugin contract from image metadata annotation
func (s *PluginService) GetPluginContract(ctx context.Context, repository, tag string) (*internal.Plugin, error) {
	// Get the plugin-contract label from image
	contractJSON, exists, err := s.client.GetLabel(ctx, repository, tag, "plugin-contract")
	if err != nil {
		return nil, fmt.Errorf("failed to get image labels: %w", err)
	}

	if !exists {
		return nil, fmt.Errorf("plugin-contract annotation not found in image metadata")
	}

	// Parse the contract JSON into DTO
	var contract PluginContract
	if err := json.Unmarshal([]byte(contractJSON), &contract); err != nil {
		return nil, fmt.Errorf("failed to parse plugin contract: %w", err)
	}

	// Convert to domain entity
	return contractToDomain(&contract), nil
}

// ExtractPlugin downloads the plugin image and extracts it to the specified location
func (s *PluginService) ExtractPlugin(ctx context.Context, repository, tag, destination string) error {
	// Get image layers
	layers, err := s.client.GetImageLayers(ctx, repository, tag)
	if err != nil {
		return fmt.Errorf("failed to get image layers: %w", err)
	}

	// Create destination directory if it doesn't exist
	if err := os.MkdirAll(destination, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Extract all layers
	return s.extractLayers(layers, destination)
}

// extractLayers extracts all layers to the destination
func (s *PluginService) extractLayers(layers []v1.Layer, destination string) error {

	// Extract each layer
	for i, layer := range layers {
		fmt.Printf("Extracting layer %d/%d...\n", i+1, len(layers))

		// Get the layer as an uncompressed tar stream
		layerReader, err := layer.Uncompressed()
		if err != nil {
			return fmt.Errorf("failed to uncompress layer %d: %w", i, err)
		}
		defer layerReader.Close()

		// Extract the tar
		if err := s.extractTar(layerReader, destination); err != nil {
			return fmt.Errorf("failed to extract layer %d: %w", i, err)
		}
	}

	return nil
}

// extractTar extracts a tar archive to the destination directory
func (s *PluginService) extractTar(r io.Reader, destination string) error {
	tr := tar.NewReader(r)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		// Construct the full path
		target := filepath.Join(destination, header.Name)

		// Ensure the path is within the destination (prevent path traversal)
		rel, err := filepath.Rel(destination, target)
		if err != nil || len(rel) > 0 && rel[0] == '.' && rel[1] == '.' {
			return fmt.Errorf("invalid file path (path traversal attempt): %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			// Create directory
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", target, err)
			}

		case tar.TypeReg:
			// Create file
			// Ensure parent directory exists
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory for %s: %w", target, err)
			}

			// Create the file
			outFile, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", target, err)
			}

			// Copy the file contents
			if _, err := io.Copy(outFile, tr); err != nil {
				outFile.Close()
				return fmt.Errorf("failed to write file %s: %w", target, err)
			}
			outFile.Close()

		case tar.TypeSymlink:
			// Create symlink
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory for symlink %s: %w", target, err)
			}

			// Remove if exists
			os.Remove(target)

			if err := os.Symlink(header.Linkname, target); err != nil {
				return fmt.Errorf("failed to create symlink %s: %w", target, err)
			}

		case tar.TypeLink:
			// Create hard link
			linkTarget := filepath.Join(destination, header.Linkname)
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory for link %s: %w", target, err)
			}

			// Remove if exists
			os.Remove(target)

			if err := os.Link(linkTarget, target); err != nil {
				return fmt.Errorf("failed to create hard link %s: %w", target, err)
			}

		default:
			// Skip unsupported types
			fmt.Printf("Skipping unsupported tar entry type %d for %s\n", header.Typeflag, header.Name)
		}
	}

	return nil
}

// contractToDomain converts PluginContract DTO to Plugin domain entity
func contractToDomain(contract *PluginContract) *internal.Plugin {
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
