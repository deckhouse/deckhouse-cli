package plugins

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse/pkg/log"
)

const (
	// PluginRepositoryPrefix is the base path for plugin images in the registry
	// This is relative to the registry base path (e.g., if registry is "registry.deckhouse.io/deckhouse/ee/modules"
	// then full path will be "registry.deckhouse.io/deckhouse/ee/modules/deckhouse/cli/plugins")
	PluginRepositoryPrefix = "deckhouse/ee/modules"
)

// PluginService provides high-level operations for plugin management
type PluginService struct {
	client pkg.RegistryClient
	log    *log.Logger
}

// NewPluginService creates a new plugin service
func NewPluginService(client pkg.RegistryClient, logger *log.Logger) *PluginService {
	return &PluginService{
		client: client,
		log:    logger,
	}
}

// GetPluginContract reads the plugin contract from image metadata annotation
func (s *PluginService) GetPluginContract(ctx context.Context, pluginName, tag string) (*internal.Plugin, error) {
	// The pluginName is just the simple name like "code", "commander", etc.
	// The registry client already has the full base path, so we just pass the plugin name
	repository := pluginName
	s.log.Debug("Getting plugin contract", slog.String("plugin", pluginName), slog.String("repository", repository), slog.String("tag", tag))

	// Get the plugin-contract label from image
	contractJSON, exists, err := s.client.GetLabel(ctx, repository, tag, "plugin-contract")
	if err != nil {
		return nil, fmt.Errorf("failed to get image labels: %w", err)
	}

	if !exists {
		s.log.Debug("Plugin contract not found in image", slog.String("plugin", pluginName), slog.String("repository", repository), slog.String("tag", tag))

		return nil, fmt.Errorf("plugin-contract annotation not found in image metadata")
	}

	// Parse the contract JSON into DTO
	var contract PluginContract
	if err := json.Unmarshal([]byte(contractJSON), &contract); err != nil {
		return nil, fmt.Errorf("failed to parse plugin contract: %w", err)
	}

	s.log.Debug("Plugin contract parsed successfully", slog.String("plugin", pluginName), slog.String("repository", repository), slog.String("tag", tag), slog.String("name", contract.Name), slog.String("version", contract.Version))

	// Convert to domain entity
	return contractToDomain(&contract), nil
}

// ExtractPlugin downloads the plugin image and extracts it to the specified location
func (s *PluginService) ExtractPlugin(ctx context.Context, pluginName, tag, destination string) error {
	// The pluginName is just the simple name like "code", "commander", etc.
	// The registry client already has the full base path, so we just pass the plugin name
	repository := pluginName
	s.log.Debug("Extracting plugin", slog.String("plugin", pluginName), slog.String("repository", repository), slog.String("tag", tag), slog.String("destination", destination))

	// Create destination directory if it doesn't exist
	if err := os.MkdirAll(destination, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	s.log.Debug("Destination directory created", slog.String("destination", destination))

	// Extract image layers using handler pattern
	return s.client.ExtractImageLayers(ctx, repository, tag, func(stream pkg.LayerStream) error {
		s.log.Info("Extracting layer", slog.Int("index", stream.GetIndex()), slog.Int("total", stream.GetTotal()))

		// Extract the tar stream
		if err := s.extractTar(stream.GetReader(), destination); err != nil {
			return fmt.Errorf("failed to extract tar: %w", err)
		}

		return nil
	})
}

// extractTar extracts a tar archive to the destination directory
func (s *PluginService) extractTar(r io.Reader, destination string) error {
	s.log.Debug("Starting tar extraction", slog.String("destination", destination))

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
			s.log.Debug("Creating directory", slog.String("path", target))

			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", target, err)
			}

		case tar.TypeReg:
			// Create file
			s.log.Debug("Extracting file", slog.String("path", target), slog.Int64("size", header.Size))

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

			s.log.Debug("File extracted successfully", slog.String("path", target))

		default:
			// Skip unsupported types
			s.log.Warn("Skipping unsupported tar entry", slog.Int("type", int(header.Typeflag)), slog.String("name", header.Name))
		}
	}

	s.log.Debug("Tar extraction completed", slog.String("destination", destination))

	return nil
}

// contractToDomain converts PluginContract DTO to Plugin domain entity
func contractToDomain(contract *PluginContract) *internal.Plugin {
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

// ListPlugins lists all available plugin names from the registry
// Note: This requires the registry to support the catalog API and grant access to it.
// If the registry doesn't allow catalog access, this will return an error.
func (s *PluginService) ListPlugins(ctx context.Context) ([]string, error) {
	s.log.Debug("Listing all plugins", slog.String("prefix", PluginRepositoryPrefix))

	// List all repositories under the plugin prefix
	repos, err := s.client.ListRepositories(ctx, PluginRepositoryPrefix)
	if err != nil {
		s.log.Warn("Failed to list repositories from registry. The registry may not allow catalog access or you may need special permissions.",
			slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to list repositories (registry may not allow catalog access): %w", err)
	}

	// Extract plugin names from repository paths
	// Repository format: deckhouse/ee/modules/<plugin-name>
	// The ListRepositories returns paths like: "deckhouse/ee/modules/code", "deckhouse/ee/modules/commander", etc.
	// We need to extract just the plugin name (last part after the prefix)
	var pluginNames []string
	prefixWithSlash := PluginRepositoryPrefix + "/"

	s.log.Debug("Processing repository list",
		slog.Int("total_repos", len(repos)),
		slog.String("prefix", PluginRepositoryPrefix))

	for _, repo := range repos {
		s.log.Debug("Checking repository", slog.String("repo", repo))
		// Check if repo starts with the prefix
		if len(repo) > len(prefixWithSlash) && repo[:len(prefixWithSlash)] == prefixWithSlash {
			// Extract just the plugin name (everything after the prefix)
			pluginName := repo[len(prefixWithSlash):]
			pluginNames = append(pluginNames, pluginName)
			s.log.Debug("Found plugin", slog.String("name", pluginName))
		}
	}

	s.log.Debug("Plugins listed successfully", slog.Int("count", len(pluginNames)))

	return pluginNames, nil
}

// ListPluginTags lists all available tags for a specific plugin
func (s *PluginService) ListPluginTags(ctx context.Context, pluginName string) ([]string, error) {
	// The pluginName is just the simple name like "code", "commander", etc.
	// The registry client already has the full base path, so we just pass the plugin name
	repository := pluginName
	s.log.Debug("Listing plugin tags", slog.String("plugin", pluginName), slog.String("repository", repository))

	tags, err := s.client.ListTags(ctx, repository)
	if err != nil {
		return nil, fmt.Errorf("failed to list tags for plugin %s: %w", pluginName, err)
	}

	s.log.Debug("Plugin tags listed successfully", slog.String("plugin", pluginName), slog.Int("count", len(tags)))

	return tags, nil
}
