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

package stub

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/deckhouse/deckhouse-cli/pkg"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// RegistryClientStub provides a stub implementation of RegistryClient for testing
type RegistryClientStub struct {
	registries      map[string]*RegistryData
	currentRegistry string
}

// RegistryData holds data for a specific registry
type RegistryData struct {
	repositories map[string]*RepositoryData
}

// RepositoryData holds data for a specific repository
type RepositoryData struct {
	tags    []string
	images  map[string]*ImageData
	digests map[string]*v1.Hash
}

// ImageData holds data for a specific image
type ImageData struct {
	manifest []byte
	config   *v1.ConfigFile
	image    pkg.RegistryImage
	digest   *v1.Hash
	exists   bool
}

// RegistryImageStub provides a simple stub implementation of RegistryImage
type RegistryImageStub struct {
	manifest *v1.Manifest
	config   *v1.ConfigFile
	digest   v1.Hash
}

// StubLayer provides a simple stub implementation of v1.Layer
type StubLayer struct {
	digest v1.Hash
	size   int64
	data   []byte
}

// Digest implements v1.Layer
func (l *StubLayer) Digest() (v1.Hash, error) {
	return l.digest, nil
}

// DiffID implements v1.Layer
func (l *StubLayer) DiffID() (v1.Hash, error) {
	return l.digest, nil
}

// Compressed implements v1.Layer
func (l *StubLayer) Compressed() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.data)), nil
}

// Uncompressed implements v1.Layer
func (l *StubLayer) Uncompressed() (io.ReadCloser, error) {
	// Decompress the gzipped data
	gzipReader, err := gzip.NewReader(bytes.NewReader(l.data))
	if err != nil {
		return nil, err
	}
	return gzipReader, nil
}

// Size implements v1.Layer
func (l *StubLayer) Size() (int64, error) {
	return l.size, nil
}

// MediaType implements v1.Layer
func (l *StubLayer) MediaType() (types.MediaType, error) {
	return types.DockerLayer, nil
}

// Digest implements v1.Image
func (r *RegistryImageStub) Digest() (v1.Hash, error) {
	return r.digest, nil
}

// Manifest implements v1.Image
func (r *RegistryImageStub) Manifest() (*v1.Manifest, error) {
	return r.manifest, nil
}

// ConfigFile implements v1.Image
func (r *RegistryImageStub) ConfigFile() (*v1.ConfigFile, error) {
	return r.config, nil
}

// ConfigName implements v1.Image
func (r *RegistryImageStub) ConfigName() (v1.Hash, error) {
	return r.digest, nil
}

// RawManifest implements v1.Image
func (r *RegistryImageStub) RawManifest() ([]byte, error) {
	return []byte(`{"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json"}`), nil
}

// RawConfigFile implements v1.Image
func (r *RegistryImageStub) RawConfigFile() ([]byte, error) {
	return []byte(`{"config":{"Image":"test"},"rootfs":{"type":"layers","diff_ids":["sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"]}}`), nil
}

// MediaType implements v1.Image
func (r *RegistryImageStub) MediaType() (types.MediaType, error) {
	return types.DockerManifestSchema2, nil
}

// Size implements v1.Image
func (r *RegistryImageStub) Size() (int64, error) {
	return 1024, nil
}

// LayerByDigest implements v1.Image
func (r *RegistryImageStub) LayerByDigest(h v1.Hash) (v1.Layer, error) {
	return nil, fmt.Errorf("LayerByDigest not implemented in stub")
}

// LayerByDiffID implements v1.Image
func (r *RegistryImageStub) LayerByDiffID(h v1.Hash) (v1.Layer, error) {
	return nil, fmt.Errorf("LayerByDiffID not implemented in stub")
}

// Layers implements v1.Image
func (r *RegistryImageStub) Layers() ([]v1.Layer, error) {
	// Create a gzipped tar containing changelog.yaml and version.json
	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)
	tarWriter := tar.NewWriter(gzipWriter)

	// Add changelog.yaml file
	changelogData := `candi:
  fixes:
  - summary: "Fix deckhouse containerd start after installing new containerd-deckhouse package."
    pull_request: "https://github.com/deckhouse/deckhouse/pull/6329"
`
	changelogHeader := &tar.Header{
		Name: "changelog.yaml",
		Mode: 0644,
		Size: int64(len(changelogData)),
	}
	if err := tarWriter.WriteHeader(changelogHeader); err != nil {
		return nil, err
	}
	if _, err := tarWriter.Write([]byte(changelogData)); err != nil {
		return nil, err
	}

	// Add version.json file
	versionData := `{"disruptions":{"1.56":["ingressNginx"]},"requirements":{"containerdOnAllNodes":"true","ingressNginx":"1.1","k8s":"1.23.0","nodesMinimalOSVersionUbuntu":"18.04"},"version":"v1.72.10"}`
	versionHeader := &tar.Header{
		Name: "version.json",
		Mode: 0644,
		Size: int64(len(versionData)),
	}
	if err := tarWriter.WriteHeader(versionHeader); err != nil {
		return nil, err
	}
	if _, err := tarWriter.Write([]byte(versionData)); err != nil {
		return nil, err
	}

	tarWriter.Close()
	gzipWriter.Close()

	compressedData := buf.Bytes()
	digest, _ := v1.NewHash("sha256:e692418e4cbaf90ca69d05a66403747baa33ee08806650b51fab815ad7fc331f")

	return []v1.Layer{
		&StubLayer{
			digest: digest,
			size:   int64(len(compressedData)),
			data:   compressedData,
		},
	}, nil
}

// Extract implements RegistryImage
func (r *RegistryImageStub) Extract() io.ReadCloser {
	// Create a simple tar stream with version.json and changelog.yaml for stub purposes
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Add changelog.yaml file first
	changelogData := `candi:
  fixes:
  - summary: "Fix deckhouse containerd start after installing new containerd-deckhouse package."
    pull_request: "https://github.com/deckhouse/deckhouse/pull/6329"
`
	changelogHeader := &tar.Header{
		Name:     "changelog.yaml",
		Mode:     0644,
		Size:     int64(len(changelogData)),
		ModTime:  time.Now(),
		Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(changelogHeader); err != nil {
		return io.NopCloser(strings.NewReader(""))
	}
	if _, err := tw.Write([]byte(changelogData)); err != nil {
		return io.NopCloser(strings.NewReader(""))
	}

	// Add version.json file
	versionData := `{"disruptions":{"1.56":["ingressNginx"]},"requirements":{"containerdOnAllNodes":"true","ingressNginx":"1.1","k8s":"1.23.0","nodesMinimalOSVersionUbuntu":"18.04"},"version":"v1.72.10"}`
	header := &tar.Header{
		Name:     "version.json",
		Mode:     0644,
		Size:     int64(len(versionData)),
		ModTime:  time.Now(),
		Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(header); err != nil {
		return io.NopCloser(strings.NewReader(""))
	}
	if _, err := tw.Write([]byte(versionData)); err != nil {
		return io.NopCloser(strings.NewReader(""))
	}

	tw.Close()

	return io.NopCloser(bytes.NewReader(buf.Bytes()))
}

// GetMetadata implements RegistryImage
func (r *RegistryImageStub) GetMetadata() (pkg.ImageMeta, error) {
	return &ImageMetaStub{
		tagRef:    "registry.deckhouse.ru/deckhouse/fe:alpha",
		digestRef: fmt.Sprintf("sha256:%s", r.digest.String()),
		digest:    &r.digest,
	}, nil
}

// ImageMetaStub provides a simple stub implementation of ImageMeta
type ImageMetaStub struct {
	tagRef    string
	digestRef string
	digest    *v1.Hash
}

func (i *ImageMetaStub) GetTagReference() string {
	return i.tagRef
}

func (i *ImageMetaStub) GetDigestReference() string {
	return i.digestRef
}

func (i *ImageMetaStub) GetDigest() *v1.Hash {
	return i.digest
}

// NewRegistryClientStub creates a new stub registry client with predefined data
func NewRegistryClientStub() *RegistryClientStub {
	stub := &RegistryClientStub{
		registries: make(map[string]*RegistryData),
	}

	// Initialize with multiple registries
	stub.initializeRegistries()
	return stub
}

// initializeRegistries sets up mock data for multiple registries
func (s *RegistryClientStub) initializeRegistries() {
	// Registry 1: docker.io
	s.addRegistry("docker.io/library", map[string][]string{
		"alpine":  {"3.18", "3.19", "latest"},
		"nginx":   {"1.24", "1.25", "latest"},
		"busybox": {"1.36", "latest"},
	})

	// Registry 2: registry.deckhouse.ru
	s.addRegistry("registry.deckhouse.ru/deckhouse/fe", map[string][]string{
		"":                   {"alpha", "beta", "early-access", "stable", "rock-solid", "v1.72.10"},
		"release-channel":    {"alpha", "beta", "early-access", "stable", "rock-solid"},
		"install":            {"v1.72.10", "alpha", "beta", "early-access", "stable", "rock-solid"},
		"install-standalone": {"v1.72.10", "alpha", "beta", "early-access", "stable", "rock-solid"},
	})

	// Registry 3: gcr.io
	s.addRegistry("gcr.io/google-containers", map[string][]string{
		"pause":          {"3.9", "latest"},
		"kube-apiserver": {"v1.28.0", "v1.29.0", "latest"},
	})

	// Registry 4: quay.io
	s.addRegistry("quay.io/prometheus", map[string][]string{
		"prometheus":   {"v2.45.0", "v2.46.0", "latest"},
		"alertmanager": {"v0.26.0", "v0.27.0", "latest"},
	})
}

// addRegistry adds a registry with repositories and tags
func (s *RegistryClientStub) addRegistry(registryPath string, repos map[string][]string) {
	if s.registries[registryPath] == nil {
		s.registries[registryPath] = &RegistryData{
			repositories: make(map[string]*RepositoryData),
		}
	}

	regData := s.registries[registryPath]
	for repo, tags := range repos {
		if regData.repositories[repo] == nil {
			regData.repositories[repo] = &RepositoryData{
				tags:    tags,
				images:  make(map[string]*ImageData),
				digests: make(map[string]*v1.Hash),
			}
		}

		repoData := regData.repositories[repo]
		for _, tag := range tags {
			// Create mock image data for each tag
			imageData := s.createMockImageData(registryPath, repo, tag)
			repoData.images[tag] = imageData

			// Set digest
			repoData.digests[tag] = imageData.digest
		}
	}
}

// createMockImageData creates mock image data with manifest, config, and registry image
func (s *RegistryClientStub) createMockImageData(registry, repo, tag string) *ImageData {
	// Create a mock digest - use a valid SHA256 hash
	digest, _ := v1.NewHash("sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")

	// Create mock manifest
	manifest := []byte(`{
		"schemaVersion": 2,
		"mediaType": "application/vnd.docker.distribution.manifest.v2+json",
		"config": {
			"mediaType": "application/vnd.docker.container.image.v1+json",
			"size": 1469,
			"digest": "sha256:b5d2b2c507a0944348e0303114d8d93aaaa081732b86451d9bce1f432a537bc7"
		},
		"layers": [
			{
				"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
				"size": 32654,
				"digest": "sha256:e692418e4cbaf90ca69d05a66403747baa33ee08806650b51fab815ad7fc331f"
			}
		]
	}`)

	// Create mock config
	config := &v1.ConfigFile{
		Config: v1.Config{
			Image: fmt.Sprintf("%s/%s:%s", registry, repo, tag),
			Labels: map[string]string{
				"org.opencontainers.image.created": "2024-01-01T00:00:00Z",
				"org.opencontainers.image.version": tag,
				"org.opencontainers.image.source":  fmt.Sprintf("https://%s", registry),
			},
		},
		RootFS: v1.RootFS{
			Type: "layers",
			DiffIDs: []v1.Hash{
				func() v1.Hash {
					h, _ := v1.NewHash("sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
					return h
				}(),
			},
		},
		History: []v1.History{
			{
				Created: v1.Time{Time: v1.Time{}.Time},
				Comment: fmt.Sprintf("Build for %s/%s:%s", registry, repo, tag),
			},
		},
	}

	// Create mock registry image
	imageStub := &RegistryImageStub{
		manifest: &v1.Manifest{
			SchemaVersion: 2,
			MediaType:     types.DockerManifestSchema2,
			Config: v1.Descriptor{
				MediaType: types.DockerConfigJSON,
				Size:      1469,
				Digest: func() v1.Hash {
					h, _ := v1.NewHash("sha256:b5d2b2c507a0944348e0303114d8d93aaaa081732b86451d9bce1f432a537bc7")
					return h
				}(),
			},
			Layers: []v1.Descriptor{
				{
					MediaType: types.DockerLayer,
					Size:      32654,
					Digest: func() v1.Hash {
						h, _ := v1.NewHash("sha256:e692418e4cbaf90ca69d05a66403747baa33ee08806650b51fab815ad7fc331f")
						return h
					}(),
				},
			},
		},
		config: config,
		digest: digest,
	}

	return &ImageData{
		manifest: manifest,
		config:   config,
		image:    imageStub,
		digest:   &digest,
		exists:   true,
	}
}

// WithSegment creates a new client with an additional scope path segment
func (s *RegistryClientStub) WithSegment(segments ...string) pkg.RegistryClient {
	newRegistry := s.currentRegistry
	if newRegistry == "" {
		// If no current registry, use the first one as base
		for registry := range s.registries {
			newRegistry = registry
			break
		}
	}

	// Append segments to the registry path
	if len(segments) > 0 {
		parts := strings.Split(newRegistry, "/")
		if len(parts) >= 1 {
			// Replace the repository part
			newRegistry = parts[0] + "/" + strings.Join(segments, "/")
		}
	}

	return &RegistryClientStub{
		registries:      s.registries,
		currentRegistry: newRegistry,
	}
}

// GetRegistry returns the full registry path
func (s *RegistryClientStub) GetRegistry() string {
	if s.currentRegistry != "" {
		return s.currentRegistry
	}
	// Return the first registry as default
	for registry := range s.registries {
		return registry
	}
	return ""
}

// GetDigest retrieves the digest for a specific image tag
func (s *RegistryClientStub) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	// First try current registry
	if s.currentRegistry != "" {
		if regData, exists := s.registries[s.currentRegistry]; exists {
			for _, repoData := range regData.repositories {
				if digest, exists := repoData.digests[tag]; exists {
					return digest, nil
				}
			}
		}
	}

	// Fall back to all registries
	for _, regData := range s.registries {
		for _, repoData := range regData.repositories {
			if digest, exists := repoData.digests[tag]; exists {
				return digest, nil
			}
		}
	}
	return nil, fmt.Errorf("digest not found for tag: %s", tag)
}

// GetManifest retrieves the manifest for a specific image tag
func (s *RegistryClientStub) GetManifest(ctx context.Context, tag string) ([]byte, error) {
	// First try current registry
	if s.currentRegistry != "" {
		if regData, exists := s.registries[s.currentRegistry]; exists {
			for _, repoData := range regData.repositories {
				if imageData, exists := repoData.images[tag]; exists {
					return imageData.manifest, nil
				}
			}
		}
	}

	// Fall back to all registries
	for _, regData := range s.registries {
		for _, repoData := range regData.repositories {
			if imageData, exists := repoData.images[tag]; exists {
				return imageData.manifest, nil
			}
		}
	}
	return nil, fmt.Errorf("manifest not found for tag: %s", tag)
}

// GetImageConfig retrieves the image config file
func (s *RegistryClientStub) GetImageConfig(ctx context.Context, tag string) (*v1.ConfigFile, error) {
	// First try current registry
	if s.currentRegistry != "" {
		if regData, exists := s.registries[s.currentRegistry]; exists {
			for _, repoData := range regData.repositories {
				if imageData, exists := repoData.images[tag]; exists {
					return imageData.config, nil
				}
			}
		}
	}

	// Fall back to all registries
	for _, regData := range s.registries {
		for _, repoData := range regData.repositories {
			if imageData, exists := repoData.images[tag]; exists {
				return imageData.config, nil
			}
		}
	}
	return nil, fmt.Errorf("config not found for tag: %s", tag)
}

// CheckImageExists checks if a specific image exists
func (s *RegistryClientStub) CheckImageExists(ctx context.Context, tag string) error {
	// First try current registry
	if s.currentRegistry != "" {
		if regData, exists := s.registries[s.currentRegistry]; exists {
			for _, repoData := range regData.repositories {
				if _, exists := repoData.images[tag]; exists {
					return nil
				}
			}
		}
	}

	// Fall back to all registries
	for _, regData := range s.registries {
		for _, repoData := range regData.repositories {
			if _, exists := repoData.images[tag]; exists {
				return nil
			}
		}
	}
	return fmt.Errorf("image not found: %s", tag)
}

// GetImage retrieves an image for a specific reference
func (s *RegistryClientStub) GetImage(ctx context.Context, tag string) (pkg.RegistryImage, error) {
	// Handle digest references (start with @)
	if strings.HasPrefix(tag, "@") {
		digestStr := strings.TrimPrefix(tag, "@")
		digest, err := v1.NewHash(digestStr)
		if err != nil {
			return nil, fmt.Errorf("invalid digest: %s", digestStr)
		}

		// Find image by digest - first try current registry
		if s.currentRegistry != "" {
			if regData, exists := s.registries[s.currentRegistry]; exists {
				for _, repoData := range regData.repositories {
					for _, imageData := range repoData.images {
						if imageData.digest != nil && imageData.digest.String() == digest.String() {
							return imageData.image, nil
						}
					}
				}
			}
		}

		// Fall back to all registries
		for _, regData := range s.registries {
			for _, repoData := range regData.repositories {
				for _, imageData := range repoData.images {
					if imageData.digest != nil && imageData.digest.String() == digest.String() {
						return imageData.image, nil
					}
				}
			}
		}
		return nil, fmt.Errorf("image not found for digest: %s", digestStr)
	}

	// Handle tag references - first try current registry
	if s.currentRegistry != "" {
		if regData, exists := s.registries[s.currentRegistry]; exists {
			for _, repoData := range regData.repositories {
				if imageData, exists := repoData.images[tag]; exists {
					return imageData.image, nil
				}
			}
		}
	}

	// Fall back to all registries
	for _, regData := range s.registries {
		for _, repoData := range regData.repositories {
			if imageData, exists := repoData.images[tag]; exists {
				return imageData.image, nil
			}
		}
	}
	return nil, fmt.Errorf("image not found for tag: %s", tag)
}

// PushImage pushes an image to the registry
func (s *RegistryClientStub) PushImage(ctx context.Context, tag string, img v1.Image) error {
	// Stub implementation - always succeeds
	return nil
}

// ListTags retrieves all available tags
func (s *RegistryClientStub) ListTags(ctx context.Context) ([]string, error) {
	var allTags []string
	for _, regData := range s.registries {
		for _, repoData := range regData.repositories {
			allTags = append(allTags, repoData.tags...)
		}
	}
	return allTags, nil
}

// ListRepositories retrieves all sub-repositories
func (s *RegistryClientStub) ListRepositories(ctx context.Context) ([]string, error) {
	var allRepos []string
	for _, regData := range s.registries {
		for repo := range regData.repositories {
			allRepos = append(allRepos, repo)
		}
	}
	return allRepos, nil
}
