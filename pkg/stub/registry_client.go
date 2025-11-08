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
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/image"
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
	tag      string
	version  string
}

// StubLayer provides a simple stub implementation of v1.Layer
type StubLayer struct {
	digest v1.Hash
	size   int64
	data   []byte // gzipped tar data
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
	// Determine the version based on the tag
	version := r.version
	if version == "" {
		// Fallback: parse the tag to determine version
		parts := strings.Split(r.tag, ":")
		if len(parts) == 2 {
			tag := parts[1]
			if strings.Contains(r.tag, "/release-channel:") {
				// For release channels, map to semver versions
				switch tag {
				case "alpha":
					version = "v1.72.10"
				case "beta":
					version = "v1.71.0"
				case "early-access":
					version = "v1.70.0"
				case "stable":
					version = "v1.69.0"
				case "rock-solid":
					version = "v1.68.0"
				default:
					if strings.HasPrefix(tag, "v") {
						version = tag
					} else {
						version = "v1.72.10"
					}
				}
			} else if strings.HasPrefix(tag, "v") {
				version = tag
			} else {
				version = "v1.72.10"
			}
		} else {
			version = "v1.72.10"
		}
	}

	// Create uncompressed tar
	var tarBuf bytes.Buffer
	tarWriter := tar.NewWriter(&tarBuf)

	// Add directory headers
	tarWriter.WriteHeader(&tar.Header{
		Name:     "deckhouse/",
		Typeflag: tar.TypeDir,
		Mode:     0755,
		ModTime:  time.Now(),
	})
	tarWriter.WriteHeader(&tar.Header{
		Name:     "deckhouse/candi/",
		Typeflag: tar.TypeDir,
		Mode:     0755,
		ModTime:  time.Now(),
	})

	// Add changelog.yaml file
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
	if err := tarWriter.WriteHeader(changelogHeader); err != nil {
		return nil, err
	}
	if _, err := tarWriter.Write([]byte(changelogData)); err != nil {
		return nil, err
	}

	// Add version.json file
	versionData := fmt.Sprintf(`{"version":"%s"}`, version)
	versionHeader := &tar.Header{
		Name:     "version.json",
		Mode:     0644,
		Size:     int64(len(versionData)),
		ModTime:  time.Now(),
		Typeflag: tar.TypeReg,
	}
	if err := tarWriter.WriteHeader(versionHeader); err != nil {
		return nil, err
	}
	if _, err := tarWriter.Write([]byte(versionData)); err != nil {
		return nil, err
	}

	// Add deckhouse/candi/images_tags.json file
	imagesTagsData := `{}`
	imagesTagsHeader := &tar.Header{
		Name:     "deckhouse/candi/images_tags.json",
		Mode:     0644,
		Size:     int64(len(imagesTagsData)),
		ModTime:  time.Now(),
		Typeflag: tar.TypeReg,
	}
	if err := tarWriter.WriteHeader(imagesTagsHeader); err != nil {
		return nil, err
	}
	if _, err := tarWriter.Write([]byte(imagesTagsData)); err != nil {
		return nil, err
	}

	tarWriter.Close()

	// Now gzip the tar
	var gzipBuf bytes.Buffer
	gzipWriter := gzip.NewWriter(&gzipBuf)
	if _, err := gzipWriter.Write(tarBuf.Bytes()); err != nil {
		return nil, err
	}
	gzipWriter.Close()

	digest, _ := v1.NewHash("sha256:e692418e4cbaf90ca69d05a66403747baa33ee08806650b51fab815ad7fc331f")

	return []v1.Layer{
		&StubLayer{
			digest: digest,
			size:   int64(gzipBuf.Len()),
			data:   gzipBuf.Bytes(), // gzipped tar
		},
	}, nil
}

// Extract implements RegistryImage
func (r *RegistryImageStub) Extract() io.ReadCloser {
	// Determine the version based on the tag
	version := r.version
	if version == "" {
		// Fallback: parse the tag to determine version
		parts := strings.Split(r.tag, ":")
		if len(parts) == 2 {
			tag := parts[1]
			if strings.Contains(r.tag, "/release-channel:") {
				// For release channels, map to semver versions
				switch tag {
				case "alpha":
					version = "v1.72.10"
				case "beta":
					version = "v1.71.0"
				case "early-access":
					version = "v1.70.0"
				case "stable":
					version = "v1.69.0"
				case "rock-solid":
					version = "v1.68.0"
				default:
					if strings.HasPrefix(tag, "v") {
						version = tag
					} else {
						version = "v1.72.10"
					}
				}
			} else if strings.HasPrefix(tag, "v") {
				version = tag
			} else {
				version = "v1.72.10"
			}
		} else {
			version = "v1.72.10"
		}
	}

	// Create uncompressed tar
	var tarBuf bytes.Buffer
	tarWriter := tar.NewWriter(&tarBuf)

	// Add directory headers
	tarWriter.WriteHeader(&tar.Header{
		Name:     "deckhouse/",
		Typeflag: tar.TypeDir,
		Mode:     0755,
		ModTime:  time.Now(),
	})
	tarWriter.WriteHeader(&tar.Header{
		Name:     "deckhouse/candi/",
		Typeflag: tar.TypeDir,
		Mode:     0755,
		ModTime:  time.Now(),
	})

	// Add changelog.yaml file
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
	if err := tarWriter.WriteHeader(changelogHeader); err != nil {
		return io.NopCloser(strings.NewReader(""))
	}
	if _, err := tarWriter.Write([]byte(changelogData)); err != nil {
		return io.NopCloser(strings.NewReader(""))
	}

	// Add version.json file
	versionData := fmt.Sprintf(`{"version":"%s"}`, version)
	versionHeader := &tar.Header{
		Name:     "version.json",
		Mode:     0644,
		Size:     int64(len(versionData)),
		ModTime:  time.Now(),
		Typeflag: tar.TypeReg,
	}
	if err := tarWriter.WriteHeader(versionHeader); err != nil {
		return io.NopCloser(strings.NewReader(""))
	}
	if _, err := tarWriter.Write([]byte(versionData)); err != nil {
		return io.NopCloser(strings.NewReader(""))
	}

	// Add deckhouse/candi/images_tags.json file
	imagesTagsData := `{}`
	imagesTagsHeader := &tar.Header{
		Name:     "deckhouse/candi/images_tags.json",
		Mode:     0644,
		Size:     int64(len(imagesTagsData)),
		ModTime:  time.Now(),
		Typeflag: tar.TypeReg,
	}
	if err := tarWriter.WriteHeader(imagesTagsHeader); err != nil {
		return io.NopCloser(strings.NewReader(""))
	}
	if _, err := tarWriter.Write([]byte(imagesTagsData)); err != nil {
		return io.NopCloser(strings.NewReader(""))
	}

	tarWriter.Close()

	return io.NopCloser(bytes.NewBuffer(tarBuf.Bytes()))
}

// GetMetadata implements RegistryImage
func (r *RegistryImageStub) GetMetadata() (pkg.ImageMeta, error) {
	return image.NewImageMeta(r.tag, fmt.Sprintf("sha256:%s", r.digest.String()), &r.digest), nil
}

func (r *RegistryImageStub) SetMetadata(pkg.ImageMeta) {
	// No-op for mock
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

// NewRegistryClientStub creates a new stub registry client
func NewRegistryClientStub() pkg.RegistryClient {
	stub := &RegistryClientStub{
		registries: make(map[string]*RegistryData),
	}
	stub.initializeRegistries()
	return stub
}

// getSourceFromArgs parses the command line arguments to find the --source flag value
func getSourceFromArgs() string {
	args := os.Args
	for i, arg := range args {
		if arg == "--source" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--source=") {
			return strings.TrimPrefix(arg, "--source=")
		}
	}

	return "registry.deckhouse.ru/deckhouse/fe" // default
}

// initializeRegistries sets up mock data for multiple registries
func (s *RegistryClientStub) initializeRegistries() {
	source := getSourceFromArgs()

	// Registry 1: dynamic source
	s.addRegistry(source, map[string][]string{
		"":                   {"alpha", "beta", "early-access", "stable", "rock-solid", "v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0"},
		"release-channel":    {"alpha", "beta", "early-access", "stable", "rock-solid"}, // channel names, not versions
		"install":            {"v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0", "alpha", "beta", "early-access", "stable", "rock-solid"},
		"install-standalone": {"v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0", "alpha", "beta", "early-access", "stable", "rock-solid"},
	})

	// Registry 2: gcr.io
	s.addRegistry("gcr.io/google-containers", map[string][]string{
		"":                   {"alpha", "beta", "early-access", "stable", "rock-solid", "v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0"},
		"pause":              {"3.9", "latest"},
		"kube-apiserver":     {"v1.28.0", "v1.29.0", "latest"},
		"release-channel":    {"alpha", "beta", "early-access", "stable", "rock-solid"},
		"install":            {"v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0", "alpha", "beta", "early-access", "stable", "rock-solid"},
		"install-standalone": {"v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0", "alpha", "beta", "early-access", "stable", "rock-solid"},
	})

	// Registry 3: quay.io
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

// generateUniqueDigest creates a unique SHA256 digest based on the image reference
func generateUniqueDigest(registry, repo, tag string) v1.Hash {
	data := fmt.Sprintf("%s/%s:%s", registry, repo, tag)
	hash := sha256.Sum256([]byte(data))
	digestStr := fmt.Sprintf("sha256:%x", hash)
	digest, _ := v1.NewHash(digestStr)
	return digest
}

// findRegistryAndRepo finds the registry and repo from currentRegistry
func (s *RegistryClientStub) findRegistryAndRepo() (registry, repo string) {
	if s.currentRegistry == "" {
		return "", ""
	}

	// Find the longest registry key that is a prefix of currentRegistry
	for reg := range s.registries {
		if strings.HasPrefix(s.currentRegistry, reg+"/") || s.currentRegistry == reg {
			if len(reg) > len(registry) {
				registry = reg
			}
		}
	}

	if registry == "" {
		return s.currentRegistry, ""
	}

	if s.currentRegistry == registry {
		return registry, ""
	}

	repo = strings.TrimPrefix(s.currentRegistry, registry+"/")
	return registry, repo
}

// createMockImageData creates mock image data with manifest, config, and registry image
func (s *RegistryClientStub) createMockImageData(reg, repo, tag string) *ImageData {
	// Create a unique digest based on the image reference
	digest := generateUniqueDigest(reg, repo, tag)

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
			Image: fmt.Sprintf("%s/%s:%s", reg, repo, tag),
			Labels: map[string]string{
				"org.opencontainers.image.created": "2024-01-01T00:00:00Z",
				"org.opencontainers.image.version": tag,
				"org.opencontainers.image.source":  fmt.Sprintf("https://%s", reg),
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
				Comment: fmt.Sprintf("Build for %s/%s:%s", reg, repo, tag),
			},
		},
	}

	// Determine the version to put in version.json based on the tag
	version := tag
	if repo == "release-channel" {
		// For release channels, map to semver versions
		switch tag {
		case "alpha":
			version = "v1.72.10"
		case "beta":
			version = "v1.71.0"
		case "early-access":
			version = "v1.70.0"
		case "stable":
			version = "v1.69.0"
		case "rock-solid":
			version = "v1.68.0"
		default:
			// If it's already a semver version, use it
			if strings.HasPrefix(tag, "v") {
				version = tag
			} else {
				version = "v1.72.10" // fallback
			}
		}
	} else if strings.HasPrefix(tag, "v") {
		version = tag
	} else {
		version = "v1.72.10" // fallback for other tags
	}

	// Create mock registry image (v1.Image implementation)
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
		config:  config,
		digest:  digest,
		tag:     fmt.Sprintf("%s/%s:%s", reg, repo, tag),
		version: version, // Store the version for the Extract method
	}

	// Create real registry.Image wrapping the mock v1.Image
	tagReference := fmt.Sprintf("%s/%s:%s", reg, repo, tag)
	digestReference := fmt.Sprintf("%s/%s@%s", reg, repo, digest.String())
	_ = image.NewImageMeta(tagReference, digestReference, &digest)

	// Always use the stub directly to ensure Extract method works
	var registryImage pkg.RegistryImage = imageStub

	return &ImageData{
		manifest: manifest,
		config:   config,
		image:    registryImage,
		digest:   &digest,
		exists:   true,
	}
}

// WithSegment creates a new client with an additional scope path segment
func (s *RegistryClientStub) WithSegment(segments ...string) pkg.RegistryClient {
	newRegistry := s.currentRegistry
	if len(segments) > 0 {
		if newRegistry == "" {
			newRegistry = strings.Join(segments, "/")
		} else {
			newRegistry = newRegistry + "/" + strings.Join(segments, "/")
		}
	}

	return &RegistryClientStub{
		registries:      s.registries,
		currentRegistry: newRegistry,
	}
}

// GetRegistry returns the full registry path
func (s *RegistryClientStub) GetRegistry() string {
	if s.currentRegistry == "" {
		for registry := range s.registries {
			return registry
		}
		return ""
	}

	registry, _ := s.findRegistryAndRepo()
	return registry
}

// GetDigest retrieves the digest for a specific image tag
func (s *RegistryClientStub) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	registry, repo := s.findRegistryAndRepo()

	if regData, exists := s.registries[registry]; exists {
		if repoData, exists := regData.repositories[repo]; exists {
			if digest, exists := repoData.digests[tag]; exists {
				return digest, nil
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
	registry, repo := s.findRegistryAndRepo()

	if regData, exists := s.registries[registry]; exists {
		if repoData, exists := regData.repositories[repo]; exists {
			if imageData, exists := repoData.images[tag]; exists {
				return imageData.manifest, nil
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
	registry, repo := s.findRegistryAndRepo()

	if regData, exists := s.registries[registry]; exists {
		if repoData, exists := regData.repositories[repo]; exists {
			if imageData, exists := repoData.images[tag]; exists {
				return imageData.config, nil
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
	registry, repo := s.findRegistryAndRepo()

	if regData, exists := s.registries[registry]; exists {
		if repoData, exists := regData.repositories[repo]; exists {
			if _, exists := repoData.images[tag]; exists {
				return nil
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
func (s *RegistryClientStub) GetImage(ctx context.Context, tag string, opts ...pkg.ImageGetOption) (pkg.ClientImage, error) {
	// Handle digest references (start with @)
	if strings.HasPrefix(tag, "@") {
		digestStr := strings.TrimPrefix(tag, "@")
		digest, err := v1.NewHash(digestStr)
		if err != nil {
			return nil, fmt.Errorf("invalid digest: %s", digestStr)
		}

		// Find image by digest
		for _, regData := range s.registries {
			for _, repoData := range regData.repositories {
				for _, imageData := range repoData.images {
					if imageData.digest != nil && imageData.digest.String() == digest.String() {
						return imageData.image.(pkg.ClientImage), nil
					}
				}
			}
		}
		return nil, fmt.Errorf("image not found for digest: %s", digestStr)
	}

	registry, repo := s.findRegistryAndRepo()

	if regData, exists := s.registries[registry]; exists {
		if repoData, exists := regData.repositories[repo]; exists {
			if imageData, exists := repoData.images[tag]; exists {
				return imageData.image.(pkg.ClientImage), nil
			}
		}
	}

	// Fall back to all registries
	for _, regData := range s.registries {
		for _, repoData := range regData.repositories {
			if imageData, exists := repoData.images[tag]; exists {
				return imageData.image.(pkg.ClientImage), nil
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
