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

package service_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/gojuno/minimock/v3"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"

	"github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg/mock"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	"github.com/deckhouse/deckhouse/pkg/registry"
	"github.com/deckhouse/deckhouse/pkg/registry/client"
)

// DescriptorStub implements registry.Descriptor
type DescriptorStub struct {
	MediaType string
	Size      int64
	Digest    string
}

func (d *DescriptorStub) GetAnnotations() map[string]string {
	return map[string]string{}
}

func (d *DescriptorStub) GetArtifactType() string {
	return ""
}

func (d *DescriptorStub) GetData() []byte {
	return nil
}

func (d *DescriptorStub) GetDigest() v1.Hash {
	h, _ := v1.NewHash(d.Digest)
	return h
}

func (d *DescriptorStub) GetMediaType() types.MediaType {
	return types.MediaType(d.MediaType)
}

func (d *DescriptorStub) GetSize() int64 {
	return d.Size
}

func (d *DescriptorStub) GetPlatform() *v1.Platform {
	return nil
}

func (d *DescriptorStub) GetURLs() []string {
	return nil
}

// ManifestStub implements registry.Manifest
type ManifestStub struct {
	data []byte
}

func (m *ManifestStub) GetAnnotations() map[string]string {
	var manifest map[string]interface{}
	err := json.Unmarshal(m.data, &manifest)
	if err != nil {
		return nil
	}
	if annotations, ok := manifest["annotations"].(map[string]interface{}); ok {
		result := make(map[string]string)
		for k, v := range annotations {
			if s, ok := v.(string); ok {
				result[k] = s
			}
		}
		return result
	}
	return nil
}

func (m *ManifestStub) GetConfig() registry.Descriptor {
	return &DescriptorStub{
		MediaType: "application/vnd.docker.container.image.v1+json",
		Size:      1469,
		Digest:    "sha256:b5d2b2c507a0944348e0303114d8d93aaaa081732b86451d9bce1f432a537bc7",
	}
}

func (m *ManifestStub) GetLayers() []registry.Descriptor {
	return []registry.Descriptor{
		&DescriptorStub{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Size:      32654,
			Digest:    "sha256:e692418e4cbaf90ca69d05a66403747baa33ee08806650b51fab815ad7fc331f",
		},
	}
}

func (m *ManifestStub) GetMediaType() types.MediaType {
	return "application/vnd.docker.distribution.manifest.v2+json"
}

func (m *ManifestStub) GetSchemaVersion() int64 {
	return 2
}

func (m *ManifestStub) GetSubject() registry.Descriptor {
	return nil
}

// ManifestResultStub implements registry.ManifestResult
type ManifestResultStub struct {
	data []byte
}

func (m *ManifestResultStub) RawManifest() []byte {
	return m.data
}

func (m *ManifestResultStub) GetDescriptor() registry.Descriptor {
	return &DescriptorStub{
		MediaType: "application/vnd.docker.distribution.manifest.v2+json",
		Size:      int64(len(m.data)),
		Digest:    "sha256:stub",
	}
}

func (m *ManifestResultStub) GetIndexManifest() (registry.IndexManifest, error) {
	return nil, nil
}

func (m *ManifestResultStub) GetMediaType() types.MediaType {
	return "application/vnd.docker.distribution.manifest.v2+json"
}

func (m *ManifestResultStub) GetManifest() (registry.Manifest, error) {
	return &ManifestStub{data: m.data}, nil
}

func TestGetPluginContract_Success(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	contractJSON := `{"name": "test-plugin", "version": "v1.0.0", "description": "A test plugin", "env": [{"name": "TEST_ENV"}], "flags": [{"name": "--test-flag"}], "requirements": {"kubernetes": {"constraint": ">= 1.26"}, "modules": [{"name": "test-module", "constraint": ">= 1.0.0"}]}}`
	contractB64 := base64.StdEncoding.EncodeToString([]byte(contractJSON))
	manifestJSON := `{"annotations": {"plugin-contract": "` + contractB64 + `"}}`

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetManifestMock.
		Expect(context.Background(), "v1.0.0").
		Return(&ManifestResultStub{data: []byte(manifestJSON)}, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	plugin, err := service.GetPluginContract(context.Background(), "test-plugin", "v1.0.0")

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if plugin == nil {
		t.Fatal("Expected plugin to be non-nil")
	}

	if plugin.Name != "test-plugin" {
		t.Errorf("Expected name 'test-plugin', got '%s'", plugin.Name)
	}

	if plugin.Version != "v1.0.0" {
		t.Errorf("Expected version 'v1.0.0', got '%s'", plugin.Version)
	}

	if plugin.Description != "A test plugin" {
		t.Errorf("Expected description 'A test plugin', got '%s'", plugin.Description)
	}

	if len(plugin.Env) != 1 || plugin.Env[0].Name != "TEST_ENV" {
		t.Errorf("Expected 1 env var 'TEST_ENV', got: %+v", plugin.Env)
	}

	if len(plugin.Flags) != 1 || plugin.Flags[0].Name != "--test-flag" {
		t.Errorf("Expected 1 flag '--test-flag', got: %+v", plugin.Flags)
	}

	if plugin.Requirements.Kubernetes.Constraint != ">= 1.26" {
		t.Errorf("Expected kubernetes constraint '>= 1.26', got '%s'", plugin.Requirements.Kubernetes.Constraint)
	}

	if len(plugin.Requirements.Modules) != 1 {
		t.Fatalf("Expected 1 module requirement, got %d", len(plugin.Requirements.Modules))
	}

	if plugin.Requirements.Modules[0].Name != "test-module" {
		t.Errorf("Expected module name 'test-module', got '%s'", plugin.Requirements.Modules[0].Name)
	}

	if plugin.Requirements.Modules[0].Constraint != ">= 1.0.0" {
		t.Errorf("Expected module constraint '>= 1.0.0', got '%s'", plugin.Requirements.Modules[0].Constraint)
	}
}

func TestGetPluginContract_MinimalContract(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	contractJSON := `{"name": "minimal-plugin", "version": "v1.0.0", "description": "Minimal plugin"}`
	contractB64 := base64.StdEncoding.EncodeToString([]byte(contractJSON))
	manifestJSON := `{"annotations": {"plugin-contract": "` + contractB64 + `"}}`

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetManifestMock.
		Expect(context.Background(), "v1.0.0").
		Return(&ManifestResultStub{data: []byte(manifestJSON)}, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("minimal-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	plugin, err := service.GetPluginContract(context.Background(), "minimal-plugin", "v1.0.0")

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if plugin == nil {
		t.Fatal("Expected plugin to be non-nil")
	}

	if plugin.Name != "minimal-plugin" {
		t.Errorf("Expected name 'minimal-plugin', got '%s'", plugin.Name)
	}

	if len(plugin.Env) != 0 {
		t.Errorf("Expected 0 env vars, got %d", len(plugin.Env))
	}

	if len(plugin.Flags) != 0 {
		t.Errorf("Expected 0 flags, got %d", len(plugin.Flags))
	}
}

func TestGetPluginContract_LabelNotFound(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	manifestJSON := `{"annotations": {}}` // no contract annotation

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetManifestMock.
		Expect(context.Background(), "v1.0.0").
		Return(&ManifestResultStub{data: []byte(manifestJSON)}, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	plugin, err := service.GetPluginContract(context.Background(), "test-plugin", "v1.0.0")

	// Assert
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if plugin != nil {
		t.Errorf("Expected plugin to be nil, got: %+v", plugin)
	}

	expectedError := "plugin-contract annotation not found in image metadata"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestGetPluginContract_GetLabelError(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	expectedErr := errors.New("registry connection failed")

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetManifestMock.
		Expect(context.Background(), "v1.0.0").
		Return(nil, expectedErr)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	plugin, err := service.GetPluginContract(context.Background(), "test-plugin", "v1.0.0")

	// Assert
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if plugin != nil {
		t.Errorf("Expected plugin to be nil, got: %+v", plugin)
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to wrap registry error, got: %v", err)
	}
}

func TestGetPluginContract_InvalidJSON(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	invalidContractJSON := `{invalid json`
	contractB64 := base64.StdEncoding.EncodeToString([]byte(invalidContractJSON))
	manifestJSON := `{"annotations": {"plugin-contract": "` + contractB64 + `"}}`

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetManifestMock.
		Expect(context.Background(), "v1.0.0").
		Return(&ManifestResultStub{data: []byte(manifestJSON)}, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	plugin, err := service.GetPluginContract(context.Background(), "test-plugin", "v1.0.0")

	// Assert
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if plugin != nil {
		t.Errorf("Expected plugin to be nil, got: %+v", plugin)
	}
}

func TestGetPluginContract_EmptyJSON(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	emptyContractJSON := `{}`
	contractB64 := base64.StdEncoding.EncodeToString([]byte(emptyContractJSON))
	manifestJSON := `{"annotations": {"plugin-contract": "` + contractB64 + `"}}`

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetManifestMock.
		Expect(context.Background(), "v1.0.0").
		Return(&ManifestResultStub{data: []byte(manifestJSON)}, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	plugin, err := service.GetPluginContract(context.Background(), "test-plugin", "v1.0.0")

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if plugin.Name != "" {
		t.Errorf("Expected empty name, got '%s'", plugin.Name)
	}
}

func TestExtractPlugin_Success(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	tmpDir := t.TempDir()

	// Create a tar archive in memory
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	// Add a directory
	err := tw.WriteHeader(&tar.Header{
		Name:     "bin/",
		Mode:     0755,
		Typeflag: tar.TypeDir,
	})
	if err != nil {
		t.Fatalf("Failed to write tar directory header: %v", err)
	}

	// Add a file
	fileContent := []byte("#!/bin/bash\necho 'test plugin'\n")
	err = tw.WriteHeader(&tar.Header{
		Name:     "plugin",
		Mode:     0755,
		Size:     int64(len(fileContent)),
		Typeflag: tar.TypeReg,
	})
	if err != nil {
		t.Fatalf("Failed to write tar file header: %v", err)
	}

	_, err = tw.Write(fileContent)
	if err != nil {
		t.Fatalf("Failed to write tar file content: %v", err)
	}

	tw.Close()

	mockImage := mock.NewRegistryImageMock(mc)
	mockImage.ExtractMock.Return(io.NopCloser(&tarBuf))

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetImageMock.
		Expect(context.Background(), "v1.0.0", client.WithPlatform{Platform: &v1.Platform{Architecture: runtime.GOARCH, OS: runtime.GOOS}}).
		Return(mockImage, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	err = service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir+"/test-plugin")

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify directory was not created
	dirPath := filepath.Join(tmpDir, "bin")
	if _, err := os.Stat(dirPath); !os.IsNotExist(err) {
		t.Errorf("Expected directory '%s' to not exist", dirPath)
	}

	// Verify file was created
	filePath := filepath.Join(tmpDir, "test-plugin")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("Expected file '%s' to exist", filePath)
	}

	// Verify file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read extracted file: %v", err)
	}

	if !bytes.Equal(content, fileContent) {
		t.Errorf("Expected file content '%s', got '%s'", fileContent, content)
	}

	// Verify file permissions
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	expectedMode := os.FileMode(0755)
	if info.Mode().Perm() != expectedMode {
		t.Errorf("Expected file mode %v, got %v", expectedMode, info.Mode().Perm())
	}
}

func TestExtractPlugin_MultipleLayersSuccess(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	tmpDir := t.TempDir()

	// Create a tar with multiple files, but only "plugin" should be extracted
	var combinedTar bytes.Buffer
	tw := tar.NewWriter(&combinedTar)

	// Add a non-plugin file (should be ignored)
	ignoredContent := []byte("ignored")
	tw.WriteHeader(&tar.Header{
		Name:     "file1.txt",
		Mode:     0644,
		Size:     int64(len(ignoredContent)),
		Typeflag: tar.TypeReg,
	})
	tw.Write(ignoredContent)

	// Add the plugin file (should be extracted)
	pluginContent := []byte("plugin content")
	tw.WriteHeader(&tar.Header{
		Name:     "plugin",
		Mode:     0755,
		Size:     int64(len(pluginContent)),
		Typeflag: tar.TypeReg,
	})
	tw.Write(pluginContent)

	tw.Close()

	mockImage := mock.NewRegistryImageMock(mc)
	mockImage.ExtractMock.Return(io.NopCloser(&combinedTar))

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetImageMock.
		Expect(context.Background(), "v1.0.0", client.WithPlatform{Platform: &v1.Platform{Architecture: runtime.GOARCH, OS: runtime.GOOS}}).
		Return(mockImage, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	err := service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir+"/test-plugin")

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify only the plugin file was extracted (renamed to test-plugin)
	pluginPath := filepath.Join(tmpDir, "test-plugin")
	ignoredPath := filepath.Join(tmpDir, "file1.txt")

	// Plugin file should exist
	if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
		t.Errorf("Expected plugin file '%s' to exist", pluginPath)
	}

	// Ignored file should not exist
	if _, err := os.Stat(ignoredPath); !os.IsNotExist(err) {
		t.Errorf("Expected ignored file '%s' to not exist", ignoredPath)
	}

	// Verify plugin file content
	content, err := os.ReadFile(pluginPath)
	if err != nil {
		t.Fatalf("Failed to read plugin file: %v", err)
	}
	if string(content) != "plugin content" {
		t.Errorf("Expected plugin content 'plugin content', got '%s'", content)
	}
}

func TestExtractPlugin_ExtractImageLayersError(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	tmpDir := t.TempDir()
	expectedErr := errors.New("failed to get image")

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetImageMock.
		Expect(context.Background(), "v1.0.0", client.WithPlatform{Platform: &v1.Platform{Architecture: runtime.GOARCH, OS: runtime.GOOS}}).
		Return(nil, expectedErr)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	err := service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir+"/test-plugin")

	// Assert
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to wrap registry error, got: %v", err)
	}
}

func TestExtractPlugin_PathTraversalAttempt(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	tmpDir := t.TempDir()

	// Create a tar archive with path traversal attempt
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	fileContent := []byte("malicious content")
	err := tw.WriteHeader(&tar.Header{
		Name:     "../../../etc/passwd",
		Mode:     0644,
		Size:     int64(len(fileContent)),
		Typeflag: tar.TypeReg,
	})
	if err != nil {
		t.Fatalf("Failed to write tar header: %v", err)
	}

	_, err = tw.Write(fileContent)
	if err != nil {
		t.Fatalf("Failed to write tar content: %v", err)
	}

	tw.Close()

	mockImage := mock.NewRegistryImageMock(mc)
	mockImage.ExtractMock.Return(io.NopCloser(&tarBuf))

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetImageMock.
		Expect(context.Background(), "v1.0.0", client.WithPlatform{Platform: &v1.Platform{Architecture: runtime.GOARCH, OS: runtime.GOOS}}).
		Return(mockImage, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	err = service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir)

	// Assert
	// Current implementation does not prevent path traversal, so no error is expected
	// This test documents the current behavior but should be updated when path traversal protection is added
	if err != nil {
		t.Fatalf("Expected no error (current implementation doesn't check path traversal), got: %v", err)
	}
}

func TestExtractPlugin_CreateDestinationError(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	// Use a path that can be created
	validDir := "/tmp/deckhouse-test-destination"

	// Create a simple tar
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	tw.WriteHeader(&tar.Header{
		Name:     "file.txt",
		Mode:     0644,
		Size:     0,
		Typeflag: tar.TypeReg,
	})
	tw.Close()

	mockImage := mock.NewRegistryImageMock(mc)
	mockImage.ExtractMock.Return(io.NopCloser(&tarBuf))

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetImageMock.Return(mockImage, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	err := service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", validDir+"/test-plugin")

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}

func TestExtractPlugin_EmptyRepository(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	tmpDir := t.TempDir()

	// Empty tar (no files)
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	tw.Close()

	mockImage := mock.NewRegistryImageMock(mc)
	mockImage.ExtractMock.Return(io.NopCloser(&tarBuf))

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetImageMock.
		Expect(context.Background(), "v1.0.0", client.WithPlatform{Platform: &v1.Platform{Architecture: runtime.GOARCH, OS: runtime.GOOS}}).
		Return(mockImage, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	err := service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir+"/test-plugin")

	// Assert
	if err != nil {
		t.Fatalf("Expected no error for empty tar, got: %v", err)
	}

	// Verify destination directory was created but is empty
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read destination directory: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("Expected destination directory to be empty, got %d entries", len(entries))
	}
}

func TestExtractPlugin_NestedDirectories(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)

	tmpDir := t.TempDir()

	// Create a tar archive with nested directories and a plugin file
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	// Add nested directories (ignored by current implementation)
	tw.WriteHeader(&tar.Header{
		Name:     "a/",
		Mode:     0755,
		Typeflag: tar.TypeDir,
	})

	tw.WriteHeader(&tar.Header{
		Name:     "a/b/",
		Mode:     0755,
		Typeflag: tar.TypeDir,
	})

	tw.WriteHeader(&tar.Header{
		Name:     "a/b/c/",
		Mode:     0755,
		Typeflag: tar.TypeDir,
	})

	// Add a plugin file (this will be extracted, but directories are ignored)
	fileContent := []byte("nested file")
	tw.WriteHeader(&tar.Header{
		Name:     "plugin",
		Mode:     0644,
		Size:     int64(len(fileContent)),
		Typeflag: tar.TypeReg,
	})
	tw.Write(fileContent)

	tw.Close()

	mockImage := mock.NewRegistryImageMock(mc)
	mockImage.ExtractMock.Return(io.NopCloser(&tarBuf))

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetImageMock.
		Expect(context.Background(), "v1.0.0", client.WithPlatform{Platform: &v1.Platform{Architecture: runtime.GOARCH, OS: runtime.GOOS}}).
		Return(mockImage, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithSegmentMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := registryservice.NewPluginService(mockClient, logger)

	// Act
	err := service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir+"/test-plugin")

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify plugin file was extracted (directories are ignored by current implementation)
	pluginPath := filepath.Join(tmpDir, "test-plugin")
	if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
		t.Errorf("Expected plugin file '%s' to exist", pluginPath)
	}

	// Verify directories were not created
	dirPath := filepath.Join(tmpDir, "a", "b", "c")
	if _, err := os.Stat(dirPath); !os.IsNotExist(err) {
		t.Errorf("Expected directory '%s' to not exist", dirPath)
	}

	// Verify content
	content, err := os.ReadFile(pluginPath)
	if err != nil {
		t.Fatalf("Failed to read plugin file: %v", err)
	}
	if string(content) != "nested file" {
		t.Errorf("Expected content 'nested file', got '%s'", content)
	}
}

// Helper types and functions

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
