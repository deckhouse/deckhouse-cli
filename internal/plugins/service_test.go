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

package plugins_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/plugins"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/mock"
	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/gojuno/minimock/v3"
)

func TestGetPluginContract_Success(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)
	defer mc.Finish()

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetLabelMock.
		Expect(
			minimock.AnyContext,
			"v1.0.0",
			"plugin-contract",
		).
		Return(`{
            "name": "test-plugin",
            "version": "v1.0.0",
            "description": "A test plugin",
            "env": [{"name": "TEST_ENV"}],
            "flags": [{"name": "--test-flag"}],
            "requirements": {
                "kubernetes": {"constraint": ">= 1.26"},
                "modules": [
                    {"name": "test-module", "constraint": ">= 1.0.0"}
                ]
            }
        }`, true, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

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
	defer mc.Finish()

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetLabelMock.
		Expect(
			minimock.AnyContext,
			"v1.0.0",
			"plugin-contract",
		).
		Return(`{
			"name": "minimal-plugin",
			"version": "v1.0.0",
			"description": "Minimal plugin"
		}`, true, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("minimal-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

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
	defer mc.Finish()

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetLabelMock.
		Expect(
			minimock.AnyContext,
			"v1.0.0",
			"plugin-contract",
		).
		Return("", false, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

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
	defer mc.Finish()

	expectedErr := errors.New("registry connection failed")
	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetLabelMock.
		Expect(
			minimock.AnyContext,
			"v1.0.0",
			"plugin-contract",
		).
		Return("", false, expectedErr)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

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
	defer mc.Finish()

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetLabelMock.
		Expect(
			minimock.AnyContext,
			"v1.0.0",
			"plugin-contract",
		).
		Return(`{invalid json`, true, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

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
	defer mc.Finish()

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.GetLabelMock.
		Expect(
			minimock.AnyContext,
			"v1.0.0",
			"plugin-contract",
		).
		Return(`{}`, true, nil)

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

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
	defer mc.Finish()

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
		Name:     "bin/plugin",
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

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.ExtractImageLayersMock.
		Set(func(ctx context.Context, tag string, handler func(pkg.LayerStream) error) error {
			if tag != "v1.0.0" {
				t.Errorf("Expected tag 'v1.0.0', got '%s'", tag)
			}

			// Create a mock layer stream
			stream := &mockLayerStream{
				index:  1,
				total:  1,
				reader: io.NopCloser(bytes.NewReader(tarBuf.Bytes())),
			}

			return handler(stream)
		})

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

	// Act
	err = service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify directory was created
	dirPath := filepath.Join(tmpDir, "bin")
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		t.Errorf("Expected directory '%s' to exist", dirPath)
	}

	// Verify file was created
	filePath := filepath.Join(tmpDir, "bin", "plugin")
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
	defer mc.Finish()

	tmpDir := t.TempDir()

	// Create two tar archives (simulating multiple layers)
	createTarLayer := func(filename, content string) *bytes.Buffer {
		var tarBuf bytes.Buffer
		tw := tar.NewWriter(&tarBuf)

		fileBytes := []byte(content)
		err := tw.WriteHeader(&tar.Header{
			Name:     filename,
			Mode:     0644,
			Size:     int64(len(fileBytes)),
			Typeflag: tar.TypeReg,
		})
		if err != nil {
			t.Fatalf("Failed to write tar header: %v", err)
		}

		_, err = tw.Write(fileBytes)
		if err != nil {
			t.Fatalf("Failed to write tar content: %v", err)
		}

		tw.Close()
		return &tarBuf
	}

	layer1 := createTarLayer("file1.txt", "content1")
	layer2 := createTarLayer("file2.txt", "content2")

	callCount := 0
	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.ExtractImageLayersMock.
		Set(func(ctx context.Context, tag string, handler func(pkg.LayerStream) error) error {
			// Simulate two layers
			layers := []io.Reader{layer1, layer2}

			for i, layer := range layers {
				stream := &mockLayerStream{
					index:  i + 1,
					total:  2,
					reader: io.NopCloser(layer),
				}

				if err := handler(stream); err != nil {
					return err
				}
				callCount++
			}

			return nil
		})

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

	// Act
	err := service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if callCount != 2 {
		t.Errorf("Expected handler to be called 2 times, got %d", callCount)
	}

	// Verify both files were created
	file1Path := filepath.Join(tmpDir, "file1.txt")
	file2Path := filepath.Join(tmpDir, "file2.txt")

	if _, err := os.Stat(file1Path); os.IsNotExist(err) {
		t.Errorf("Expected file '%s' to exist", file1Path)
	}

	if _, err := os.Stat(file2Path); os.IsNotExist(err) {
		t.Errorf("Expected file '%s' to exist", file2Path)
	}

	// Verify file contents
	content1, _ := os.ReadFile(file1Path)
	if string(content1) != "content1" {
		t.Errorf("Expected file1 content 'content1', got '%s'", content1)
	}

	content2, _ := os.ReadFile(file2Path)
	if string(content2) != "content2" {
		t.Errorf("Expected file2 content 'content2', got '%s'", content2)
	}
}

func TestExtractPlugin_ExtractImageLayersError(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)
	defer mc.Finish()

	tmpDir := t.TempDir()
	expectedErr := errors.New("failed to get layers")

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.ExtractImageLayersMock.
		Set(func(ctx context.Context, tag string, handler func(pkg.LayerStream) error) error {
			if tag != "v1.0.0" {
				t.Errorf("Expected tag 'v1.0.0', got '%s'", tag)
			}
			return expectedErr
		})

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

	// Act
	err := service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir)

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
	defer mc.Finish()

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

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.ExtractImageLayersMock.
		Set(func(ctx context.Context, tag string, handler func(pkg.LayerStream) error) error {
			stream := &mockLayerStream{
				index:  1,
				total:  1,
				reader: io.NopCloser(bytes.NewReader(tarBuf.Bytes())),
			}
			return handler(stream)
		})

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

	// Act
	err = service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir)

	// Assert
	if err == nil {
		t.Fatal("Expected error for path traversal attempt, got nil")
	}

	expectedErrorMsg := "invalid file path (path traversal attempt)"
	if !contains(err.Error(), expectedErrorMsg) {
		t.Errorf("Expected error message to contain '%s', got '%s'", expectedErrorMsg, err.Error())
	}
}

func TestExtractPlugin_CreateDestinationError(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)
	defer mc.Finish()

	// Use a path that cannot be created (e.g., under /dev/null)
	invalidDir := "/dev/null/invalid-path"

	mockClient := mock.NewRegistryClientMock(mc)
	// ExtractImageLayers should not be called

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

	// Act
	err := service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", invalidDir)

	// Assert
	if err == nil {
		t.Fatal("Expected error when creating invalid destination directory, got nil")
	}

	expectedErrorMsg := "failed to create destination directory"
	if !contains(err.Error(), expectedErrorMsg) {
		t.Errorf("Expected error message to contain '%s', got '%s'", expectedErrorMsg, err.Error())
	}
}

func TestExtractPlugin_EmptyRepository(t *testing.T) {
	// Arrange
	mc := minimock.NewController(t)
	defer mc.Finish()

	tmpDir := t.TempDir()

	// Empty tar (no files)
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	tw.Close()

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.ExtractImageLayersMock.
		Set(func(ctx context.Context, tag string, handler func(pkg.LayerStream) error) error {
			stream := &mockLayerStream{
				index:  1,
				total:  1,
				reader: io.NopCloser(bytes.NewReader(tarBuf.Bytes())),
			}
			return handler(stream)
		})

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

	// Act
	err := service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir)

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
	defer mc.Finish()

	tmpDir := t.TempDir()

	// Create a tar archive with nested directories
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	// Add nested directories
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

	// Add a file in the nested directory
	fileContent := []byte("nested file")
	tw.WriteHeader(&tar.Header{
		Name:     "a/b/c/file.txt",
		Mode:     0644,
		Size:     int64(len(fileContent)),
		Typeflag: tar.TypeReg,
	})
	tw.Write(fileContent)

	tw.Close()

	mockScopedClient := mock.NewRegistryClientMock(mc)
	mockScopedClient.ExtractImageLayersMock.
		Set(func(ctx context.Context, tag string, handler func(pkg.LayerStream) error) error {
			stream := &mockLayerStream{
				index:  1,
				total:  1,
				reader: io.NopCloser(bytes.NewReader(tarBuf.Bytes())),
			}
			return handler(stream)
		})

	mockClient := mock.NewRegistryClientMock(mc)
	mockClient.WithScopeMock.
		Expect("test-plugin").
		Return(mockScopedClient)

	logger := log.NewNop()
	service := plugins.NewPluginService(mockClient, logger)

	// Act
	err := service.ExtractPlugin(context.Background(), "test-plugin", "v1.0.0", tmpDir)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify nested file exists
	filePath := filepath.Join(tmpDir, "a", "b", "c", "file.txt")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("Expected file '%s' to exist", filePath)
	}

	// Verify content
	content, _ := os.ReadFile(filePath)
	if string(content) != "nested file" {
		t.Errorf("Expected content 'nested file', got '%s'", content)
	}
}

// Helper types and functions

type mockLayerStream struct {
	index  int
	total  int
	reader io.ReadCloser
}

func (m *mockLayerStream) GetIndex() int {
	return m.index
}

func (m *mockLayerStream) GetTotal() int {
	return m.total
}

func (m *mockLayerStream) GetReader() io.ReadCloser {
	return m.reader
}

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
