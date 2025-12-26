/*
Copyright 2024 Flant JSC

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

package mirror

import (
	"io"
	"io/fs"
	golog "log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/registry"
)

// TestRegistry is a disk-based container registry for e2e testing.
// Blobs are stored on disk to avoid memory exhaustion when mirroring large images.
type TestRegistry struct {
	server     *httptest.Server
	storageDir string

	Host string // e.g. "127.0.0.1:12345"
}

// NewTestRegistry creates a new disk-based test registry.
// Storage is created in a temporary directory that will be cleaned up on Close().
func NewTestRegistry(useTLS bool) (*TestRegistry, error) {
	storageDir, err := os.MkdirTemp("", "test-registry-*")
	if err != nil {
		return nil, err
	}

	blobHandler := registry.NewDiskBlobHandler(storageDir)

	handler := registry.New(
		registry.WithBlobHandler(blobHandler),
		registry.Logger(golog.New(io.Discard, "", 0)),
	)

	server := httptest.NewUnstartedServer(handler)
	if useTLS {
		server.StartTLS()
	} else {
		server.Start()
	}

	host := strings.TrimPrefix(server.URL, "http://")
	if useTLS {
		host = strings.TrimPrefix(server.URL, "https://")
	}

	return &TestRegistry{
		server:     server,
		storageDir: storageDir,
		Host:       host,
	}, nil
}

// Close stops the registry server and removes all stored data.
func (r *TestRegistry) Close() {
	if r.server != nil {
		r.server.Close()
	}
	if r.storageDir != "" {
		os.RemoveAll(r.storageDir)
	}
}

// StoragePath returns the path to the on-disk blob storage.
// Useful for debugging or inspecting stored data.
func (r *TestRegistry) StoragePath() string {
	return r.storageDir
}

// BlobCount returns the number of blobs currently stored in the registry.
func (r *TestRegistry) BlobCount() int {
	count := 0
	_ = filepath.WalkDir(r.storageDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			count++
		}
		return nil
	})
	return count
}

// ListBlobs returns a list of blob digests stored in the registry.
// This is useful for verifying what was pushed.
func (r *TestRegistry) ListBlobs() []string {
	var blobs []string
	_ = filepath.WalkDir(r.storageDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			// Extract digest from path (format: storageDir/sha256/abc123...)
			rel, _ := filepath.Rel(r.storageDir, path)
			parts := strings.Split(rel, string(filepath.Separator))
			if len(parts) == 2 {
				blobs = append(blobs, parts[0]+":"+parts[1])
			}
		}
		return nil
	})
	return blobs
}

// SetupTestRegistry creates a disk-based registry for testing.
// Returns *TestRegistry - use reg.Host to get the address, then append your own repo path.
func SetupTestRegistry(useTLS bool) *TestRegistry {
	reg, err := NewTestRegistry(useTLS)
	if err != nil {
		panic("failed to create test registry: " + err.Error())
	}
	return reg
}
