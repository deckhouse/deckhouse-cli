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
	"context"
	"io"
	golog "log"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// ListableBlobHandler wraps a BlobHandler to track ingested blobs
type ListableBlobHandler struct {
	registry.BlobHandler
	registry.BlobPutHandler

	mu            sync.Mutex
	ingestedBlobs []string
}

// Get implements registry.BlobHandler and tracks accessed blobs
func (h *ListableBlobHandler) Get(ctx context.Context, repo string, hash v1.Hash) (io.ReadCloser, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ingestedBlobs = append(h.ingestedBlobs, hash.String())

	return h.BlobHandler.Get(ctx, repo, hash)
}

// ListBlobs returns all blobs that have been accessed
func (h *ListableBlobHandler) ListBlobs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string{}, h.ingestedBlobs...)
}

// TestRegistry holds the test registry server and its resources
type TestRegistry struct {
	Server      *httptest.Server
	Host        string
	RepoPath    string
	BlobHandler *ListableBlobHandler
}

// Close stops the test registry server
func (r *TestRegistry) Close() {
	if r.Server != nil {
		r.Server.Close()
	}
}

// FullPath returns the full registry path including host and repo
func (r *TestRegistry) FullPath() string {
	return r.Host + r.RepoPath
}

// SetupEmptyRegistryRepo creates an in-memory registry for testing
// Returns host, repoPath, and a ListableBlobHandler to track blob access
func SetupEmptyRegistryRepo(useTLS bool) ( /*host*/ string /*repoPath*/, string, *ListableBlobHandler) {
	reg := SetupTestRegistry(useTLS)
	return reg.Host, reg.RepoPath, reg.BlobHandler
}

// SetupTestRegistry creates an in-memory registry for testing and returns a TestRegistry
func SetupTestRegistry(useTLS bool) *TestRegistry {
	memBlobHandler := registry.NewInMemoryBlobHandler()
	bh := &ListableBlobHandler{
		BlobHandler:    memBlobHandler,
		BlobPutHandler: memBlobHandler.(registry.BlobPutHandler),
	}

	registryHandler := registry.New(
		registry.WithBlobHandler(bh),
		registry.Logger(golog.New(io.Discard, "", 0)),
	)

	server := httptest.NewUnstartedServer(registryHandler)
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
		Server:      server,
		Host:        host,
		RepoPath:    "/deckhouse/ee",
		BlobHandler: bh,
	}
}

// SetupTestRegistryWithPath creates an in-memory registry with a custom repo path
func SetupTestRegistryWithPath(useTLS bool, repoPath string) *TestRegistry {
	reg := SetupTestRegistry(useTLS)
	reg.RepoPath = repoPath
	return reg
}
