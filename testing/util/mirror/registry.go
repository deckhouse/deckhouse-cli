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

type ListableBlobHandler struct {
	registry.BlobHandler
	registry.BlobPutHandler

	mu            sync.Mutex
	ingestedBlobs []string
}

func (h *ListableBlobHandler) Get(ctx context.Context, repo string, hash v1.Hash) (io.ReadCloser, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ingestedBlobs = append(h.ingestedBlobs, hash.String())

	return h.BlobHandler.Get(ctx, repo, hash)
}

func (h *ListableBlobHandler) ListBlobs() []string {
	return h.ingestedBlobs
}

func SetupEmptyRegistryRepo(useTLS bool) (host, repoPath string, blobHandler *ListableBlobHandler) {
	memBlobHandler := registry.NewInMemoryBlobHandler()
	bh := &ListableBlobHandler{
		BlobHandler:    memBlobHandler,
		BlobPutHandler: memBlobHandler.(registry.BlobPutHandler),
	}
	registryHandler := registry.New(registry.WithBlobHandler(bh), registry.Logger(golog.New(io.Discard, "", 0)))

	server := httptest.NewUnstartedServer(registryHandler)
	if useTLS {
		server.StartTLS()
	} else {
		server.Start()
	}

	host = strings.TrimPrefix(server.URL, "http://")
	repoPath = "/deckhouse/ee"
	if useTLS {
		host = strings.TrimPrefix(server.URL, "https://")
	}

	return host, repoPath, bh
}
