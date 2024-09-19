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
