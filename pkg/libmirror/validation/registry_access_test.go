package validation

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
)

func TestInsecureReadAccessValidation(t *testing.T) {
	blobHandler := registry.NewInMemoryBlobHandler()
	registryHandler := registry.New(registry.WithBlobHandler(blobHandler))
	server := httptest.NewServer(registryHandler)
	imageTag := strings.TrimPrefix(server.URL, "http://") + "/test:latest"

	img, err := random.Image(256, 1)
	require.NoError(t, err)

	ref, err := name.ParseReference(imageTag, name.Insecure)
	require.NoError(t, err)

	err = remote.Write(ref, img, remote.WithPlatform(v1.Platform{Architecture: "amd64", OS: "linux"}))
	require.NoError(t, err)

	err = NewRemoteRegistryAccessValidator().ValidateReadAccessForImage(context.TODO(), imageTag, UsePlainHTTP())
	require.NoError(t, err, "Should validate successfully")
}

func TestReadAccessValidationWithSkipTLSVerify(t *testing.T) {
	blobHandler := registry.NewInMemoryBlobHandler()
	registryHandler := registry.New(registry.WithBlobHandler(blobHandler))
	server := httptest.NewTLSServer(registryHandler)
	imageTag := strings.TrimPrefix(server.URL, "https://") + "/test:latest"

	img, err := random.Image(256, 1)
	require.NoError(t, err)

	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(nil, false, true)
	ref, err := name.ParseReference(imageTag, nameOpts...)
	require.NoError(t, err)
	remoteOpts = append(remoteOpts, remote.WithPlatform(v1.Platform{Architecture: "amd64", OS: "linux"}))

	err = remote.Write(ref, img, remoteOpts...)
	require.NoError(t, err)

	err = NewRemoteRegistryAccessValidator().ValidateReadAccessForImage(context.TODO(), imageTag, SkipTLSVerification())
	require.NoError(t, err, "Should validate successfully")
}

func TestWriteAccessValidationWithSkipTLSVerify(t *testing.T) {
	blobHandler := registry.NewInMemoryBlobHandler()
	registryHandler := registry.New(registry.WithBlobHandler(blobHandler))
	server := httptest.NewTLSServer(registryHandler)
	repo := strings.TrimPrefix(server.URL, "https://") + "/test"

	err := NewRemoteRegistryAccessValidator().ValidateWriteAccessForRepo(context.TODO(), repo, SkipTLSVerification())
	require.NoError(t, err, "Should validate successfully")
}

func TestWriteAccessValidationInsecure(t *testing.T) {
	blobHandler := registry.NewInMemoryBlobHandler()
	registryHandler := registry.New(registry.WithBlobHandler(blobHandler))
	server := httptest.NewServer(registryHandler)
	repo := strings.TrimPrefix(server.URL, "http://") + "/test"

	err := NewRemoteRegistryAccessValidator().ValidateWriteAccessForRepo(context.TODO(), repo, UsePlainHTTP())
	require.NoError(t, err, "Should validate successfully")
}
