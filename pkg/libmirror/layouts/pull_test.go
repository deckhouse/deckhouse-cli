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

package layouts

import (
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

var testLogger = log.NewSLogger(slog.LevelDebug)

func TestPullTrivyVulnerabilityDatabaseImageSuccessSkipTLS(t *testing.T) {
	blobHandler := registry.NewInMemoryBlobHandler()
	registryHandler := registry.New(registry.WithBlobHandler(blobHandler))
	server := httptest.NewTLSServer(registryHandler)
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(authn.Anonymous, false, true)

	deckhouseRepo := strings.TrimPrefix(server.URL, "https://") + "/deckhouse/ee"
	images := []string{
		deckhouseRepo + "/security/trivy-db:2",
		deckhouseRepo + "/security/trivy-bdu:1",
		deckhouseRepo + "/security/trivy-java-db:1",
	}

	wantImages := make([]v1.Image, 0)
	for _, image := range images {
		ref, err := name.ParseReference(image, nameOpts...)
		require.NoError(t, err)
		wantImage, err := random.Image(256, 1)
		require.NoError(t, err)
		require.NoError(t, remote.Write(ref, wantImage, remoteOpts...))
		wantImages = append(wantImages, wantImage)
	}

	layouts := &ImageLayouts{
		TrivyDB:     createEmptyOCILayout(t),
		TrivyBDU:    createEmptyOCILayout(t),
		TrivyJavaDB: createEmptyOCILayout(t),
	}

	err := PullTrivyVulnerabilityDatabasesImages(
		&contexts.PullContext{BaseContext: contexts.BaseContext{
			Logger:                testLogger,
			RegistryAuth:          authn.Anonymous,
			DeckhouseRegistryRepo: deckhouseRepo,
			SkipTLSVerification:   true,
		}},
		layouts,
	)
	require.NoError(t, err)

	for i, wantImage := range wantImages {
		wantDigest, err := wantImage.Digest()
		require.NoError(t, err)

		gotImage, err := layoutByIndex(t, layouts, i).Image(wantDigest)
		require.NoError(t, err)

		gotDigest, err := gotImage.Digest()
		require.NoError(t, err)
		require.Equal(t, wantDigest, gotDigest)
	}
}

func TestPullTrivyVulnerabilityDatabaseImageSuccessInsecure(t *testing.T) {
	blobHandler := registry.NewInMemoryBlobHandler()
	registryHandler := registry.New(registry.WithBlobHandler(blobHandler))
	server := httptest.NewServer(registryHandler)
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(authn.Anonymous, true, false)

	deckhouseRepo := strings.TrimPrefix(server.URL, "http://") + "/deckhouse/ee"
	images := []string{
		deckhouseRepo + "/security/trivy-db:2",
		deckhouseRepo + "/security/trivy-bdu:1",
		deckhouseRepo + "/security/trivy-java-db:1",
	}

	wantImages := make([]v1.Image, 0)
	for _, image := range images {
		ref, err := name.ParseReference(image, nameOpts...)
		require.NoError(t, err)
		wantImage, err := random.Image(256, 1)
		require.NoError(t, err)
		require.NoError(t, remote.Write(ref, wantImage, remoteOpts...))
		wantImages = append(wantImages, wantImage)
	}

	layouts := &ImageLayouts{
		TrivyDB:     createEmptyOCILayout(t),
		TrivyBDU:    createEmptyOCILayout(t),
		TrivyJavaDB: createEmptyOCILayout(t),
	}

	err := PullTrivyVulnerabilityDatabasesImages(
		&contexts.PullContext{BaseContext: contexts.BaseContext{
			Logger:                testLogger,
			RegistryAuth:          authn.Anonymous,
			DeckhouseRegistryRepo: deckhouseRepo,
			Insecure:              true,
		}},
		layouts,
	)
	require.NoError(t, err)

	for i, wantImage := range wantImages {
		wantDigest, err := wantImage.Digest()
		require.NoError(t, err)

		gotImage, err := layoutByIndex(t, layouts, i).Image(wantDigest)
		require.NoError(t, err)

		gotDigest, err := gotImage.Digest()
		require.NoError(t, err)
		require.Equal(t, wantDigest, gotDigest)
	}
}

func layoutByIndex(t *testing.T, layouts *ImageLayouts, idx int) layout.Path {
	t.Helper()
	switch idx {
	case 0:
		return layouts.TrivyDB
	case 1:
		return layouts.TrivyBDU
	case 2:
		return layouts.TrivyJavaDB
	default:
		t.Fatalf("Unexpected layout index, expected only [0-2], but got %d", idx)
		return ""
	}
}

func createEmptyOCILayout(t *testing.T) layout.Path {
	t.Helper()

	l, err := CreateEmptyImageLayoutAtPath(t.TempDir())
	require.NoError(t, err)
	return l
}
