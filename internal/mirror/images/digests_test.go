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

package images

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/crane"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/contexts"
)

func TestExtractImageDigestsFromDeckhouseInstaller(t *testing.T) {
	expectedImages := []string{
		"localhost:5001/deckhouse@sha256:72623af14db0cf2411cdf6364089b1954cbfd10e76e13ff08816a628b52a9712",
		"localhost:5001/deckhouse@sha256:f58a7f8b3fbdc78a90578b45e8ddb1bf587102206d9320e9ce9f4fe9474f5650",
	}
	installerTag := "localhost:5001/deckhouse/install:stable"

	installersLayout := createOCILayoutWithInstallerImage(t, "localhost:5001/deckhouse", installerTag, expectedImages)
	images, err := ExtractImageDigestsFromDeckhouseInstaller(
		&contexts.PullContext{BaseContext: contexts.BaseContext{DeckhouseRegistryRepo: "localhost:5001/deckhouse"}},
		installerTag,
		installersLayout,
	)
	require.NoError(t, err)
	require.True(t, len(images) == len(expectedImages))
	require.ElementsMatch(t, maps.Keys(images), expectedImages)
}

func createOCILayoutWithInstallerImage(t *testing.T, imagesReoo, installerTag string, images []string) layout.Path {
	t.Helper()

	// FROM scratch
	base := empty.Image
	layers := make([]v1.Layer, 0)

	// COPY ./version /deckhouse/version
	// COPY ./images_digests.json /deckhouse/candi/images_digests.json
	imagesDigests, err := json.Marshal(
		map[string]map[string]string{
			"common": {
				"alpine": strings.TrimPrefix(images[0], imagesReoo+"@"),
			},
			"nodeManager": {
				"bashibleApiserver": strings.TrimPrefix(images[1], imagesReoo+"@"),
			},
		})
	require.NoError(t, err)
	l, err := crane.Layer(map[string][]byte{
		"deckhouse/candi/images_digests.json": imagesDigests,
	})
	require.NoError(t, err)
	layers = append(layers, l)

	img, err := mutate.AppendLayers(base, layers...)
	require.NoError(t, err)

	tempDir, err := os.MkdirTemp(os.TempDir(), "digests_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(tempDir)
	})

	installersLayout := createEmptyImageLayout(t, tempDir)
	err = installersLayout.AppendImage(img, layout.WithAnnotations(map[string]string{
		"org.opencontainers.image.ref.name": installerTag,
	}))
	require.NoError(t, err)

	return installersLayout
}

func createEmptyImageLayout(t *testing.T, path string) layout.Path {
	t.Helper()

	layoutFilePath := filepath.Join(path, "oci-layout")
	indexFilePath := filepath.Join(path, "index.json")
	blobsPath := filepath.Join(path, "blobs")

	layoutContents := []byte(`{"imageLayoutVersion": "1.0.0"}`)
	indexContents := []byte(`{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json"}`)

	require.NoError(t, os.MkdirAll(blobsPath, 0o755))
	require.NoError(t, os.WriteFile(indexFilePath, indexContents, 0o644))
	require.NoError(t, os.WriteFile(layoutFilePath, layoutContents, 0o644))

	l, err := layout.FromPath(path)
	require.NoError(t, err)
	return l
}
