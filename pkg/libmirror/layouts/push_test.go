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
	"math/rand/v2"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/mock"
	mirrorTestUtils "github.com/deckhouse/deckhouse-cli/testing/util/mirror"
)

func TestPushLayoutToRepoWithParallelism(t *testing.T) {
	s := require.New(t)

	const totalImages, layersPerImage = 10, 3
	imagesLayout := createEmptyOCILayout(t)
	reg := mirrorTestUtils.SetupTestRegistry(false)
	defer reg.Close()
	repoPath := reg.Host + "/deckhouse/ee"

	client := mock.NewRegistryClientMock(t)
	client.PushImageMock.Return(nil)

	platformOpt := layout.WithPlatform(v1.Platform{OS: "linux", Architecture: "amd64"})
	for range [totalImages]struct{}{} {
		img, err := random.Image(rand.Int64N(513), layersPerImage)
		s.NoError(err)
		digest, err := img.Digest()
		s.NoError(err)
		err = imagesLayout.AppendImage(img, platformOpt, layout.WithAnnotations(map[string]string{
			"org.opencontainers.image.ref.name": repoPath + "@" + digest.String(),
			"io.deckhouse.image.short_tag":      digest.Hex,
		}))
		s.NoError(err)
	}

	err := PushLayoutToRepo(
		client,
		imagesLayout,
		repoPath,
		authn.Anonymous,
		log.NewSLogger(slog.LevelDebug),
		params.ParallelismConfig{
			Blobs:  4,
			Images: 5,
		},
		true,  // Use plain insecure HTTP
		false, // TLS verification irrelevant to HTTP requests
	)

	s.NoError(err, "Push should not fail")

	// Verify that PushImage was called for each image
	s.Len(client.PushImageMock.Calls(), totalImages, "PushImage should be called for each image")
}

func TestPushLayoutToRepoWithoutParallelism(t *testing.T) {
	s := require.New(t)

	const totalImages, layersPerImage = 10, 3
	imagesLayout := createEmptyOCILayout(t)
	reg := mirrorTestUtils.SetupTestRegistry(false)
	defer reg.Close()
	repoPath := reg.Host + "/deckhouse/ee"

	client := mock.NewRegistryClientMock(t)
	client.PushImageMock.Return(nil)

	platformOpt := layout.WithPlatform(v1.Platform{OS: "linux", Architecture: "amd64"})
	for range [totalImages]struct{}{} {
		img, err := random.Image(rand.Int64N(513), layersPerImage)
		s.NoError(err)
		digest, err := img.Digest()
		s.NoError(err)
		err = imagesLayout.AppendImage(img, platformOpt, layout.WithAnnotations(map[string]string{
			"org.opencontainers.image.ref.name": repoPath + "@" + digest.String(),
			"io.deckhouse.image.short_tag":      digest.Hex,
		}))
		s.NoError(err)
	}

	err := PushLayoutToRepo(
		client,
		imagesLayout,
		repoPath,
		authn.Anonymous,
		log.NewSLogger(slog.LevelDebug),
		params.ParallelismConfig{
			Blobs:  4,
			Images: 1,
		},
		true,  // Use plain insecure HTTP
		false, // TLS verification irrelevant to HTTP requests
	)

	s.NoError(err, "Push should not fail")

	// Verify that PushImage was called for each image
	s.Len(client.PushImageMock.Calls(), totalImages, "PushImage should be called for each image")
}

func TestPushEmptyLayoutToRepo(t *testing.T) {
	s := require.New(t)
	reg := mirrorTestUtils.SetupTestRegistry(false)
	defer reg.Close()
	repoPath := reg.Host + "/deckhouse/ee"

	client := mock.NewRegistryClientMock(t)

	emptyLayout := createEmptyOCILayout(t)
	err := PushLayoutToRepo(
		client,
		emptyLayout,
		repoPath,
		authn.Anonymous,
		log.NewSLogger(slog.LevelDebug),
		params.DefaultParallelism,
		true,  // Use plain insecure HTTP
		false, // TLS verification irrelevant to HTTP requests
	)
	s.NoError(err, "Push should not fail")
	s.Len(reg.ListBlobs(), 0, "No blobs should be pushed to registry")
}
