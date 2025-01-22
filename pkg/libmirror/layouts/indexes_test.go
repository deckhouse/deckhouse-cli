package layouts

import (
	"sort"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/stretchr/testify/require"
)

func TestSortIndexManifests(t *testing.T) {
	const imagesCount = 25
	l := createEmptyOCILayout(t)
	for i := 0; i < imagesCount; i++ {
		img, err := random.Image(512, 4)
		require.NoError(t, err, "Images should be generated without problems")

		digest, err := img.Digest()
		require.NoError(t, err, "Digest should be a resolved")
		imageRef := "localhost/repo/image:" + digest.Hex

		require.NoError(t, l.AppendImage(
			img,
			layout.WithPlatform(v1.Platform{Architecture: "amd64", OS: "linux"}),
			layout.WithAnnotations(map[string]string{
				"org.opencontainers.image.ref.name": imageRef,
				"io.deckhouse.image.short_tag":      digest.Hex,
			})), "Images should be added to layout")
	}

	err := SortIndexManifests(l)
	require.NoError(t, err, "Should be able to sort index manifests without failures")
	index, err := l.ImageIndex()
	require.NoError(t, err, "Should be able to read index")
	indexManifest, err := index.IndexManifest()
	require.NoError(t, err, "Should be able to parse index manifest")
	require.Len(t, indexManifest.Manifests, imagesCount, "Number of images should not be changed after sorting")
	require.True(t, sort.SliceIsSorted(indexManifest.Manifests, func(i, j int) bool {
		ref1 := indexManifest.Manifests[i].Annotations["org.opencontainers.image.ref.name"]
		ref2 := indexManifest.Manifests[j].Annotations["org.opencontainers.image.ref.name"]
		return ref1 < ref2
	}), "Index manifests should be sorted by image references")
}

func Test_indexManifestAnnotations_MarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		a    indexManifestAnnotations
		want []byte
	}{
		{
			name: "one key",
			a: indexManifestAnnotations{
				"org.opencontainers.image.ref.name": "registry.com/foo:bar",
			},
			want: []byte(`{"org.opencontainers.image.ref.name": "registry.com/foo:bar"}`),
		},
		{
			name: "multiple keys",
			a: indexManifestAnnotations{
				"org.opencontainers.image.ref.name": "registry.com/foo:bar",
				"short_tag":                         "bar",
			},
			want: []byte(`{"org.opencontainers.image.ref.name": "registry.com/foo:bar","short_tag": "bar"}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.a.MarshalJSON()
			require.NoError(t, err)

			// JSONEq validates that JSON has valid structure, Equal validates order of fields in JSON.
			require.JSONEq(t, string(tt.want), string(got))
			require.Equal(t, tt.want, got)
		})
	}
}
