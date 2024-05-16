package images

import (
	"io/fs"
	"testing"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/require"
)

func TestExtractFileFromImage(t *testing.T) {
	filesByLayer := []map[string][]byte{
		{
			"file1": []byte("hello world"),
		},
		{
			"dir1/file2": []byte("hello world x2"),
			"dir1/file4": []byte("123123"),
		},
		{
			"dir1/file3": []byte("hello world x3"),
			"dir1/file2": []byte("overwritten file"),
			"file2":      []byte("make tests great again"),
		},
	}

	img := empty.Image
	for _, layerFiles := range filesByLayer {
		layer, err := crane.Layer(layerFiles)
		require.NoError(t, err)
		img, err = mutate.AppendLayers(img, layer)
		require.NoError(t, err)
	}

	file, err := ExtractFileFromImage(img, "file1")
	require.NoError(t, err)
	require.Equal(t, filesByLayer[0]["file1"], file.Bytes())

	file, err = ExtractFileFromImage(img, "dir1/file4")
	require.NoError(t, err)
	require.Equal(t, filesByLayer[1]["dir1/file4"], file.Bytes())

	file, err = ExtractFileFromImage(img, "file2")
	require.NoError(t, err)
	require.Equal(t, filesByLayer[2]["file2"], file.Bytes())

	file, err = ExtractFileFromImage(img, "dir1/file2")
	require.NoError(t, err)
	require.Equal(t, filesByLayer[2]["dir1/file2"], file.Bytes())

	file, err = ExtractFileFromImage(img, "does_not_exist")
	require.Nil(t, file)
	require.ErrorIs(t, err, fs.ErrNotExist)
}
