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
