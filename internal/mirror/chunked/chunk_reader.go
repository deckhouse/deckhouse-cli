/*
Copyright 2025 Flant JSC

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

package chunked

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-multierror"
	"github.com/samber/lo"
)

type FileReader struct {
	chunks  []*os.File
	multiRd io.Reader
}

func Open(baseDir, baseFileName string) (*FileReader, error) {
	chunkIndex := 0
	chunks := make([]*os.File, 0)
	for {
		chunkName := fmt.Sprintf("%s.%04d.chunk", baseFileName, chunkIndex)
		chunk, err := os.Open(filepath.Join(baseDir, chunkName))
		switch {
		case errors.Is(err, os.ErrNotExist) && chunkIndex == 0:
			return nil, err
		case errors.Is(err, os.ErrNotExist) && chunkIndex > 0:
			return &FileReader{
				chunks:  chunks,
				multiRd: io.MultiReader(lo.Map(chunks, func(item *os.File, _ int) io.Reader { return item })...),
			}, nil
		case err != nil:
			return nil, fmt.Errorf("opening chunk %s: %w", chunkName, err)
		}

		chunks = append(chunks, chunk)
		chunkIndex += 1
	}
}

func (f *FileReader) Read(p []byte) (n int, err error) {
	return f.multiRd.Read(p)
}

func (f *FileReader) Close() error {
	err := &multierror.Error{}
	for _, chunk := range f.chunks {
		if chErr := chunk.Close(); chErr != nil {
			err = multierror.Append(err, fmt.Errorf("closing chunk %s: %w", chunk.Name(), chErr))
		}
	}
	return err.ErrorOrNil()
}
