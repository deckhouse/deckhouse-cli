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

package adapters

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/backup/domain"
	"github.com/deckhouse/deckhouse-cli/internal/backup/usecase"
)

// TarballWriterAdapter implements usecase.TarballWriter using tar/gzip
type TarballWriterAdapter struct {
	mu       sync.Mutex
	file     *os.File
	writer   *tar.Writer
	gzwriter *gzip.Writer
}

// NewTarballWriterAdapter creates a new TarballWriterAdapter
func NewTarballWriterAdapter(path string, compress bool) (*TarballWriterAdapter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create file: %w", err)
	}

	var w io.Writer = file
	var gzipWriter *gzip.Writer
	if compress {
		gzipWriter = gzip.NewWriter(w)
		w = gzipWriter
	}

	return &TarballWriterAdapter{
		file:     file,
		writer:   tar.NewWriter(w),
		gzwriter: gzipWriter,
	}, nil
}

// PutObject writes a K8sObject to the tarball
func (a *TarballWriterAdapter) PutObject(obj domain.K8sObject) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	rawObject, err := obj.MarshalYAML()
	if err != nil {
		return fmt.Errorf("marshal %s %s/%s: %w", obj.GetKind(), obj.GetNamespace(), obj.GetName(), err)
	}

	namespace := obj.GetNamespace()
	if namespace == "" {
		namespace = "Cluster-Scoped"
	}

	err = a.writer.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     path.Join(namespace, obj.GetKind(), obj.GetName()+".yml"),
		Size:     int64(len(rawObject)),
		Mode:     0600,
		ModTime:  time.Now(),
	})
	if err != nil {
		return fmt.Errorf("write tar header for %s %s/%s: %w", obj.GetKind(), namespace, obj.GetName(), err)
	}

	if _, err = a.writer.Write(rawObject); err != nil {
		return fmt.Errorf("write tar content for %s %s/%s: %w", obj.GetKind(), namespace, obj.GetName(), err)
	}

	return nil
}

// Close closes the tarball writer
func (a *TarballWriterAdapter) Close() error {
	err := a.writer.Close()
	if err != nil {
		return fmt.Errorf("close tar writer: %w", err)
	}

	if a.gzwriter != nil {
		err = a.gzwriter.Close()
		if err != nil {
			return fmt.Errorf("write gzip trailer: %w", err)
		}
	}

	return a.file.Close()
}

// Compile-time check
var _ usecase.TarballWriter = (*TarballWriterAdapter)(nil)

