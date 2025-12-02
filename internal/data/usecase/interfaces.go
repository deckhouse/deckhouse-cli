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

package usecase

import (
	"context"
	"io"

	"github.com/deckhouse/deckhouse-cli/internal/data/domain"
)

// DataExportRepository handles DataExport K8s resources
type DataExportRepository interface {
	Create(ctx context.Context, params *domain.CreateExportParams) error
	Get(ctx context.Context, name, namespace string) (*domain.DataExport, error)
	GetWithRetry(ctx context.Context, name, namespace string) (*domain.DataExport, error)
	Delete(ctx context.Context, name, namespace string) error
}

// DataImportRepository handles DataImport K8s resources
type DataImportRepository interface {
	Create(ctx context.Context, params *domain.CreateImportParams) error
	Get(ctx context.Context, name, namespace string) (*domain.DataImport, error)
	GetWithRetry(ctx context.Context, name, namespace string) (*domain.DataImport, error)
	Delete(ctx context.Context, name, namespace string) error
}

// HTTPClient handles HTTP operations for data transfer
type HTTPClient interface {
	// Get performs an HTTP GET request
	Get(ctx context.Context, url string) (io.ReadCloser, int, error)
	// Head performs an HTTP HEAD request
	Head(ctx context.Context, url string) (map[string]string, int, error)
	// Put performs an HTTP PUT request with body
	Put(ctx context.Context, url string, body io.Reader, headers map[string]string) (map[string]string, int, error)
	// SetCA sets custom CA certificate for HTTPS
	SetCA(caData []byte)
	// Copy creates a copy of the client
	Copy() HTTPClient
}

// FileSystem handles file operations
type FileSystem interface {
	// Create creates a new file
	Create(path string) (io.WriteCloser, error)
	// Open opens a file for reading
	Open(path string) (io.ReadCloser, int64, error)
	// MkdirAll creates directory with parents
	MkdirAll(path string) error
	// Stat returns file info
	Stat(path string) (FileInfo, error)
}

// FileInfo represents file metadata
type FileInfo interface {
	Size() int64
	Mode() uint32
	Uid() int
	Gid() int
}

// Logger provides logging capabilities
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
}

// ProgressReporter reports progress of long operations
type ProgressReporter interface {
	Start(total int64)
	Update(current int64)
	Finish()
}

