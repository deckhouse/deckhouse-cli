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
	"os"

	"github.com/deckhouse/deckhouse-cli/internal/backup/usecase"
)

// FileSystemAdapter adapts os package to usecase.FileSystem
type FileSystemAdapter struct{}

// NewFileSystemAdapter creates a new FileSystemAdapter
func NewFileSystemAdapter() *FileSystemAdapter {
	return &FileSystemAdapter{}
}

func (a *FileSystemAdapter) CreateTemp(dir, pattern string) (usecase.WritableFile, error) {
	return os.CreateTemp(dir, pattern)
}

func (a *FileSystemAdapter) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (a *FileSystemAdapter) Remove(path string) error {
	return os.Remove(path)
}

// Compile-time check
var _ usecase.FileSystem = (*FileSystemAdapter)(nil)

