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
	"io"
	"os"
	"syscall"

	"github.com/deckhouse/deckhouse-cli/internal/data/usecase"
)

// Compile-time check that OSFileSystem implements usecase.FileSystem
var _ usecase.FileSystem = (*OSFileSystem)(nil)

// OSFileSystem adapts OS file operations to usecase.FileSystem interface
type OSFileSystem struct{}

// NewOSFileSystem creates a new OSFileSystem
func NewOSFileSystem() *OSFileSystem {
	return &OSFileSystem{}
}

func (fs *OSFileSystem) Create(path string) (io.WriteCloser, error) {
	return os.Create(path)
}

func (fs *OSFileSystem) Open(path string) (io.ReadCloser, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	
	return f, fi.Size(), nil
}

func (fs *OSFileSystem) MkdirAll(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}

func (fs *OSFileSystem) Stat(path string) (usecase.FileInfo, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return &osFileInfo{fi: fi}, nil
}

// osFileInfo wraps os.FileInfo to implement usecase.FileInfo
type osFileInfo struct {
	fi os.FileInfo
}

func (i *osFileInfo) Size() int64 {
	return i.fi.Size()
}

func (i *osFileInfo) Mode() uint32 {
	return uint32(i.fi.Mode().Perm())
}

func (i *osFileInfo) Uid() int {
	if st, ok := i.fi.Sys().(*syscall.Stat_t); ok {
		return int(st.Uid)
	}
	return os.Getuid()
}

func (i *osFileInfo) Gid() int {
	if st, ok := i.fi.Sys().(*syscall.Stat_t); ok {
		return int(st.Gid)
	}
	return os.Getgid()
}

