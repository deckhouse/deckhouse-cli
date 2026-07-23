//go:build unix

/*
Copyright 2026 Flant JSC

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

package archive

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestComputeNodeChecksumRejectsUnixFIFOs(t *testing.T) {
	tests := []struct {
		name string
		path func(nodeDir string) string
	}{
		{
			name: "filesystem payload",
			path: func(nodeDir string) string {
				return filepath.Join(nodeDir, FsTarName)
			},
		},
		{
			name: "legacy data file",
			path: func(nodeDir string) string {
				return filepath.Join(nodeDir, DataDirName, "pvc.bin")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodeDir := makeNodeDir(t)
			path := tc.path(nodeDir)

			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("mkdir parent: %v", err)
			}

			if err := syscall.Mkfifo(path, 0o600); err != nil {
				t.Fatalf("mkfifo: %v", err)
			}

			_, err := ComputeNodeChecksum(nodeDir)
			if !errors.Is(err, ErrNonRegularArchiveArtifact) {
				t.Fatalf("ComputeNodeChecksum error = %v, want ErrNonRegularArchiveArtifact", err)
			}
		})
	}
}
