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

package archive_test

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

func TestOpenRegularFileRejectsUnixSpecialFilesWithoutBlocking(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T) string
	}{
		{
			name: "fifo",
			build: func(t *testing.T) string {
				t.Helper()

				path := filepath.Join(t.TempDir(), "artifact")
				if err := syscall.Mkfifo(path, 0o600); err != nil {
					t.Fatalf("mkfifo: %v", err)
				}

				return path
			},
		},
		{
			name: "unix socket",
			build: func(t *testing.T) string {
				t.Helper()

				dir, err := os.MkdirTemp("", "d8-snapshot-socket-")
				if err != nil {
					t.Fatalf("mkdir temp: %v", err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(dir) })

				path := filepath.Join(dir, "artifact")
				listener, err := net.Listen("unix", path)
				if err != nil {
					t.Fatalf("listen unix: %v", err)
				}
				t.Cleanup(func() { _ = listener.Close() })

				return path
			},
		},
	}

	if info, err := os.Lstat("/dev/null"); err == nil && info.Mode()&os.ModeDevice != 0 {
		tests = append(tests, struct {
			name  string
			build func(t *testing.T) string
		}{
			name:  "device",
			build: func(*testing.T) string { return "/dev/null" },
		})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.build(t)

			file, err := archive.OpenRegularFile(path)
			if file != nil {
				_ = file.Close()
			}

			if !errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
				t.Fatalf("OpenRegularFile error = %v, want ErrNonRegularArchiveArtifact", err)
			}

			if !strings.Contains(err.Error(), path) {
				t.Errorf("error %q does not contain offending path %q", err, path)
			}
		})
	}
}

func TestClassifyBlockPayloadRejectsUnixSpecialExactNames(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T, path string)
	}{
		{
			name: "fifo",
			build: func(t *testing.T, path string) {
				t.Helper()

				if err := syscall.Mkfifo(path, 0o600); err != nil {
					t.Fatalf("mkfifo: %v", err)
				}
			},
		},
		{
			name: "socket",
			build: func(t *testing.T, path string) {
				t.Helper()

				listener, err := net.Listen("unix", path)
				if err != nil {
					t.Fatalf("listen unix: %v", err)
				}
				t.Cleanup(func() { _ = listener.Close() })
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodeDir, err := os.MkdirTemp("", "d8-snapshot-special-")
			if err != nil {
				t.Fatalf("mkdir temp: %v", err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(nodeDir) })

			path := filepath.Join(nodeDir, archive.DataBlockName(".zst"))
			tc.build(t, path)

			_, found, err := archive.ClassifyBlockPayload(nodeDir)
			if found {
				t.Error("found = true, want false")
			}

			if !errors.Is(err, archive.ErrInvalidBlockPayload) ||
				!errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
				t.Errorf("error = %v, want ErrInvalidBlockPayload and ErrNonRegularArchiveArtifact", err)
			}
		})
	}
}
