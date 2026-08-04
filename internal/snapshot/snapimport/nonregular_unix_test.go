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

package snapimport

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

func TestRunRejectsUnixSpecialArchiveArtifactsBeforeExternalCalls(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T) (string, string)
	}{
		{
			name: "block payload fifo",
			build: func(t *testing.T) (string, string) {
				t.Helper()

				root := buildTwoLevelArchive(t)
				path := filepath.Join(childDir(root, "VolumeSnapshot", "pvc-1"), archive.DataBlockName(""))
				if err := os.Remove(path); err != nil {
					t.Fatalf("remove block payload: %v", err)
				}

				if err := syscall.Mkfifo(path, 0o600); err != nil {
					t.Fatalf("mkfifo: %v", err)
				}

				return root, path
			},
		},
		{
			name: "block payload socket",
			build: func(t *testing.T) (string, string) {
				t.Helper()

				root, err := os.MkdirTemp("/tmp", "d8-snapshot-socket-")
				if err != nil {
					t.Fatalf("mkdir temp: %v", err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(root) })

				writeArchiveNode(t, root, archiveNode{
					apiVersion: snapshotAPIVersion,
					kind:       snapshotKind,
					name:       "root",
				})

				leaf := childDir(root, "VolumeSnapshot", "pvc-1")
				writeArchiveNode(t, leaf, archiveNode{
					apiVersion: "snapshot.storage.k8s.io/v1",
					kind:       "VolumeSnapshot",
					name:       "pvc-1",
					blockData:  []byte("rawbytes"),
				})

				path := filepath.Join(leaf, archive.DataBlockName(""))
				if err := os.Remove(path); err != nil {
					t.Fatalf("remove block payload: %v", err)
				}

				listener, err := net.Listen("unix", path)
				if err != nil {
					t.Fatalf("listen unix: %v", err)
				}
				t.Cleanup(func() { _ = listener.Close() })

				return root, path
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root, _ := tc.build(t)
			up := &stubUploader{}
			vol := &stubVolumes{}
			dyn := newFakeDynamic(readyRootSnapshot())
			mapper := &countingRESTMapper{RESTMapper: testMapper()}
			cfg := baseConfig(root, up, vol, dyn)
			cfg.Mapper = mapper

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			err := Run(ctx, cfg)
			if !errors.Is(err, archive.ErrNonRegularArchiveArtifact) {
				t.Fatalf("Run error = %v, want ErrNonRegularArchiveArtifact", err)
			}

			if calls := mapper.calls.Load(); calls != 0 {
				t.Errorf("RESTMapper calls = %d, want 0", calls)
			}

			if actions := dyn.Actions(); len(actions) != 0 {
				t.Errorf("dynamic client actions = %v, want none", actions)
			}

			if len(up.calls) != 0 || len(vol.ensure) != 0 || len(vol.upload) != 0 {
				t.Errorf("mutations = manifests %d ensure %v upload %v, want none",
					len(up.calls), vol.ensure, vol.upload)
			}
		})
	}
}
