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

package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

func publicationTestNode() *source.Node {
	return &source.Node{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "root",
		Namespace:  "source",
		UID:        types.UID("root-uid"),
	}
}

func publicationTestDestination(t *testing.T) (*archive.RootedDestination, string) {
	t.Helper()

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, archive.ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir manifests: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, archive.SnapshotsDirName), 0o755); err != nil {
		t.Fatalf("mkdir snapshots: %v", err)
	}

	checksum, err := archive.ComputeNodeChecksum(root)
	if err != nil {
		t.Fatalf("compute node checksum: %v", err)
	}

	childrenChecksum := archive.EmptyChildrenChecksum()
	node := publicationTestNode()
	if err := archive.WriteSnapshotYAML(root, archive.SnapshotYAML{
		APIVersion:       node.APIVersion,
		Kind:             node.Kind,
		Name:             node.Name,
		Namespace:        node.Namespace,
		UID:              string(node.UID),
		Checksum:         checksum,
		ChildrenChecksum: &childrenChecksum,
	}); err != nil {
		t.Fatalf("write snapshot.yaml: %v", err)
	}

	destination, err := archive.OpenRootedDestination(root, nil)
	if err != nil {
		t.Fatalf("open rooted destination: %v", err)
	}
	t.Cleanup(func() { _ = destination.Close() })

	return destination, root
}

func TestPublicationTransactionRejectsMalformedForeignAndPartialState(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(t *testing.T, destination *archive.RootedDestination, root, treeDigest string)
		wantErr bool
		wantNil bool
	}{
		{
			name: "malformed",
			prepare: func(t *testing.T, destination *archive.RootedDestination, _, _ string) {
				t.Helper()
				if err := os.WriteFile(
					publicationStatePath(destination, publicationTransactionName),
					[]byte("{"),
					0o600,
				); err != nil {
					t.Fatalf("write malformed transaction: %v", err)
				}
			},
			wantErr: true,
		},
		{
			name: "foreign archive root",
			prepare: func(t *testing.T, destination *archive.RootedDestination, root, treeDigest string) {
				t.Helper()
				transaction := publicationTransaction{
					Version:          publicationStateVersion,
					ArchiveRoot:      root + "-foreign",
					SourceTreeDigest: treeDigest,
					Entries: []publicationEntry{{
						Path:             "foreign",
						Identity:         publicationIdentityForNode(publicationTestNode()),
						NodeChecksum:     archive.EmptyChildrenChecksum(),
						ChildrenChecksum: archive.EmptyChildrenChecksum(),
					}},
				}
				if err := sealPublicationTransaction(&transaction); err != nil {
					t.Fatalf("seal foreign transaction: %v", err)
				}
				data, err := json.Marshal(transaction)
				if err != nil {
					t.Fatalf("marshal foreign transaction: %v", err)
				}
				if err := os.WriteFile(
					publicationStatePath(destination, publicationTransactionName),
					data,
					0o600,
				); err != nil {
					t.Fatalf("write foreign transaction: %v", err)
				}
			},
			wantErr: true,
		},
		{
			name: "partially durable temporary file",
			prepare: func(t *testing.T, destination *archive.RootedDestination, _, _ string) {
				t.Helper()
				if err := os.WriteFile(
					publicationStatePath(destination, publicationTransactionName)+".tmp",
					[]byte("partial"),
					0o600,
				); err != nil {
					t.Fatalf("write partial transaction: %v", err)
				}
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			destination, root := publicationTestDestination(t)
			treeDigest, err := publicationTreeDigest(publicationTestNode())
			if err != nil {
				t.Fatalf("compute tree digest: %v", err)
			}

			tt.prepare(t, destination, root, treeDigest)

			transaction, err := loadPublicationTransaction(destination, treeDigest)
			if tt.wantErr && !errors.Is(err, errPublicationTransactionInvalid) {
				t.Fatalf("load transaction error = %v, want errPublicationTransactionInvalid", err)
			}
			if tt.wantNil && (err != nil || transaction != nil) {
				t.Fatalf("load partial transaction = (%v, %v), want (nil, nil)", transaction, err)
			}
		})
	}
}

func TestPublicationTransactionCleanupIsDurableAndConvergent(t *testing.T) {
	destination, root := publicationTestDestination(t)
	node := publicationTestNode()
	treeDigest, err := publicationTreeDigest(node)
	if err != nil {
		t.Fatalf("compute tree digest: %v", err)
	}

	tasks := []nodeTask{{node: node, nodeDir: root, done: true}}
	transaction, err := buildPublicationTransaction(
		destination,
		treeDigest,
		tasks,
		map[*source.Node]bool{node: true},
		destination.ComputeNodeChecksum,
	)
	if err != nil {
		t.Fatalf("build transaction: %v", err)
	}
	if err := writePublicationTransaction(context.Background(), destination, transaction); err != nil {
		t.Fatalf("write transaction: %v", err)
	}
	if err := completePublicationTransaction(context.Background(), destination, transaction); err != nil {
		t.Fatalf("complete transaction: %v", err)
	}

	if _, err := os.Stat(publicationStatePath(destination, publicationTransactionName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active transaction stat error = %v, want os.ErrNotExist", err)
	}

	receipt, err := loadPublicationReceipt(destination, treeDigest)
	if err != nil {
		t.Fatalf("load receipt: %v", err)
	}
	if receipt == nil || !receipt.Cleaned || receipt.TransactionChecksum != transaction.Checksum {
		t.Fatalf("cleanup receipt = %#v, want cleaned receipt for %s", receipt, transaction.Checksum)
	}
}
