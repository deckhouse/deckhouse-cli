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

package source

import (
	"errors"
	"testing"
)

// buildFindTestTree constructs a small in-memory tree for FindNode tests:
//
//	root (Snapshot/root-snap)
//	├── vm  (DemoVirtualMachineSnapshot/vm-snap)
//	│   └── disk (DemoDiskSnapshot/disk-snap)
//	└── leaf (VolumeSnapshot/pvc-orphan)  ← orphan leaf
func buildFindTestTree() (root, vm, disk, leaf *Node) {
	root = &Node{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "root-snap",
		Namespace:  "default",
	}

	vm = &Node{
		APIVersion: "demo.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualMachineSnapshot",
		Name:       "vm-snap",
		Namespace:  "default",
		Parent:     root,
	}

	disk = &Node{
		APIVersion: "demo.deckhouse.io/v1alpha1",
		Kind:       "DemoDiskSnapshot",
		Name:       "disk-snap",
		Namespace:  "default",
		Parent:     vm,
	}
	vm.Children = []*Node{disk}

	leaf = &Node{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "pvc-orphan",
		Namespace:  "default",
		Parent:     root,
	}

	root.Children = []*Node{vm, leaf}

	return root, vm, disk, leaf
}

func TestFindNode(t *testing.T) {
	t.Helper()

	root, vm, disk, leaf := buildFindTestTree()

	cases := []struct {
		name     string
		kind     string
		nodeName string
		wantNode *Node
		wantAnc  []*Node
		wantErr  error
	}{
		{
			name:     "root_node",
			kind:     "Snapshot",
			nodeName: "root-snap",
			wantNode: root,
			wantAnc:  nil,
		},
		{
			name:     "vm_node",
			kind:     "DemoVirtualMachineSnapshot",
			nodeName: "vm-snap",
			wantNode: vm,
			wantAnc:  []*Node{root},
		},
		{
			name:     "disk_node",
			kind:     "DemoDiskSnapshot",
			nodeName: "disk-snap",
			wantNode: disk,
			wantAnc:  []*Node{root, vm},
		},
		{
			name:     "volume_snapshot_leaf",
			kind:     "VolumeSnapshot",
			nodeName: "pvc-orphan",
			wantNode: leaf,
			wantAnc:  []*Node{root},
		},
		{
			name:     "not_found_wrong_name",
			kind:     "Snapshot",
			nodeName: "nonexistent",
			wantErr:  ErrNodeNotFound,
		},
		{
			name:     "not_found_wrong_kind",
			kind:     "UnknownKind",
			nodeName: "root-snap",
			wantErr:  ErrNodeNotFound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotNode, gotAnc, err := FindNode(root, tc.kind, tc.nodeName)

			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("expected error wrapping %v, got: %v", tc.wantErr, err)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if gotNode != tc.wantNode {
				t.Fatalf("node pointer mismatch: got %p (%s/%s), want %p (%s/%s)",
					gotNode, gotNode.Kind, gotNode.Name,
					tc.wantNode, tc.wantNode.Kind, tc.wantNode.Name)
			}

			if len(gotAnc) != len(tc.wantAnc) {
				t.Fatalf("ancestor chain length: got %d, want %d", len(gotAnc), len(tc.wantAnc))
			}

			for i, want := range tc.wantAnc {
				if gotAnc[i] != want {
					t.Fatalf("ancestor[%d]: got %p (%s/%s), want %p (%s/%s)",
						i, gotAnc[i], gotAnc[i].Kind, gotAnc[i].Name,
						want, want.Kind, want.Name)
				}
			}
		})
	}
}

func TestFindNode_Ambiguous(t *testing.T) {
	t.Helper()

	root := &Node{Kind: "Snapshot", Name: "root"}
	dup1 := &Node{Kind: "DemoDiskSnapshot", Name: "disk", Parent: root}
	dup2 := &Node{Kind: "DemoDiskSnapshot", Name: "disk", Parent: root}
	root.Children = []*Node{dup1, dup2}

	_, _, err := FindNode(root, "DemoDiskSnapshot", "disk")
	if !errors.Is(err, ErrAmbiguousNode) {
		t.Fatalf("expected ErrAmbiguousNode, got: %v", err)
	}
}

func TestFindNode_RootReturnsEmptyAncestors(t *testing.T) {
	t.Helper()

	root := &Node{Kind: "Snapshot", Name: "snap"}

	gotNode, gotAnc, err := FindNode(root, "Snapshot", "snap")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotNode != root {
		t.Fatal("expected root node to be returned")
	}

	if len(gotAnc) != 0 {
		t.Fatalf("expected empty ancestor chain for root, got %d ancestors", len(gotAnc))
	}
}
