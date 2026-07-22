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

import "testing"

func TestNode_DisplayLabel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		node *Node
		want string
	}{
		{
			name: "child with SourceRef prefers captured source identity",
			node: &Node{
				Kind: "DemoDiskSnapshot",
				Name: "nss-child-abc123",
				Parent: &Node{
					Kind: "Snapshot",
					Name: "root-snap",
				},
				SourceRef: &SourceRefIdentity{Kind: "DemoDiskSnapshot", Name: "my-disk"},
			},
			want: "DemoDiskSnapshot/my-disk",
		},
		{
			name: "child with nil SourceRef falls back to CR identity",
			node: &Node{
				Kind: "DemoDiskSnapshot",
				Name: "nss-child-abc123",
				Parent: &Node{
					Kind: "Snapshot",
					Name: "root-snap",
				},
			},
			want: "DemoDiskSnapshot/nss-child-abc123",
		},
		{
			name: "child with SourceRef missing Name falls back to CR identity",
			node: &Node{
				Kind: "DemoDiskSnapshot",
				Name: "nss-child-abc123",
				Parent: &Node{
					Kind: "Snapshot",
					Name: "root-snap",
				},
				SourceRef: &SourceRefIdentity{Kind: "DemoDiskSnapshot"},
			},
			want: "DemoDiskSnapshot/nss-child-abc123",
		},
		{
			name: "orphan volume leaf with SourceRef prefers captured PVC name",
			node: &Node{
				Kind: "VolumeSnapshot",
				Name: "nss-vs-xyz",
				Parent: &Node{
					Kind: "Snapshot",
					Name: "root-snap",
				},
				SourceRef: &SourceRefIdentity{Kind: "PersistentVolumeClaim", Name: "pvc-orphan"},
			},
			want: "PersistentVolumeClaim/pvc-orphan",
		},
		{
			name: "root node with SourceRef set still reports its own typed identity",
			node: &Node{
				Kind:      "Snapshot",
				Name:      "root-snap",
				SourceRef: &SourceRefIdentity{Kind: "Namespace", Name: "some-ns"},
			},
			want: "Snapshot/root-snap",
		},
		{
			name: "root node with nil SourceRef reports its own identity",
			node: &Node{
				Kind: "Snapshot",
				Name: "root-snap",
			},
			want: "Snapshot/root-snap",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.node.DisplayLabel()
			if got != tc.want {
				t.Fatalf("DisplayLabel(): got %q, want %q", got, tc.want)
			}
		})
	}
}
