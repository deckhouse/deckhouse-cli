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

package treeview_test

import (
	"bytes"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/treeview"
)

func TestRender(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		root treeview.Node
		want string
	}{
		{
			// Zero-value root: empty label, no children, no volumes.
			// Render emits a single newline for the root, then nothing.
			name: "empty root",
			root: treeview.Node{},
			want: "\n",
		},
		{
			name: "root only",
			root: treeview.Node{Label: "root"},
			want: "root\n",
		},
		{
			// A single child is the last (and only) sibling, so it uses └──.
			name: "single child",
			root: treeview.Node{
				Label: "root",
				Children: []treeview.Node{
					{Label: "child"},
				},
			},
			want: "root\n└── child\n",
		},
		{
			// Two children: first gets ├──, last gets └──.
			name: "two children last-sibling connectors",
			root: treeview.Node{
				Label: "root",
				Children: []treeview.Node{
					{Label: "child1"},
					{Label: "child2"},
				},
			},
			want: "root\n├── child1\n└── child2\n",
		},
		{
			// Volumes only — no children. The same ├──/└── last-sibling logic
			// applies to volumes since the total sibling count is len(volumes).
			name: "volumes only",
			root: treeview.Node{
				Label:   "root",
				Volumes: []string{"vol-A", "vol-B"},
			},
			want: "root\n├── vol-A\n└── vol-B\n",
		},
		{
			// Children appear before volumes (PINNED ordering). The total count
			// drives last-sibling detection across both groups together.
			name: "children before volumes",
			root: treeview.Node{
				Label: "root",
				Children: []treeview.Node{
					{Label: "child"},
				},
				Volumes: []string{"vol-A"},
			},
			want: "root\n├── child\n└── vol-A\n",
		},
		{
			// Multiple children followed by multiple volumes; last sibling
			// is vol-B (index 3 of 4 total entries).
			name: "two children two volumes ordering",
			root: treeview.Node{
				Label: "root",
				Children: []treeview.Node{
					{Label: "child1"},
					{Label: "child2"},
				},
				Volumes: []string{"vol-A", "vol-B"},
			},
			want: "root\n├── child1\n├── child2\n├── vol-A\n└── vol-B\n",
		},
		{
			// Child node carries its own volumes; the continuation prefix for
			// child's entries is the parent's continuation (four spaces, because
			// child is the last sibling under root).
			name: "child with own volumes",
			root: treeview.Node{
				Label: "root",
				Children: []treeview.Node{
					{
						Label:   "child",
						Volumes: []string{"vol-X", "vol-Y"},
					},
				},
			},
			want: "root\n└── child\n    ├── vol-X\n    └── vol-Y\n",
		},
		{
			// Three-level tree verifying correct │/space continuation at
			// every depth and last-sibling handling at each level.
			name: "nested three levels deep",
			root: treeview.Node{
				Label: "root",
				Children: []treeview.Node{
					{
						Label: "a",
						Children: []treeview.Node{
							{Label: "a1"},
							{Label: "a2"},
						},
					},
					{
						Label: "b",
						Children: []treeview.Node{
							{Label: "b1"},
						},
					},
				},
			},
			want: "root\n├── a\n│   ├── a1\n│   └── a2\n└── b\n    └── b1\n",
		},
		{
			// Realistic snapshot tree: a VM snapshot with two disk-snapshot
			// children, each owning a compressed block volume.
			name: "snapshot tree with volumes in children",
			root: treeview.Node{
				Label: "snapshot-my-vm",
				Children: []treeview.Node{
					{
						Label:   "snapshot-disk1",
						Volumes: []string{"data.bin.zst"},
					},
					{
						Label:   "snapshot-disk2",
						Volumes: []string{"data.bin.zst"},
					},
				},
			},
			want: "snapshot-my-vm\n├── snapshot-disk1\n│   └── data.bin.zst\n└── snapshot-disk2\n    └── data.bin.zst\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			err := treeview.Render(&buf, tc.root)

			if err != nil {
				t.Fatalf("Render: %v", err)
			}

			if got := buf.String(); got != tc.want {
				t.Errorf("Render output mismatch:\ngot:  %q\nwant: %q", got, tc.want)
			}
		})
	}
}
