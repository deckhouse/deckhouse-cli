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

package util

import (
	"bytes"
	"strings"
	"testing"

	v1alpha1 "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

// demoView mirrors the plan's demo tree: Snap/app -> {VM/vm -> {Disk/os data, Disk/data data}, Disk/extra data}.
func demoView() *v1alpha1.SnapshotView {
	return &v1alpha1.SnapshotView{
		Version: "v1",
		Root: v1alpha1.SnapshotViewNode{
			APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "Snapshot", Namespace: "ns", Name: "app",
			Children: []v1alpha1.SnapshotViewNode{
				{
					APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1", Kind: "DemoVirtualMachineSnapshot", Namespace: "ns", Name: "vm",
					Children: []v1alpha1.SnapshotViewNode{
						{Kind: "DemoVirtualDiskSnapshot", Namespace: "ns", Name: "os", HasData: true, VolumeMode: "Block", SizeBytes: 2 * 1024 * 1024 * 1024},
						{Kind: "DemoVirtualDiskSnapshot", Namespace: "ns", Name: "data", HasData: true, VolumeMode: "Filesystem"},
					},
				},
				{Kind: "DemoVirtualDiskSnapshot", Namespace: "ns", Name: "extra", HasData: true, VolumeMode: "Block", SizeBytes: 500},
			},
		},
	}
}

func TestRenderView_Tree(t *testing.T) {
	var buf bytes.Buffer
	RenderView(&buf, demoView())
	got := buf.String()

	for _, want := range []string{
		"Snapshot/app [ns]",
		"DemoVirtualMachineSnapshot/vm [ns]",
		"DemoVirtualDiskSnapshot/os [ns] (data: Block, 2.0GiB)",
		"DemoVirtualDiskSnapshot/data [ns] (data: Filesystem)",
		"DemoVirtualDiskSnapshot/extra [ns] (data: Block, 500B)",
		"├── ",
		"└── ",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered tree missing %q\n---\n%s", want, got)
		}
	}

	// The root must not be prefixed by a branch glyph.
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 nodes rendered, got %d:\n%s", len(lines), got)
	}
	if strings.HasPrefix(lines[0], " ") || strings.Contains(lines[0], "── ") {
		t.Errorf("root line should carry no branch prefix, got %q", lines[0])
	}
	// A dataless node must carry no data suffix.
	if strings.Contains(lines[1], "(data") {
		t.Errorf("dataless VM node should have no data suffix, got %q", lines[1])
	}
}

func TestParseView(t *testing.T) {
	raw := []byte(`{"version":"v1","root":{"apiVersion":"storage.deckhouse.io/v1alpha1","kind":"Snapshot","namespace":"ns","name":"app","hasData":false}}`)
	v, err := ParseView(raw)
	if err != nil {
		t.Fatalf("ParseView: %v", err)
	}
	if v.Root.Name != "app" || v.Version != "v1" {
		t.Fatalf("unexpected parsed view: %+v", v)
	}

	if _, err := ParseView([]byte(`{"version":"v1","root":{}}`)); err == nil {
		t.Fatal("expected error for a view with an empty root")
	}
	if _, err := ParseView([]byte("not json")); err == nil {
		t.Fatal("expected a parse error for malformed JSON")
	}
}

func TestHumanBytes(t *testing.T) {
	cases := map[int64]string{
		0:                      "0B",
		512:                    "512B",
		1024:                   "1.0KiB",
		2 * 1024 * 1024:        "2.0MiB",
		3 * 1024 * 1024 * 1024: "3.0GiB",
	}
	for in, want := range cases {
		if got := humanBytes(in); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}
