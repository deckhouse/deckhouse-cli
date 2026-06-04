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

package listing_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/listing"
)

func makeTestTree() *listing.Tree {
	return &listing.Tree{
		Source: listing.Source{
			Kind:      "cluster",
			Cluster:   "https://example.com:6443",
			Namespace: "demo",
			Snapshot:  "my-snap",
		},
		Selection: archive.SelectionFull,
		Root: &listing.NodeView{
			ID:          "Snapshot--my-snap",
			APIVersion:  "storage.deckhouse.io/v1alpha1",
			Kind:        "Snapshot",
			Name:        "my-snap",
			Namespace:   "demo",
			ObjectCount: 2,
			Objects: []listing.ObjectView{
				{APIVersion: "v1", Kind: "ConfigMap", Name: "cm1", Namespace: "demo"},
				{APIVersion: "apps/v1", Kind: "Deployment", Name: "app", Namespace: "demo"},
			},
			Children: []*listing.NodeView{
				{
					ID:          "Snapshot--child",
					Kind:        "Snapshot",
					Name:        "child",
					Namespace:   "demo",
					ObjectCount: 1,
				},
			},
		},
	}
}

func TestRenderHuman_ContainsNodeLines(t *testing.T) {
	var buf bytes.Buffer

	if err := listing.Render(&buf, makeTestTree(), listing.FormatHuman); err != nil {
		t.Fatalf("Render human: %v", err)
	}

	out := buf.String()

	checks := []string{
		"Snapshot--my-snap",
		"Snapshot/my-snap",
		"Snapshot--child",
		"(2 objects)",
	}

	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Fatalf("human output missing %q:\n%s", want, out)
		}
	}
}

func TestRenderHuman_ObjectLines(t *testing.T) {
	var buf bytes.Buffer

	if err := listing.Render(&buf, makeTestTree(), listing.FormatHuman); err != nil {
		t.Fatalf("Render human: %v", err)
	}

	out := buf.String()

	if !strings.Contains(out, "ConfigMap") {
		t.Fatalf("human output missing ConfigMap object line:\n%s", out)
	}

	if !strings.Contains(out, "Deployment") {
		t.Fatalf("human output missing Deployment object line:\n%s", out)
	}
}

func TestRenderJSON_RoundTrip(t *testing.T) {
	tree := makeTestTree()

	var buf bytes.Buffer

	if err := listing.Render(&buf, tree, listing.FormatJSON); err != nil {
		t.Fatalf("Render json: %v", err)
	}

	var got listing.Tree

	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal json output: %v", err)
	}

	if got.Root == nil {
		t.Fatal("json round-trip: Root is nil")
	}

	if got.Root.ID != tree.Root.ID {
		t.Fatalf("json round-trip Root.ID = %q, want %q", got.Root.ID, tree.Root.ID)
	}

	if len(got.Root.Children) != 1 {
		t.Fatalf("json round-trip: expected 1 child, got %d", len(got.Root.Children))
	}
}

func TestRenderYAML_ContainsFields(t *testing.T) {
	var buf bytes.Buffer

	if err := listing.Render(&buf, makeTestTree(), listing.FormatYAML); err != nil {
		t.Fatalf("Render yaml: %v", err)
	}

	out := buf.String()

	checks := []string{"kind: cluster", "snapshot: my-snap", "selection: full"}

	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Fatalf("yaml output missing %q:\n%s", want, out)
		}
	}
}

func TestRenderHuman_TreeConnectors(t *testing.T) {
	var buf bytes.Buffer

	if err := listing.Render(&buf, makeTestTree(), listing.FormatHuman); err != nil {
		t.Fatalf("Render human: %v", err)
	}

	out := buf.String()
	lines := strings.Split(out, "\n")

	// Root line must start with the node ID, with no leading connector.
	var rootLine string
	for _, l := range lines {
		if strings.HasPrefix(l, "Snapshot--my-snap") {
			rootLine = l
			break
		}
	}
	if rootLine == "" {
		t.Fatalf("root line not found in output:\n%s", out)
	}
	if strings.HasPrefix(rootLine, "├─") || strings.HasPrefix(rootLine, "└─") {
		t.Fatalf("root line must not start with a connector, got: %q", rootLine)
	}

	// There must be at least one ├─ connector (objects before the last one).
	if !strings.Contains(out, "├─ ") {
		t.Fatalf("expected ├─ connector in output:\n%s", out)
	}

	// There must be at least one └─ connector (last sibling in each group).
	if !strings.Contains(out, "└─ ") {
		t.Fatalf("expected └─ connector in output:\n%s", out)
	}

	// The child node line must start with a connector (not raw indentation).
	var childLine string
	for _, l := range lines {
		if strings.Contains(l, "Snapshot--child") {
			childLine = l
			break
		}
	}
	if childLine == "" {
		t.Fatalf("child node line not found in output:\n%s", out)
	}
	if !strings.HasPrefix(childLine, "├─ ") && !strings.HasPrefix(childLine, "└─ ") {
		t.Fatalf("child node line must start with a connector, got: %q", childLine)
	}
}

func TestRenderArchiveHuman(t *testing.T) {
	tree := &listing.Tree{
		Source: listing.Source{
			Kind:       "archive",
			ArchiveDir: "/tmp/snap",
			ArchiveID:  "a-20260604",
		},
		Selection: archive.SelectionFull,
		Complete:  true,
		Root: &listing.NodeView{
			ID:          "Snapshot--my-snap",
			Kind:        "Snapshot",
			Name:        "my-snap",
			ObjectCount: 5,
		},
	}

	var buf bytes.Buffer

	if err := listing.Render(&buf, tree, listing.FormatHuman); err != nil {
		t.Fatalf("Render human (archive): %v", err)
	}

	out := buf.String()

	if !strings.Contains(out, "a-20260604") {
		t.Fatalf("human output missing archive ID:\n%s", out)
	}

	if !strings.Contains(out, "Complete:  yes") {
		t.Fatalf("human output missing Complete line:\n%s", out)
	}
}
