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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// The reference vectors of the snapshot archive format, copied from state-snapshotter (see
// testdata/snapshotarchive/README.md). They pin this package's implementation of the format to
// the reference one: every digest d8 computes over them must equal the one expected.json lists.

// vectorsDir holds the reference vectors.
var vectorsDir = filepath.Join("testdata", "snapshotarchive")

// vectorNode is the digests one node of a vector tree must have.
type vectorNode struct {
	Path             string `json:"path"`
	Checksum         string `json:"checksum"`
	ChildrenChecksum string `json:"childrenChecksum"`
	MetadataChecksum string `json:"metadataChecksum"`
}

// vectorDescriptor is one stand-alone descriptor vector.
type vectorDescriptor struct {
	File             string `json:"file"`
	FormatVersion    int    `json:"formatVersion"`
	MetadataChecksum string `json:"metadataChecksum"`
}

// vectorTampered is one tampered descriptor vector: File replaces the snapshot.yaml of Node in
// the version 4 tree, and verification must then fail at FailingNode with Error.
type vectorTampered struct {
	File        string `json:"file"`
	Node        string `json:"node"`
	FailingNode string `json:"failingNode"`
	Error       string `json:"error"`
}

// vectorExpected is expected.json.
type vectorExpected struct {
	EmptyChildrenChecksum string             `json:"emptyChildrenChecksum"`
	TreeRoot              string             `json:"treeRoot"`
	Nodes                 []vectorNode       `json:"nodes"`
	Descriptors           []vectorDescriptor `json:"descriptors"`
	ChildMetadataNodes    []vectorNode       `json:"childMetadataNodes"`
	TamperedChildMetadata []vectorTampered   `json:"tamperedChildMetadata"`
}

func loadVectorExpected(t *testing.T) vectorExpected {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(vectorsDir, "expected.json"))
	if err != nil {
		t.Fatalf("read expected.json: %v", err)
	}

	var expected vectorExpected
	if err := json.Unmarshal(data, &expected); err != nil {
		t.Fatalf("decode expected.json: %v", err)
	}

	if expected.TreeRoot == "" || len(expected.Nodes) == 0 || len(expected.Descriptors) == 0 {
		t.Fatal("expected.json lists no vectors")
	}

	return expected
}

// copyVectorTree copies the vector tree testdata/snapshotarchive/<tree> into a fresh temporary
// directory, so that tests may rewrite it, and returns the path of its root node.
func copyVectorTree(t *testing.T, tree, treeRoot string) string {
	t.Helper()

	dir := t.TempDir()
	if err := os.CopyFS(dir, os.DirFS(filepath.Join(vectorsDir, tree))); err != nil {
		t.Fatalf("copy vector tree %s: %v", tree, err)
	}

	return filepath.Join(dir, treeRoot)
}

// vectorTreeNodes lists the node directories of the tree at root, relative to it and
// "/"-separated, parents before their children.
func vectorTreeNodes(t *testing.T, root string) []string {
	t.Helper()

	var nodes []string

	var walk func(rel string)
	walk = func(rel string) {
		nodes = append(nodes, rel)

		entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(rel), SnapshotsDirName))
		if os.IsNotExist(err) {
			return
		}

		if err != nil {
			t.Fatalf("read children of %s: %v", rel, err)
		}

		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}

		sort.Strings(names)

		for _, name := range names {
			child := SnapshotsDirName + "/" + name
			if rel != "." {
				child = rel + "/" + child
			}

			walk(child)
		}
	}

	walk(".")

	return nodes
}

// verifyVectorTree verifies every node of the tree at root from the root down, through both the
// local verification path (VerifyNode and ValidateNodeMetadata, as `d8 snapshot local` runs
// them) and the upload path (VerifiedArchive.VerifyNode). It returns the first failure, prefixed
// with the relative path of the node it failed at.
func verifyVectorTree(t *testing.T, root string) error {
	t.Helper()

	verified, err := OpenVerifiedArchive(root)
	if err != nil {
		t.Fatalf("OpenVerifiedArchive: %v", err)
	}

	defer func() { _ = verified.Close() }()

	for _, rel := range vectorTreeNodes(t, root) {
		nodeDir := filepath.Join(root, filepath.FromSlash(rel))

		if err := VerifyNode(nodeDir); err != nil {
			return fmt.Errorf("%s: VerifyNode: %w", rel, err)
		}

		if err := ValidateNodeMetadata(nodeDir); err != nil {
			return fmt.Errorf("%s: ValidateNodeMetadata: %w", rel, err)
		}

		if _, err := verified.VerifyNode(context.Background(), nodeDir); err != nil {
			return fmt.Errorf("%s: VerifiedArchive.VerifyNode: %w", rel, err)
		}
	}

	return nil
}

// recomputeVectorChildrenChecksum recomputes the children checksum of the node at nodeDir from
// its children on disk.
func recomputeVectorChildrenChecksum(t *testing.T, nodeDir string, _ int) NodeChecksum {
	t.Helper()

	checksum, err := ComputeNodeChildrenChecksum(nodeDir)
	if err != nil {
		t.Fatalf("ComputeNodeChildrenChecksum %s: %v", nodeDir, err)
	}

	return checksum
}

// checkVectorTree verifies the vector tree testdata/snapshotarchive/<tree> and compares every
// node's recorded and recomputed digests and its format version with want. It also requires
// encoding each decoded descriptor again to reproduce its snapshot.yaml byte for byte.
func checkVectorTree(t *testing.T, tree, treeRoot string, version int, want []vectorNode) {
	t.Helper()

	root := copyVectorTree(t, tree, treeRoot)

	if err := verifyVectorTree(t, root); err != nil {
		t.Fatalf("the vector tree must verify: %v", err)
	}

	if nodes := vectorTreeNodes(t, root); len(nodes) != len(want) {
		t.Fatalf("vector tree has %d nodes, expected.json lists %d", len(nodes), len(want))
	}

	for _, node := range want {
		nodeDir := filepath.Join(root, filepath.FromSlash(node.Path))

		sy, err := ReadSnapshotYAML(nodeDir)
		if err != nil {
			t.Fatalf("%s: ReadSnapshotYAML: %v", node.Path, err)
		}

		if sy.FormatVersion != version {
			t.Errorf("%s: formatVersion %d, want %d", node.Path, sy.FormatVersion, version)
		}

		checksum, err := ComputeNodeChecksum(nodeDir)
		if err != nil {
			t.Fatalf("%s: ComputeNodeChecksum: %v", node.Path, err)
		}

		if checksum.Hex != node.Checksum || sy.Checksum.Hex != node.Checksum {
			t.Errorf("%s: checksum computed %s, recorded %s, want %s",
				node.Path, checksum.Hex, sy.Checksum.Hex, node.Checksum)
		}

		children := recomputeVectorChildrenChecksum(t, nodeDir, sy.FormatVersion)
		if children.Hex != node.ChildrenChecksum || sy.ChildrenChecksum == nil ||
			sy.ChildrenChecksum.Hex != node.ChildrenChecksum {
			t.Errorf("%s: childrenChecksum computed %s, recorded %+v, want %s",
				node.Path, children.Hex, sy.ChildrenChecksum, node.ChildrenChecksum)
		}

		metadata, err := computeSnapshotMetadataChecksum(sy)
		if err != nil {
			t.Fatalf("%s: computeSnapshotMetadataChecksum: %v", node.Path, err)
		}

		if metadata.Hex != node.MetadataChecksum || sy.MetadataChecksum == nil ||
			sy.MetadataChecksum.Hex != node.MetadataChecksum {
			t.Errorf("%s: metadataChecksum computed %s, recorded %+v, want %s",
				node.Path, metadata.Hex, sy.MetadataChecksum, node.MetadataChecksum)
		}

		original, err := os.ReadFile(filepath.Join(nodeDir, SnapshotYAMLName))
		if err != nil {
			t.Fatal(err)
		}

		rewriteDir := t.TempDir()
		if err := WriteSnapshotYAML(rewriteDir, sy); err != nil {
			t.Fatalf("%s: WriteSnapshotYAML: %v", node.Path, err)
		}

		rewritten, err := os.ReadFile(filepath.Join(rewriteDir, SnapshotYAMLName))
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(rewritten, original) {
			t.Errorf("%s: rewritten snapshot.yaml differs from the vector\n got:\n%s\nwant:\n%s",
				node.Path, rewritten, original)
		}
	}
}

func TestReferenceVectorTree(t *testing.T) {
	expected := loadVectorExpected(t)

	t.Run("version 3", func(t *testing.T) {
		checkVectorTree(t, "tree", expected.TreeRoot, SnapshotFormatVersionPayloadSizes, expected.Nodes)
	})
}

// TestReferenceVectorDescriptors decodes the stand-alone descriptor vectors and requires their
// canonical form to be exactly the bytes the reference takes the metadata checksum over.
func TestReferenceVectorDescriptors(t *testing.T) {
	expected := loadVectorExpected(t)

	for _, want := range expected.Descriptors {
		t.Run(want.File, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(vectorsDir, "descriptors", want.File))
			if err != nil {
				t.Fatal(err)
			}

			sy, err := UnmarshalSnapshotYAML(raw, SnapshotYAMLReadOptions{})
			if err != nil {
				t.Fatalf("UnmarshalSnapshotYAML: %v", err)
			}

			if sy.FormatVersion != want.FormatVersion {
				t.Errorf("formatVersion %d, want %d", sy.FormatVersion, want.FormatVersion)
			}

			metadata, err := computeSnapshotMetadataChecksum(sy)
			if err != nil {
				t.Fatal(err)
			}

			if metadata.Hex != want.MetadataChecksum {
				t.Errorf("metadataChecksum %s, want %s", metadata.Hex, want.MetadataChecksum)
			}

			canonical, err := os.ReadFile(filepath.Join(vectorsDir, "canonical", want.File+".json"))
			if err != nil {
				t.Fatal(err)
			}

			stripped := sy
			stripped.MetadataChecksum = nil

			encoded, err := json.Marshal(snapshotYAMLWire(stripped))
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(encoded, canonical) {
				t.Errorf("canonical JSON differs from the vector\n got: %s\nwant: %s", encoded, canonical)
			}

			sum := sha256.Sum256(canonical)
			if hex.EncodeToString(sum[:]) != want.MetadataChecksum {
				t.Errorf("sha256 of the canonical JSON vector is %x, want %s", sum, want.MetadataChecksum)
			}
		})
	}
}

func TestReferenceVectorEmptyChildrenChecksum(t *testing.T) {
	expected := loadVectorExpected(t)

	if got := EmptyChildrenChecksum().Hex; got != expected.EmptyChildrenChecksum {
		t.Errorf("EmptyChildrenChecksum %s, want %s", got, expected.EmptyChildrenChecksum)
	}
}
