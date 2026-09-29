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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
// its children on disk, with the encoding of format version.
func recomputeVectorChildrenChecksum(t *testing.T, nodeDir string, version int) NodeChecksum {
	t.Helper()

	source, err := OpenRootedSource(nodeDir)
	if err != nil {
		t.Fatalf("OpenRootedSource %s: %v", nodeDir, err)
	}

	defer func() { _ = source.Close() }()

	checksum, err := computeNodeChildrenChecksum(source, version, SnapshotYAMLReadOptions{})
	if err != nil {
		t.Fatalf("children checksum of %s at version %d: %v", nodeDir, version, err)
	}

	return checksum
}

// rewriteVectorSnapshotYAML reseals sy at format version and writes it as the snapshot.yaml of
// the node at nodeDir.
func rewriteVectorSnapshotYAML(t *testing.T, nodeDir string, sy SnapshotYAML, version int) {
	t.Helper()

	sealed, err := sealSnapshotYAMLAt(sy, version)
	if err != nil {
		t.Fatalf("seal %s at version %d: %v", nodeDir, version, err)
	}

	if err := WriteSnapshotYAML(nodeDir, sealed); err != nil {
		t.Fatalf("WriteSnapshotYAML %s: %v", nodeDir, err)
	}
}

// readVectorSnapshotYAML reads the snapshot.yaml of the node at rel beneath root.
func readVectorSnapshotYAML(t *testing.T, root, rel string) SnapshotYAML {
	t.Helper()

	sy, err := ReadSnapshotYAML(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("%s: ReadSnapshotYAML: %v", rel, err)
	}

	return sy
}

// replaceVectorSnapshotYAML puts the tampered descriptor file in place of the snapshot.yaml of
// the node at rel beneath root.
func replaceVectorSnapshotYAML(t *testing.T, root, rel, file string) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(vectorsDir, "tampered", file))
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(rel), SnapshotYAMLName), data, 0o644); err != nil {
		t.Fatal(err)
	}
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
	t.Run("version 4", func(t *testing.T) {
		if len(expected.ChildMetadataNodes) == 0 {
			t.Fatal("expected.json lists no version 4 nodes")
		}

		checkVectorTree(t, "tree-child-metadata", expected.TreeRoot, SnapshotFormatVersionChildMetadata,
			expected.ChildMetadataNodes)
	})
}

// TestReferenceVectorTreeResealReproducesBytes strips every digest from each vector tree and
// writes every snapshot.yaml again bottom up, children before their parent, the way d8's writers
// do. Each must come out byte for byte as stored. The version 4 tree is written by the default
// path (no version declared), so it pins what a new archive gets.
func TestReferenceVectorTreeResealReproducesBytes(t *testing.T) {
	expected := loadVectorExpected(t)

	tests := []struct {
		name    string
		tree    string
		version int
		stamp   int
	}{
		{name: "version 3", tree: "tree", version: SnapshotFormatVersionPayloadSizes, stamp: SnapshotFormatVersionPayloadSizes},
		{name: "version 4", tree: "tree-child-metadata", version: SnapshotFormatVersionCurrent, stamp: SnapshotFormatVersionLegacy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := copyVectorTree(t, tt.tree, expected.TreeRoot)
			nodes := vectorTreeNodes(t, root)

			for i := len(nodes) - 1; i >= 0; i-- {
				nodeDir := filepath.Join(root, filepath.FromSlash(nodes[i]))

				original, err := os.ReadFile(filepath.Join(nodeDir, SnapshotYAMLName))
				if err != nil {
					t.Fatal(err)
				}

				sy := readVectorSnapshotYAML(t, root, nodes[i])

				checksum, err := ComputeNodeChecksum(nodeDir)
				if err != nil {
					t.Fatalf("%s: ComputeNodeChecksum: %v", nodes[i], err)
				}

				children := recomputeVectorChildrenChecksum(t, nodeDir, tt.version)

				sy.FormatVersion = tt.stamp
				sy.Checksum = checksum
				sy.ChildrenChecksum = &children
				sy.MetadataChecksum = nil

				if err := WriteSnapshotYAML(nodeDir, sy); err != nil {
					t.Fatalf("%s: WriteSnapshotYAML: %v", nodes[i], err)
				}

				written, err := os.ReadFile(filepath.Join(nodeDir, SnapshotYAMLName))
				if err != nil {
					t.Fatal(err)
				}

				if !bytes.Equal(written, original) {
					t.Errorf("%s: written snapshot.yaml differs from the vector\n got:\n%s\nwant:\n%s",
						nodes[i], written, original)
				}
			}
		})
	}
}

// TestReferenceVectorTamperedChildMetadata puts each tampered descriptor in place of its node in
// the version 4 tree. The node still verifies on its own, but verifying the tree from the root
// must fail at the node's parent: a version 4 parent commits to its children's metadataChecksum.
func TestReferenceVectorTamperedChildMetadata(t *testing.T) {
	expected := loadVectorExpected(t)

	if len(expected.TamperedChildMetadata) == 0 {
		t.Fatal("expected.json lists no tampered descriptor vectors")
	}

	sentinels := map[string]error{"ErrChildrenChecksumMismatch": ErrChildrenChecksumMismatch}

	for _, want := range expected.TamperedChildMetadata {
		t.Run(want.File, func(t *testing.T) {
			wantErr, ok := sentinels[want.Error]
			if !ok {
				t.Fatalf("unknown error %q", want.Error)
			}

			raw, err := os.ReadFile(filepath.Join(vectorsDir, "tampered", want.File))
			if err != nil {
				t.Fatal(err)
			}

			if _, err := UnmarshalSnapshotYAML(raw, SnapshotYAMLReadOptions{}); err != nil {
				t.Fatalf("the tampered descriptor must decode on its own: %v", err)
			}

			root := copyVectorTree(t, "tree-child-metadata", expected.TreeRoot)
			replaceVectorSnapshotYAML(t, root, want.Node, want.File)

			nodeDir := filepath.Join(root, filepath.FromSlash(want.Node))
			if err := VerifyNode(nodeDir); err != nil {
				t.Fatalf("the tampered node must verify on its own: %v", err)
			}

			if err := ValidateNodeMetadata(nodeDir); err != nil {
				t.Fatalf("the tampered node must validate on its own: %v", err)
			}

			err = verifyVectorTree(t, root)
			if !errors.Is(err, wantErr) {
				t.Fatalf("verify the tree: %v, want %v", err, wantErr)
			}

			if !strings.HasPrefix(err.Error(), want.FailingNode+": ") {
				t.Errorf("error %q does not name the node %q", err, want.FailingNode)
			}
		})
	}
}

// TestReferenceVectorMixedVersions pins how a parent's own format version decides the commitment
// over children at another version.
func TestReferenceVectorMixedVersions(t *testing.T) {
	expected := loadVectorExpected(t)

	t.Run("a version 4 root over version 3 children is refused", func(t *testing.T) {
		root := copyVectorTree(t, "tree", expected.TreeRoot)

		// A writer cannot commit a current parent to them.
		if _, err := ComputeNodeChildrenChecksum(root); !errors.Is(err, ErrInvalidSnapshotYAML) {
			t.Fatalf("children checksum at the current version over version 3 children: %v, want ErrInvalidSnapshotYAML", err)
		}

		// A root restamped at version 4 over them fails, whatever it commits to.
		rewriteVectorSnapshotYAML(t, root, readVectorSnapshotYAML(t, root, "."), SnapshotFormatVersionChildMetadata)

		err := verifyVectorTree(t, root)
		if !errors.Is(err, ErrInvalidSnapshotYAML) {
			t.Fatalf("verify the tree: %v, want ErrInvalidSnapshotYAML", err)
		}

		if !strings.HasPrefix(err.Error(), ".: ") {
			t.Errorf("error %q does not name the root", err)
		}
	})

	t.Run("a version 3 root over version 4 children verifies", func(t *testing.T) {
		root := copyVectorTree(t, "tree-child-metadata", expected.TreeRoot)

		sy := readVectorSnapshotYAML(t, root, ".")
		children := recomputeVectorChildrenChecksum(t, root, SnapshotFormatVersionPayloadSizes)
		sy.ChildrenChecksum = &children
		rewriteVectorSnapshotYAML(t, root, sy, SnapshotFormatVersionPayloadSizes)

		if err := verifyVectorTree(t, root); err != nil {
			t.Fatalf("verify the tree: %v", err)
		}

		// The version 3 encoding does not commit to the children's metadata, so a resealed edit
		// of a direct child's descriptor still verifies under it.
		for _, tampered := range expected.TamperedChildMetadata {
			if tampered.FailingNode == "." {
				replaceVectorSnapshotYAML(t, root, tampered.Node, tampered.File)
			}
		}

		if err := verifyVectorTree(t, root); err != nil {
			t.Errorf("verify the tree after a resealed child edit: %v", err)
		}
	})

	t.Run("an old commitment relabelled as version 4 fails", func(t *testing.T) {
		root := copyVectorTree(t, "tree-child-metadata", expected.TreeRoot)

		sy := readVectorSnapshotYAML(t, root, ".")
		children := recomputeVectorChildrenChecksum(t, root, SnapshotFormatVersionPayloadSizes)
		sy.ChildrenChecksum = &children
		rewriteVectorSnapshotYAML(t, root, sy, SnapshotFormatVersionChildMetadata)

		err := verifyVectorTree(t, root)
		if !errors.Is(err, ErrChildrenChecksumMismatch) {
			t.Fatalf("verify the tree: %v, want ErrChildrenChecksumMismatch", err)
		}

		if !strings.HasPrefix(err.Error(), ".: ") {
			t.Errorf("error %q does not name the root", err)
		}
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

			// Encoding the decoded descriptor again keeps its version, and so its metadata
			// checksum. The vector files are written by hand, so their bytes are not compared.
			rewriteDir := t.TempDir()
			if err := WriteSnapshotYAML(rewriteDir, sy); err != nil {
				t.Fatalf("WriteSnapshotYAML: %v", err)
			}

			rewritten, err := ReadSnapshotYAML(rewriteDir)
			if err != nil {
				t.Fatalf("ReadSnapshotYAML of the rewritten descriptor: %v", err)
			}

			if rewritten.FormatVersion != want.FormatVersion || rewritten.MetadataChecksum == nil ||
				rewritten.MetadataChecksum.Hex != want.MetadataChecksum {
				t.Errorf("rewritten as version %d with metadataChecksum %+v, want version %d with %s",
					rewritten.FormatVersion, rewritten.MetadataChecksum, want.FormatVersion, want.MetadataChecksum)
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
