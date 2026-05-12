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

package fs

import (
	"io/fs"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/imagefs"
)

// buildTree relies on sorted parent-before-children ordering from
// imagefs.MergedFS. The defensive guard in insert() keeps the tree
// consistent even when that ordering assumption is violated (unsorted
// caller, malformed tar carrying both "etc" as a file and "etc/passwd"
// as a file under it). Without the guard, the file entry for "etc"
// arriving after its descendants would flip child.isDir back to false
// and hide the subtree from the rendered output.
func TestBuildTree_IsDirIsMonotonicAcrossUnsortedInput(t *testing.T) {
	// Note: deliberately unsorted - "etc/passwd" before "etc" - to mimic
	// the worst case where the parent entry is observed last.
	entries := []imagefs.Entry{
		{Path: "etc/passwd", Type: imagefs.TypeFile, Mode: 0o644},
		{Path: "etc", Type: imagefs.TypeFile, Mode: 0o644},
	}
	tree := buildTree(entries, "")

	etc, ok := tree.children["etc"]
	if !ok {
		t.Fatalf("etc node missing from tree: %+v", tree.children)
	}
	if !etc.isDir {
		t.Fatalf("etc.isDir collapsed to false; subtree would be hidden")
	}
	if _, ok := etc.children["passwd"]; !ok {
		t.Fatalf("passwd descendant missing under etc: %+v", etc.children)
	}
}

// Sorted, well-formed input must keep its natural directory/file flags.
// This is the path actually exercised in production by `fs tree`.
func TestBuildTree_SortedInputKeepsNaturalIsDir(t *testing.T) {
	entries := []imagefs.Entry{
		{Path: "etc", Type: imagefs.TypeDir, Mode: fs.ModeDir | 0o755},
		{Path: "etc/passwd", Type: imagefs.TypeFile, Mode: 0o644},
	}
	tree := buildTree(entries, "")

	etc, ok := tree.children["etc"]
	if !ok || !etc.isDir {
		t.Fatalf("etc must be a directory: ok=%v isDir=%v", ok, etc != nil && etc.isDir)
	}
	passwd, ok := etc.children["passwd"]
	if !ok {
		t.Fatalf("passwd must live under etc: %+v", etc.children)
	}
	if passwd.isDir {
		t.Fatalf("passwd must remain a regular file, got isDir=true")
	}
}
