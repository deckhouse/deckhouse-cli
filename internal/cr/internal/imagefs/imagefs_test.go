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

package imagefs

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

type tarFile struct {
	name     string
	typeflag byte
	content  []byte
	linkname string
	mode     int64
}

func buildTar(t *testing.T, files []tarFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, f := range files {
		hdr := &tar.Header{
			Name:     f.name,
			Typeflag: f.typeflag,
			Size:     int64(len(f.content)),
			Linkname: f.linkname,
			Mode:     f.mode,
		}
		if hdr.Mode == 0 {
			if f.typeflag == tar.TypeDir {
				hdr.Mode = 0o755
			} else {
				hdr.Mode = 0o644
			}
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("write header %s: %v", f.name, err)
		}
		if len(f.content) > 0 {
			if _, err := tw.Write(f.content); err != nil {
				t.Fatalf("write body %s: %v", f.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	return buf.Bytes()
}

// makeImage constructs a v1.Image out of a list of layer file sets (bottom first).
func makeImage(t *testing.T, layers [][]tarFile) v1.Image {
	t.Helper()
	img := empty.Image
	for _, files := range layers {
		data := buildTar(t, files)
		layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(data)), nil
		})
		if err != nil {
			t.Fatalf("LayerFromOpener: %v", err)
		}
		img, err = mutate.AppendLayers(img, layer)
		if err != nil {
			t.Fatalf("AppendLayers: %v", err)
		}
	}
	return img
}

// ---- tests ----

func TestMergedFS_WhiteoutDeletesFile(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{
			{name: "bin/", typeflag: tar.TypeDir},
			{name: "bin/sh", typeflag: tar.TypeReg, content: []byte("sh-content")},
			{name: "bin/cat", typeflag: tar.TypeReg, content: []byte("cat-content")},
		},
		{
			{name: "bin/.wh.sh", typeflag: tar.TypeReg},
		},
	})
	entries, err := MergedFS(img)
	if err != nil {
		t.Fatalf("MergedFS: %v", err)
	}
	paths := pathsOf(entries)
	if contains(paths, "bin/sh") {
		t.Errorf("bin/sh should have been whited out, got paths: %v", paths)
	}
	if !contains(paths, "bin/cat") {
		t.Errorf("bin/cat should remain, got paths: %v", paths)
	}
}

func TestMergedFS_OpaqueMarkerWipesDir(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{
			{name: "var/", typeflag: tar.TypeDir},
			{name: "var/log/", typeflag: tar.TypeDir},
			{name: "var/log/old.log", typeflag: tar.TypeReg, content: []byte("old")},
			{name: "var/log/keep.log", typeflag: tar.TypeReg, content: []byte("keep")},
		},
		{
			{name: "var/log/.wh..wh..opq", typeflag: tar.TypeReg},
			{name: "var/log/new.log", typeflag: tar.TypeReg, content: []byte("new")},
		},
	})
	entries, err := MergedFS(img)
	if err != nil {
		t.Fatalf("MergedFS: %v", err)
	}
	paths := pathsOf(entries)
	if contains(paths, "var/log/old.log") || contains(paths, "var/log/keep.log") {
		t.Errorf("opaque marker should have wiped var/log/ from lower layers: %v", paths)
	}
	if !contains(paths, "var/log/new.log") {
		t.Errorf("new entry in opaque-layer should remain: %v", paths)
	}
}

func TestMergedFS_UpperLayerWins(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("v1")}},
		{{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("v2")}},
	})
	entries, err := MergedFS(img)
	if err != nil {
		t.Fatalf("MergedFS: %v", err)
	}
	for _, e := range entries {
		if e.Path == "etc/passwd" && e.Size != 2 {
			t.Errorf("expected size 2 ('v2'), got %d", e.Size)
		}
	}
}

func TestReadFile_FollowsTopLayer(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("v1")}},
		{{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("v2")}},
	})
	got, err := ReadFile(img, "etc/passwd")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "v2" {
		t.Errorf("want v2, got %q", got)
	}
}

func TestReadFile_WhiteoutReturnsNotFound(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("v1")}},
		{{name: "etc/.wh.passwd", typeflag: tar.TypeReg}},
	})
	_, err := ReadFile(img, "etc/passwd")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestReadFile_WhiteoutThenReAddInSameLayer(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("v1")}},
		{
			{name: "etc/.wh.passwd", typeflag: tar.TypeReg},
			{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("v2")},
		},
	})
	got, err := ReadFile(img, "etc/passwd")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("want v2, got %q", string(got))
	}
}

// Tar-order-independence guarantee: a same-layer re-add of wantPath wins
// even when the whiteout entry comes AFTER it in tar order. OCI says
// whiteouts apply to lower layers only - never to entries from their own
// layer, regardless of position. Pre-fix: readFromLayer's "latest match
// wins" walk returned readDeleted for this layout, diverging from
// mergeLayer (which the rest of the fs/ subcommands use).
func TestReadFile_ReAddThenWhiteoutInSameLayer(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("v1")}},
		{
			{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("v2")},
			{name: "etc/.wh.passwd", typeflag: tar.TypeReg},
		},
	})
	got, err := ReadFile(img, "etc/passwd")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("want v2, got %q", string(got))
	}
}

// A regular whiteout on a directory ("etc/.wh.subdir" or "/.wh.etc") wipes
// every descendant from lower layers, not just exact-target matches. Pre-fix:
// readFromLayer applied this rule to opaque markers only, so cat would
// happily return a file already removed from the merged FS view exposed by
// fs ls / fs tree.
func TestReadFile_RegularWhiteoutOnAncestorDeletesDescendants(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("secret")}},
		{{name: ".wh.etc", typeflag: tar.TypeReg}},
	})
	_, err := ReadFile(img, "etc/passwd")
	if err == nil {
		t.Fatalf("expected ErrNotFound, got nil")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestReadFile_NotRegularFileReturnsError(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/passwd", typeflag: tar.TypeReg, content: []byte("v1")},
			{name: "etc/link", typeflag: tar.TypeSymlink, linkname: "passwd"},
		},
	})

	if _, err := ReadFile(img, "etc"); err == nil || !strings.Contains(err.Error(), ErrNotRegularFile.Error()) {
		t.Fatalf("expected not-regular-file error for directory, got: %v", err)
	}
	if _, err := ReadFile(img, "etc/link"); err == nil || !strings.Contains(err.Error(), ErrNotRegularFile.Error()) {
		t.Fatalf("expected not-regular-file error for symlink, got: %v", err)
	}
}

// ReadFile must refuse to load a regular file whose size exceeds
// maxReadFileSize, so that `fs cat` on a multi-GB log entry cannot OOM
// the CLI. The check is placed at the producer of `content` (after
// LimitReader), so the in-memory buffer never exceeds the limit + 1 byte.
func TestReadFile_RejectsFileLargerThanLimit(t *testing.T) {
	prev := maxReadFileSize
	maxReadFileSize = 8
	t.Cleanup(func() { maxReadFileSize = prev })

	img := makeImage(t, [][]tarFile{
		{{name: "big.log", typeflag: tar.TypeReg, content: bytes.Repeat([]byte("X"), 16)}},
	})
	_, err := ReadFile(img, "big.log")
	if err == nil {
		t.Fatalf("expected ErrFileTooLarge, got nil")
	}
	if !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("expected ErrFileTooLarge, got: %v", err)
	}
}

// A file exactly at the limit must be readable. Guards against an off-by-one
// in the LimitReader+check pair.
func TestReadFile_AcceptsFileAtLimit(t *testing.T) {
	prev := maxReadFileSize
	maxReadFileSize = 8
	t.Cleanup(func() { maxReadFileSize = prev })

	want := bytes.Repeat([]byte("Y"), 8)
	img := makeImage(t, [][]tarFile{
		{{name: "small.log", typeflag: tar.TypeReg, content: want}},
	})
	got, err := ReadFile(img, "small.log")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("content=%q, want %q", got, want)
	}
}

func TestSafeJoin_PathEscapeRejected(t *testing.T) {
	cases := []struct {
		base, rel string
		wantErr   bool
	}{
		{"/tmp/x", "foo/bar", false},
		{"/tmp/x", "a/b/c", false},
		{"/tmp/x", "../etc/passwd", true},
		{"/tmp/x", "/absolute/evil", false}, // leading / is stripped by safeJoin semantics
		{"/tmp/x", "sub/../../escape", true},
	}
	for _, c := range cases {
		_, err := safeJoin(c.base, c.rel)
		if (err != nil) != c.wantErr {
			t.Errorf("safeJoin(%q, %q): err=%v, wantErr=%v", c.base, c.rel, err, c.wantErr)
		}
	}
}

func TestExtractMerged_RejectsEscapingSymlinkTarget(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{
			{name: "root/", typeflag: tar.TypeDir},
			{name: "root/link", typeflag: tar.TypeSymlink, linkname: "../../outside"},
		},
	})
	dest := filepath.Join(t.TempDir(), "dest")
	_, err := ExtractMerged(context.Background(), img, dest)
	if err == nil {
		t.Fatalf("expected symlink target validation error")
	}
	if !strings.Contains(err.Error(), "unsafe symlink target") {
		t.Fatalf("expected unsafe symlink target error, got: %v", err)
	}
}

func TestExtractMerged_RejectsWriteThroughSymlinkComponent(t *testing.T) {
	tmp := t.TempDir()
	dest := filepath.Join(tmp, "dest")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatalf("mkdir dest: %v", err)
	}
	if err := os.Symlink("../outside", filepath.Join(dest, "leak")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	img := makeImage(t, [][]tarFile{
		{
			{name: "leak/pwned.txt", typeflag: tar.TypeReg, content: []byte("owned")},
		},
	})

	_, err := ExtractMerged(context.Background(), img, dest)
	if err == nil {
		t.Fatalf("expected symlink-component rejection")
	}
	if !strings.Contains(err.Error(), "symlink component") {
		t.Fatalf("expected symlink-component error, got: %v", err)
	}
}

func TestExtractMerged_RewritesAbsoluteSymlinkTarget(t *testing.T) {
	// Alpine/busybox-style layout: /bin/busybox is the real binary,
	// /bin/sh and /usr/bin/awk are absolute symlinks into it.
	img := makeImage(t, [][]tarFile{
		{
			{name: "bin/", typeflag: tar.TypeDir},
			{name: "usr/", typeflag: tar.TypeDir},
			{name: "usr/bin/", typeflag: tar.TypeDir},
			{name: "bin/busybox", typeflag: tar.TypeReg, content: []byte("#!busybox")},
			{name: "bin/sh", typeflag: tar.TypeSymlink, linkname: "/bin/busybox"},
			{name: "usr/bin/awk", typeflag: tar.TypeSymlink, linkname: "/bin/busybox"},
		},
	})
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := ExtractMerged(context.Background(), img, dest); err != nil {
		t.Fatalf("extract failed: %v", err)
	}

	cases := map[string]string{
		filepath.Join(dest, "bin/sh"):      "busybox",
		filepath.Join(dest, "usr/bin/awk"): "../../bin/busybox",
	}
	for link, wantTarget := range cases {
		gotTarget, err := os.Readlink(link)
		if err != nil {
			t.Fatalf("readlink %s: %v", link, err)
		}
		if gotTarget != wantTarget {
			t.Errorf("symlink %s: target=%q, want %q", link, gotTarget, wantTarget)
		}
		// Resolved symlink must stay inside dest. EvalSymlinks both sides
		// to neutralize per-OS path canonicalization (e.g. macOS /var → /private/var).
		resolved, err := filepath.EvalSymlinks(link)
		if err != nil {
			t.Fatalf("eval symlinks %s: %v", link, err)
		}
		want, err := filepath.EvalSymlinks(filepath.Join(dest, "bin/busybox"))
		if err != nil {
			t.Fatalf("eval want: %v", err)
		}
		if resolved != want {
			t.Errorf("symlink %s resolved to %q, want %q", link, resolved, want)
		}
	}
}

// Hardlinks must be rejected when their linkname is itself a symlink
// materialized earlier in the same extraction. The guarantee is conservative
// (we refuse the hardlink even when the symlink resolves to a path inside
// dest), because os.Link follows symlinks at the syscall layer - removing
// the guard would let a malicious tar plant a symlink and then a hardlink
// referencing it as a stepping stone to host paths.
//
// The scenario uses an in-dest target so that, without the guard, os.Link
// definitely succeeds and `pwn` appears on disk - making the regression
// catch deterministic across platforms (macOS / Linux link(2) both follow
// symlinks for the source path).
func TestExtractMerged_RejectsHardlinkThroughSymlinkTarget(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{
			{name: "data.txt", typeflag: tar.TypeReg, content: []byte("secret")},
			{name: "evil", typeflag: tar.TypeSymlink, linkname: "data.txt"},
			{name: "pwn", typeflag: tar.TypeLink, linkname: "evil"},
		},
	})
	dest := filepath.Join(t.TempDir(), "dest")
	_, err := ExtractMerged(context.Background(), img, dest)
	if err == nil {
		t.Fatalf("expected ExtractMerged to reject hardlink targeting a symlink, got nil")
	}
	if _, statErr := os.Lstat(filepath.Join(dest, "pwn")); !os.IsNotExist(statErr) {
		t.Fatalf("expected pwn to be absent on disk, got Lstat err=%v", statErr)
	}
}

// Root-level opaque marker (".wh..wh..opq" in "/") is a valid OCI directive
// meaning "lower layers contribute nothing here", so on disk it must clear
// every artifact materialized by previous layers. This test pins the OCI-
// correct behavior; treating root-opaque as a no-op would silently break
// image layouts whose upper layer truncates the rootfs.
func TestExtractMerged_RootOpaqueClearsLowerLayers(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{
			{name: "old.txt", typeflag: tar.TypeReg, content: []byte("from-lower")},
			{name: "lib/", typeflag: tar.TypeDir},
			{name: "lib/old.so", typeflag: tar.TypeReg, content: []byte("lib-lower")},
		},
		{
			{name: ".wh..wh..opq", typeflag: tar.TypeReg},
			{name: "new.txt", typeflag: tar.TypeReg, content: []byte("from-upper")},
		},
	})
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := ExtractMerged(context.Background(), img, dest); err != nil {
		t.Fatalf("extract failed: %v", err)
	}
	for _, gone := range []string{"old.txt", "lib/old.so", "lib"} {
		if _, err := os.Lstat(filepath.Join(dest, gone)); !os.IsNotExist(err) {
			t.Errorf("%s should have been cleared by root opaque marker, Lstat err=%v", gone, err)
		}
	}
	got, err := os.ReadFile(filepath.Join(dest, "new.txt"))
	if err != nil {
		t.Fatalf("read new.txt: %v", err)
	}
	if string(got) != "from-upper" {
		t.Errorf("new.txt content=%q, want %q", got, "from-upper")
	}
}

// Subdirectory opaque marker must clear only that subdirectory's lower-layer
// content, leaving sibling directories alone. Counterpart to
// TestMergedFS_OpaqueMarkerWipesDir, but on the disk-extract path.
func TestExtractMerged_SubdirOpaqueClearsOnlyThatDir(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{
			{name: "var/", typeflag: tar.TypeDir},
			{name: "var/log/", typeflag: tar.TypeDir},
			{name: "var/log/old.log", typeflag: tar.TypeReg, content: []byte("old")},
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/keep.conf", typeflag: tar.TypeReg, content: []byte("keep")},
		},
		{
			{name: "var/log/.wh..wh..opq", typeflag: tar.TypeReg},
			{name: "var/log/new.log", typeflag: tar.TypeReg, content: []byte("new")},
		},
	})
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := ExtractMerged(context.Background(), img, dest); err != nil {
		t.Fatalf("extract failed: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dest, "var/log/old.log")); !os.IsNotExist(err) {
		t.Errorf("var/log/old.log should be wiped by subdir opaque marker, Lstat err=%v", err)
	}
	if _, err := os.Lstat(filepath.Join(dest, "etc/keep.conf")); err != nil {
		t.Errorf("etc/keep.conf must survive sibling opaque, got Lstat err=%v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "var/log/new.log"))
	if err != nil {
		t.Fatalf("read var/log/new.log: %v", err)
	}
	if string(got) != "new" {
		t.Errorf("var/log/new.log content=%q, want %q", got, "new")
	}
}

// A whiteout entry whose name parses to an empty or "." target ("foo/.wh."
// or ".wh..") must not be treated as a directive to RemoveAll the parent
// directory. Pre-fix behavior: applyWhiteout(destAbs, ".") wiped dest
// before the rest of the layer was extracted.
func TestExtractMerged_DotWhiteoutDoesNotEraseDest(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{
			{name: "preserved.txt", typeflag: tar.TypeReg, content: []byte("survive")},
		},
		{
			{name: ".wh..", typeflag: tar.TypeReg},
		},
	})
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := ExtractMerged(context.Background(), img, dest); err != nil {
		t.Fatalf("extract failed: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "preserved.txt"))
	if err != nil {
		t.Fatalf("read preserved.txt: %v", err)
	}
	if string(got) != "survive" {
		t.Errorf("preserved.txt content=%q, want %q", got, "survive")
	}
}

// Upper layer replaces a lower-layer directory with a regular file of the
// same name. Pre-fix: os.OpenFile(O_CREATE|O_TRUNC) on a directory returned
// EISDIR and extract aborted.
func TestExtractMerged_UpperLayerFileReplacesDir(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/old.conf", typeflag: tar.TypeReg, content: []byte("legacy")},
		},
		{
			{name: "etc", typeflag: tar.TypeReg, content: []byte("now-a-file")},
		},
	})
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := ExtractMerged(context.Background(), img, dest); err != nil {
		t.Fatalf("extract failed: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "etc"))
	if err != nil {
		t.Fatalf("read /etc as file: %v", err)
	}
	if string(got) != "now-a-file" {
		t.Errorf("etc content=%q, want %q", got, "now-a-file")
	}
	fi, err := os.Lstat(filepath.Join(dest, "etc"))
	if err != nil {
		t.Fatalf("lstat etc: %v", err)
	}
	if !fi.Mode().IsRegular() {
		t.Errorf("etc must be a regular file after replacement, mode=%v", fi.Mode())
	}
}

// An upper layer is allowed to replace a lower-layer directory entry with
// a symlink (or hardlink) of the same name - this happens in real OCI
// images, e.g. when /tmp graduates from a directory to a tmpfs symlink in
// a sidecar layer. Pre-fix behavior: os.Remove on a non-empty directory
// returned ENOTEMPTY, the error was dropped, and os.Symlink then surfaced
// EEXIST. Verify the swap actually lands on disk.
func TestExtractMerged_UpperLayerSymlinkReplacesDir(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{
			{name: "etc/", typeflag: tar.TypeDir},
			{name: "etc/old.conf", typeflag: tar.TypeReg, content: []byte("legacy")},
			{name: "real/", typeflag: tar.TypeDir},
			{name: "real/passwd", typeflag: tar.TypeReg, content: []byte("root:x:0:0")},
		},
		{
			{name: "etc", typeflag: tar.TypeSymlink, linkname: "real"},
		},
	})
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := ExtractMerged(context.Background(), img, dest); err != nil {
		t.Fatalf("extract failed: %v", err)
	}

	target, err := os.Readlink(filepath.Join(dest, "etc"))
	if err != nil {
		t.Fatalf("etc must be a symlink, got Readlink err=%v", err)
	}
	if target != "real" {
		t.Errorf("etc -> %q, want %q", target, "real")
	}
	// Resolved through the symlink, /etc/passwd must serve the new content.
	got, err := os.ReadFile(filepath.Join(dest, "etc/passwd"))
	if err != nil {
		t.Fatalf("read /etc/passwd via symlink: %v", err)
	}
	if string(got) != "root:x:0:0" {
		t.Errorf("etc/passwd content via symlink = %q, want %q", got, "root:x:0:0")
	}
}

// A pre-cancelled context must abort ExtractMerged before any layer is
// materialized to disk, so a Ctrl-C from cobra propagates through to the
// extractor instead of running to completion on multi-GB images.
func TestExtractMerged_RespectsCancelledContext(t *testing.T) {
	img := makeImage(t, [][]tarFile{
		{{name: "data.txt", typeflag: tar.TypeReg, content: []byte("payload")}},
	})
	dest := filepath.Join(t.TempDir(), "dest")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ExtractMerged(ctx, img, dest)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	if _, statErr := os.Lstat(filepath.Join(dest, "data.txt")); !os.IsNotExist(statErr) {
		t.Fatalf("data.txt must not be materialized after cancel, Lstat err=%v", statErr)
	}
}

func TestExtractMerged_HardlinkHappyPath(t *testing.T) {
	want := []byte("payload")
	img := makeImage(t, [][]tarFile{
		{
			{name: "data.txt", typeflag: tar.TypeReg, content: want},
			{name: "alias.txt", typeflag: tar.TypeLink, linkname: "data.txt"},
		},
	})
	dest := filepath.Join(t.TempDir(), "dest")
	stats, err := ExtractMerged(context.Background(), img, dest)
	if err != nil {
		t.Fatalf("extract failed: %v", err)
	}
	if stats.Hardlinks != 1 {
		t.Errorf("stats.Hardlinks=%d, want 1", stats.Hardlinks)
	}
	got, err := os.ReadFile(filepath.Join(dest, "alias.txt"))
	if err != nil {
		t.Fatalf("read alias: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("alias content=%q, want %q", got, want)
	}
}

// ---- helpers ----

func pathsOf(entries []Entry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Path)
	}
	return out
}

func contains(ss []string, s string) bool {
	return slices.Contains(ss, s)
}

func TestApplyWhiteout_PropagatesUnreadableDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("chmod read restrictions don't apply to root")
	}
	base := t.TempDir()
	sub := filepath.Join(base, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub, "f"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write child: %v", err)
	}
	if err := os.Chmod(sub, 0); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(sub, 0o755) })

	err := applyWhiteout(base, "sub", true)
	if err == nil {
		t.Fatal("expected error from unreadable dir, got nil")
	}
	if !strings.Contains(err.Error(), "read whiteout dir") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyWhiteout_MissingDirIsFine(t *testing.T) {
	base := t.TempDir()
	if err := applyWhiteout(base, "does-not-exist", true); err != nil {
		t.Fatalf("expected nil for missing dir, got %v", err)
	}
	if err := applyWhiteout(base, "does-not-exist", false); err != nil {
		t.Fatalf("expected nil for missing single-file whiteout, got %v", err)
	}
}
