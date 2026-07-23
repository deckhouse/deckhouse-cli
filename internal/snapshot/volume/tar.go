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

package volume

import (
	"archive/tar"
	"cmp"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// TarEntry describes one entry to include in the output data.tar.
type TarEntry struct {
	// RelPath is the stored path relative to the volume root, using forward
	// slashes. For files it is OriginalPath plus the codec extension.
	RelPath string
	// Type is one of "file", "dir", or "link".
	Type string
	// Codec identifies the per-file codec. It is required for file entries.
	Codec string
	// OriginalPath is the source path before the codec extension is appended.
	// It is required for file entries.
	OriginalPath string
	// RawSize is the exact plaintext byte count before compression.
	RawSize int64
	// Mode is the Unix permission bits. Zero applies a sensible default
	// (0644 for files, 0755 for dirs, 0777 for links).
	Mode fs.FileMode
	// UID is the owner user ID; zero is used as-is.
	UID int
	// GID is the owner group ID; zero is used as-is.
	GID int
	// Mtime is the modification time. A zero value is normalized to Unix epoch 0
	// (time.Unix(0,0).UTC()) before writing the tar header, so the output is
	// deterministic and does not depend on how archive/tar handles time.Time{}.
	Mtime time.Time
	// Linkname is the symlink target; only meaningful for "link" entries.
	Linkname string
}

// WriteTar writes a deterministic plain uncompressed PAX tar to outputPath.
// Entries are sorted by RelPath for determinism. Raw bytes for "file" entries
// are read from filepath.Join(stagingDir, filepath.FromSlash(entry.RelPath)).
// The output file is written atomically (.tmp → fsync → rename).
//
// ctx is checked once per entry during assembly; if it is cancelled mid-write,
// the in-progress AtomicWriter is aborted (so no partial file is ever visible
// at outputPath) and a wrapped ctx.Err() is returned (checkable via
// errors.Is). Cancellation here never loses data: the per-file staging
// directory is untouched, so tar assembly simply resumes from the same staged
// files on the next run.
func WriteTar(ctx context.Context, outputPath string, stagingDir string, entries []TarEntry) error {
	sorted := slices.Clone(entries)
	slices.SortFunc(sorted, func(a, b TarEntry) int {
		return cmp.Compare(a.RelPath, b.RelPath)
	})

	aw, err := archive.NewAtomicWriter(outputPath)
	if err != nil {
		return fmt.Errorf("open tar output %s: %w", outputPath, err)
	}

	tw := tar.NewWriter(aw)

	if err := writeEntries(ctx, tw, stagingDir, sorted); err != nil {
		aw.Abort()

		return err
	}

	if err := tw.Close(); err != nil {
		aw.Abort()

		return fmt.Errorf("close tar writer: %w", err)
	}

	return aw.Commit()
}

func writeEntries(ctx context.Context, tw *tar.Writer, stagingDir string, entries []TarEntry) error {
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("tar assembly cancelled before entry %q: %w", e.RelPath, err)
		}

		if err := writeEntry(tw, stagingDir, e); err != nil {
			return err
		}
	}

	return nil
}

func writeEntry(tw *tar.Writer, stagingDir string, e TarEntry) error {
	switch e.Type {
	case "file":
		return writeFileEntry(tw, stagingDir, e)
	case "dir":
		return writeDirEntry(tw, e)
	case "link":
		return writeLinkEntry(tw, e)
	default:
		// Unknown entry types are silently skipped for forward compatibility.
		return nil
	}
}

// normalizeMtime returns t unchanged when non-zero, and time.Unix(0,0).UTC()
// when t is the zero value. This ensures the PAX mtime extension is always
// written with a defined value rather than relying on archive/tar's handling
// of time.Time{} (year 0001).
func normalizeMtime(t time.Time) time.Time {
	if t.IsZero() {
		return time.Unix(0, 0).UTC()
	}

	return t
}

func writeFileEntry(tw *tar.Writer, stagingDir string, e TarEntry) error {
	metadata, err := archive.NewFSMetadata(e.Codec, e.OriginalPath, e.RawSize)
	if err != nil {
		return fmt.Errorf("validate metadata for tar entry %s: %w", e.RelPath, err)
	}

	storedPath, err := metadata.StoredPath()
	if err != nil {
		return fmt.Errorf("derive stored path for tar entry %s: %w", e.RelPath, err)
	}

	if e.RelPath != storedPath {
		return fmt.Errorf("tar entry path %q does not match metadata path %q: %w",
			e.RelPath, storedPath, archive.ErrInvalidFSMetadata)
	}

	mode := e.Mode
	if mode == 0 {
		mode = 0o644
	}

	srcPath := filepath.Join(stagingDir, filepath.FromSlash(e.RelPath))

	f, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open staging file %s: %w", srcPath, err)
	}

	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat staging file %s: %w", srcPath, err)
	}

	hdr := &tar.Header{
		Format:     tar.FormatPAX,
		Typeflag:   tar.TypeReg,
		Name:       e.RelPath,
		Mode:       int64(mode.Perm()),
		Uid:        e.UID,
		Gid:        e.GID,
		ModTime:    normalizeMtime(e.Mtime),
		Size:       info.Size(),
		PAXRecords: metadata.PAXRecords(),
	}

	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar write header %s: %w", e.RelPath, err)
	}

	if _, err := io.Copy(tw, f); err != nil {
		return fmt.Errorf("copy staging file %s: %w", e.RelPath, err)
	}

	return nil
}

func writeDirEntry(tw *tar.Writer, e TarEntry) error {
	mode := e.Mode
	if mode == 0 {
		mode = 0o755
	}

	name := e.RelPath
	if len(name) > 0 && name[len(name)-1] != '/' {
		name += "/"
	}

	hdr := &tar.Header{
		Format:   tar.FormatPAX,
		Typeflag: tar.TypeDir,
		Name:     name,
		Mode:     int64(mode.Perm()),
		Uid:      e.UID,
		Gid:      e.GID,
		ModTime:  normalizeMtime(e.Mtime),
	}

	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar write dir header %s: %w", e.RelPath, err)
	}

	return nil
}

// validateSymlinkTarget rejects a symlink target that is absolute, contains an OS
// separator other than "/", carries a NUL/control byte, or that — once resolved
// relative to entryRelPath's own directory — would climb above the volume root.
// WriteTar itself never follows symlinks (entries are only ever written as tar
// headers), but a later `tar -x`/restore step does, so a target that escapes here
// becomes a real path-traversal vector at extraction time. An in-root relative target
// (a sibling file, or one that dips below and back above a subdirectory without net
// escaping the root) is left unchanged.
func validateSymlinkTarget(entryRelPath, target string) error {
	if target == "" {
		return fmt.Errorf("%w: empty symlink target", ErrUnsafePath)
	}

	for _, r := range target {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: control byte in symlink target %q", ErrUnsafePath, target)
		}
	}

	if strings.ContainsRune(target, '\\') {
		return fmt.Errorf("%w: OS separator in symlink target %q", ErrUnsafePath, target)
	}

	if strings.HasPrefix(target, "/") {
		return fmt.Errorf("%w: absolute symlink target %q", ErrUnsafePath, target)
	}

	// dir is entryRelPath's own directory (with a trailing "/", or "" for a
	// root-level entry), so target is resolved exactly as a symlink at that
	// location would resolve it: relative to its own containing directory,
	// not to the volume root.
	dir := entryRelPath[:strings.LastIndex(entryRelPath, "/")+1]

	depth := 0

	for _, seg := range strings.Split(dir+target, "/") {
		switch seg {
		case "", ".":
			continue
		case "..":
			if depth == 0 {
				return fmt.Errorf("%w: symlink target %q for %q escapes the volume root", ErrUnsafePath, target, entryRelPath)
			}

			depth--
		default:
			depth++
		}
	}

	return nil
}

func writeLinkEntry(tw *tar.Writer, e TarEntry) error {
	if err := validateSymlinkTarget(e.RelPath, e.Linkname); err != nil {
		return fmt.Errorf("write link entry %s: %w", e.RelPath, err)
	}

	mode := e.Mode
	if mode == 0 {
		mode = 0o777
	}

	hdr := &tar.Header{
		Format:   tar.FormatPAX,
		Typeflag: tar.TypeSymlink,
		Name:     e.RelPath,
		Linkname: e.Linkname,
		Mode:     int64(mode.Perm()),
		Uid:      e.UID,
		Gid:      e.GID,
		ModTime:  normalizeMtime(e.Mtime),
	}

	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar write link header %s: %w", e.RelPath, err)
	}

	return nil
}
