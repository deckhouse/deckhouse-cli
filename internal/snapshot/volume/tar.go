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
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// TarEntry describes one entry to include in the output data.tar.
type TarEntry struct {
	// RelPath is the path relative to the volume root, using forward slashes.
	RelPath string
	// Type is one of "file", "dir", or "link".
	Type string
	// Mode is the Unix permission bits. Zero applies a sensible default
	// (0644 for files, 0755 for dirs, 0777 for links).
	Mode fs.FileMode
	// UID is the owner user ID; zero is used as-is.
	UID int
	// GID is the owner group ID; zero is used as-is.
	GID int
	// Mtime is the modification time; zero value writes epoch 0 as the mtime.
	Mtime time.Time
	// Linkname is the symlink target; only meaningful for "link" entries.
	Linkname string
}

// WriteTar writes a deterministic plain uncompressed PAX tar to outputPath.
// Entries are sorted by RelPath for determinism. Raw bytes for "file" entries
// are read from filepath.Join(stagingDir, filepath.FromSlash(entry.RelPath)).
// The output file is written atomically (.tmp → fsync → rename).
func WriteTar(outputPath string, stagingDir string, entries []TarEntry) error {
	sorted := slices.Clone(entries)
	slices.SortFunc(sorted, func(a, b TarEntry) int {
		return cmp.Compare(a.RelPath, b.RelPath)
	})

	aw, err := archive.NewAtomicWriter(outputPath)
	if err != nil {
		return fmt.Errorf("open tar output %s: %w", outputPath, err)
	}

	tw := tar.NewWriter(aw)

	if err := writeEntries(tw, stagingDir, sorted); err != nil {
		aw.Abort()

		return err
	}

	if err := tw.Close(); err != nil {
		aw.Abort()

		return fmt.Errorf("close tar writer: %w", err)
	}

	return aw.Commit()
}

func writeEntries(tw *tar.Writer, stagingDir string, entries []TarEntry) error {
	for _, e := range entries {
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

func writeFileEntry(tw *tar.Writer, stagingDir string, e TarEntry) error {
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
		Format:   tar.FormatPAX,
		Typeflag: tar.TypeReg,
		Name:     e.RelPath,
		Mode:     int64(mode.Perm()),
		Uid:      e.UID,
		Gid:      e.GID,
		ModTime:  e.Mtime,
		Size:     info.Size(),
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
		ModTime:  e.Mtime,
	}

	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar write dir header %s: %w", e.RelPath, err)
	}

	return nil
}

func writeLinkEntry(tw *tar.Writer, e TarEntry) error {
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
		ModTime:  e.Mtime,
	}

	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar write link header %s: %w", e.RelPath, err)
	}

	return nil
}
