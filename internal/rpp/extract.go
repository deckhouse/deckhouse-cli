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

package rpp

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

// The proxy serves images as a gzipped tar of the (flattened) filesystem. The
// helpers below pull a single named file out of that stream by its base name, so
// callers do not duplicate gzip/tar plumbing.
//
// Safe against path traversal:
//   - destination is caller-supplied
//   - the archive name is used only for a base-name match
//   - non-regular entries (symlinks, dirs) are skipped

const (
	// ExecutableMode is the mode forced on extracted binaries so they are always
	// runnable regardless of the mode recorded in the image.
	ExecutableMode os.FileMode = 0o755

	// DefaultBinaryByteLimit caps an extracted binary. 512 MiB is far above the d8
	// binary (~130 MiB) and any plugin; shared so callers do not redefine it.
	DefaultBinaryByteLimit int64 = 512 << 20

	// maxArchiveBytes bounds the TOTAL decompressed bytes read while walking the
	// archive, so a decompression bomb placed in entries before the target cannot
	// exhaust resources. Generous (1 GiB); the per-entry caps are the tighter guard.
	maxArchiveBytes int64 = 1 << 30
)

// ExtractFileToPath finds the entry whose base name is entryName and writes it to
// destination with mode. The mode is forced (the archive's recorded mode is
// ignored) because these artifacts are executables that must be runnable. The
// copy is capped at maxBytes to guard against a decompression bomb. Returns
// ErrFileNotFound if no such entry exists.
func ExtractFileToPath(r io.Reader, entryName, destination string, mode os.FileMode, maxBytes int64) error {
	found, err := withTarGzEntry(r, entryName, func(tr *tar.Reader, _ *tar.Header) error {
		return writeCapped(destination, tr, mode, maxBytes)
	})
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("%w: %q", ErrFileNotFound, entryName)
	}

	return nil
}

// ReadFile returns the bytes of the entry whose base name is entryName, capped at
// maxBytes. found is false (with a nil error) when no such entry exists.
func ReadFile(r io.Reader, entryName string, maxBytes int64) ([]byte, bool, error) {
	var data []byte

	found, err := withTarGzEntry(r, entryName, func(tr *tar.Reader, _ *tar.Header) error {
		// +1 so an entry of exactly maxBytes is not mistaken for an overflow.
		bytes, err := io.ReadAll(io.LimitReader(tr, maxBytes+1))
		if err != nil {
			return fmt.Errorf("read %q: %w", entryName, err)
		}

		if int64(len(bytes)) > maxBytes {
			return fmt.Errorf("%q exceeds the %d-byte limit", entryName, maxBytes)
		}

		data = bytes

		return nil
	})

	return data, found, err
}

// withTarGzEntry walks the gzipped tar, and on the first regular file whose base
// name equals entryName invokes fn with the reader positioned at that entry. It
// reports whether the entry was found.
func withTarGzEntry(r io.Reader, entryName string, fn func(tr *tar.Reader, header *tar.Header) error) (bool, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return false, fmt.Errorf("open gzip stream: %w", err)
	}

	defer func() { _ = gz.Close() }()

	// Bound the total decompressed bytes across the whole walk (not just the
	// matched entry) so a bomb in a preceding entry cannot exhaust resources.
	reader := tar.NewReader(io.LimitReader(gz, maxArchiveBytes+1))

	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return false, nil
		}

		if err != nil {
			return false, fmt.Errorf("read tar: %w", err)
		}

		if header.Typeflag != tar.TypeReg || tarBaseName(header.Name) != entryName {
			continue
		}

		if err := fn(reader, header); err != nil {
			return true, err
		}

		return true, nil
	}
}

func writeCapped(destination string, r io.Reader, mode os.FileMode, maxBytes int64) error {
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("create %q: %w", destination, err)
	}

	defer func() { _ = out.Close() }()

	written, err := io.Copy(out, io.LimitReader(r, maxBytes+1))
	if err != nil {
		return fmt.Errorf("write %q: %w", destination, err)
	}

	if written > maxBytes {
		return fmt.Errorf("%q exceeds the %d-byte limit", destination, maxBytes)
	}

	// OpenFile honors the umask, so force the exact mode for executables.
	if err := os.Chmod(destination, mode); err != nil {
		return fmt.Errorf("chmod %q: %w", destination, err)
	}

	return nil
}

// tarBaseName normalizes a tar entry name to its base file name so that "plugin",
// "./plugin" and "dir/plugin" all match by their final segment.
func tarBaseName(name string) string {
	return path.Base(strings.TrimPrefix(name, "./"))
}
