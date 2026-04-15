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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// ExtractStats summarizes how many entries were materialized.
type ExtractStats struct {
	Files     int
	Dirs      int
	Symlinks  int
	Hardlinks int
	TotalSize int64
}

// ExtractMerged writes the merged filesystem of img into destDir, honoring
// whiteouts top-down. Paths attempting to escape destDir are rejected.
//
// Cancellation is checked between layers and at every tar entry, so a
// Ctrl-C from the cobra command interrupts a multi-GB extraction within
// the next entry boundary instead of running to completion.
func ExtractMerged(ctx context.Context, img v1.Image, destDir string) (ExtractStats, error) {
	layers, err := img.Layers()
	if err != nil {
		return ExtractStats{}, fmt.Errorf("get layers: %w", err)
	}
	absDest, err := filepath.Abs(destDir)
	if err != nil {
		return ExtractStats{}, fmt.Errorf("resolve dest: %w", err)
	}
	if err := os.MkdirAll(absDest, 0o755); err != nil {
		return ExtractStats{}, fmt.Errorf("create dest: %w", err)
	}

	var stats ExtractStats
	for i, layer := range layers {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		rc, err := layer.Uncompressed()
		if err != nil {
			return stats, fmt.Errorf("layer %d: uncompress: %w", i+1, err)
		}
		err = extractTarTo(ctx, rc, absDest, &stats)
		_ = rc.Close()
		if err != nil {
			return stats, fmt.Errorf("layer %d: %w", i+1, err)
		}
	}
	return stats, nil
}

func extractTarTo(ctx context.Context, rc io.Reader, destAbs string, stats *ExtractStats) error {
	return WalkTar(rc, func(hdr *tar.Header, r io.Reader) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := normalizePath(hdr.Name)
		if name == "." {
			return nil
		}

		if target, opaque := Whiteout(name); target != "" || opaque {
			return applyWhiteout(destAbs, target, opaque)
		}

		absPath, err := safeJoin(destAbs, name)
		if err != nil {
			return err
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := ensureNoSymlinkComponents(destAbs, absPath, true); err != nil {
				return err
			}
			if err := os.MkdirAll(absPath, fs.FileMode(hdr.Mode&0o777)|0o700); err != nil {
				return fmt.Errorf("mkdir %s: %w", absPath, err)
			}
			stats.Dirs++

		case tar.TypeReg:
			if err := ensureNoSymlinkComponents(destAbs, absPath, true); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
				return fmt.Errorf("mkdir parent %s: %w", absPath, err)
			}
			// If a lower layer left a directory at absPath, OpenFile would
			// return EISDIR. Symlink replacements are already rejected by
			// ensureNoSymlinkComponents above; clearStalePath finishes the
			// "upper layer replaces an entry of a different kind" matrix.
			if err := clearStalePath(absPath); err != nil {
				return err
			}
			f, err := os.OpenFile(absPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode(hdr.Mode&0o777))
			if err != nil {
				return fmt.Errorf("create %s: %w", absPath, err)
			}
			n, err := io.Copy(f, r)
			closeErr := f.Close()
			if err != nil {
				return fmt.Errorf("write %s: %w", absPath, err)
			}
			if closeErr != nil {
				return fmt.Errorf("close %s: %w", absPath, closeErr)
			}
			stats.Files++
			stats.TotalSize += n

		case tar.TypeSymlink:
			if err := ensureNoSymlinkComponents(destAbs, absPath, true); err != nil {
				return err
			}
			linkname, err := resolveSymlinkTarget(destAbs, absPath, hdr.Linkname)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
				return fmt.Errorf("mkdir parent %s: %w", absPath, err)
			}
			if err := clearStalePath(absPath); err != nil {
				return err
			}
			if err := os.Symlink(linkname, absPath); err != nil {
				return fmt.Errorf("symlink %s -> %s: %w", absPath, linkname, err)
			}
			stats.Symlinks++

		case tar.TypeLink:
			if err := ensureNoSymlinkComponents(destAbs, absPath, true); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
				return fmt.Errorf("mkdir parent %s: %w", absPath, err)
			}
			linkTarget, err := safeJoin(destAbs, normalizePath(hdr.Linkname))
			if err != nil {
				return err
			}
			if err := ensureNoSymlinkComponents(destAbs, linkTarget, false); err != nil {
				return err
			}
			if err := clearStalePath(absPath); err != nil {
				return err
			}
			if err := os.Link(linkTarget, absPath); err != nil {
				return fmt.Errorf("hardlink %s -> %s: %w", absPath, linkTarget, err)
			}
			stats.Hardlinks++
		}
		return nil
	})
}

func applyWhiteout(destAbs, target string, opaque bool) error {
	t := normalizePath(target)
	if opaque && t == "." {
		return clearDirContents(destAbs)
	}
	absPath, err := safeJoin(destAbs, t)
	if err != nil {
		return err
	}
	if err := ensureNoSymlinkComponents(destAbs, absPath, false); err != nil {
		return err
	}
	if opaque {
		return clearDirContents(absPath)
	}
	_ = os.RemoveAll(absPath)
	return nil
}

// clearStalePath removes path if it already exists, picking RemoveAll for
// directories and Remove for everything else (regular files, symlinks,
// devices). Without this, `cr fs extract` on a layered image where an upper
// layer replaces a lower-layer directory with a symlink/hardlink fails:
// os.Remove on a non-empty directory returns ENOTEMPTY, the error is then
// dropped, and the subsequent os.Symlink/os.Link surfaces a confusing
// EEXIST. Returning the error from the cleanup itself keeps the failure
// site honest.
func clearStalePath(path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("lstat %s: %w", path, err)
	}
	// Lstat on a symlink does NOT set IsDir even when the target is a
	// directory, so this branch picks RemoveAll only for real directories.
	if fi.IsDir() {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("remove stale dir %s: %w", path, err)
		}
		return nil
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale entry %s: %w", path, err)
	}
	return nil
}

// clearDirContents removes every direct entry of dir. A missing dir is fine
// (nothing to clear); any other ReadDir error is propagated so we never
// silently leave stale data on disk. Per-entry RemoveAll failures are
// aggregated and returned together - dropping them would defeat the
// whole-dir-cleared invariant promised by an opaque whiteout marker.
func clearDirContents(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read whiteout dir %s: %w", dir, err)
	}
	var errs []error
	for _, e := range entries {
		entryPath := filepath.Join(dir, e.Name())
		if rerr := os.RemoveAll(entryPath); rerr != nil {
			errs = append(errs, fmt.Errorf("remove %s: %w", entryPath, rerr))
		}
	}
	return errors.Join(errs...)
}

func safeJoin(base, rel string) (string, error) {
	if slices.Contains(strings.Split(filepath.ToSlash(rel), "/"), "..") {
		return "", fmt.Errorf("unsafe path (contains ..): %q", rel)
	}
	joined := filepath.Clean(filepath.Join(base, filepath.FromSlash(rel)))
	if !withinBase(base, joined) {
		return "", fmt.Errorf("unsafe path (escapes destination): %q", rel)
	}
	return joined, nil
}

// withinBase reports whether path is at or below base (both expected to be
// absolute). The last guard against path-traversal: filepath.Rel produces a
// "../"-prefixed result iff path escapes base.
func withinBase(base, path string) bool {
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return false
	}
	return rel == "." || (!strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "..")
}

func ensureNoSymlinkComponents(base, absPath string, allowFinalMissing bool) error {
	if !withinBase(base, absPath) {
		return fmt.Errorf("unsafe path (escapes destination): %q", absPath)
	}
	rel, err := filepath.Rel(base, absPath)
	if err != nil {
		return fmt.Errorf("resolve relative path: %w", err)
	}
	if rel == "." {
		return nil
	}
	cur := base
	parts := strings.Split(rel, string(filepath.Separator))
	for i, part := range parts {
		cur = filepath.Join(cur, part)
		info, err := os.Lstat(cur)
		if err != nil {
			if os.IsNotExist(err) {
				if allowFinalMissing && i == len(parts)-1 {
					return nil
				}
				continue
			}
			return fmt.Errorf("lstat %s: %w", cur, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe path (symlink component): %q", cur)
		}
	}
	return nil
}

// resolveSymlinkTarget validates the symlink target against destAbs and
// returns the linkname to actually create on disk.
//
// Absolute targets (typical in Alpine/busybox: /bin/sh -> /bin/busybox) are
// re-rooted at destAbs and rewritten to a relative path so the extracted tree
// stays self-contained and never points outside destAbs at follow time.
// Relative targets that would escape destAbs are still rejected.
func resolveSymlinkTarget(destAbs, linkPathAbs, target string) (string, error) {
	if target == "" {
		return "", fmt.Errorf("unsafe symlink target (empty)")
	}
	if err := ensureNoSymlinkComponents(destAbs, filepath.Dir(linkPathAbs), false); err != nil {
		return "", err
	}

	if filepath.IsAbs(target) {
		// Re-root absolute target inside destAbs and rewrite to a relative
		// path from the symlink's directory. filepath.Clean(filepath.Join(base, "/x"))
		// collapses to base+"/x" - documented Join semantics for absolute rhs.
		rooted := filepath.Clean(filepath.Join(destAbs, target))
		if !withinBase(destAbs, rooted) {
			return "", fmt.Errorf("unsafe symlink target (escapes destination): %q", target)
		}
		rel, err := filepath.Rel(filepath.Dir(linkPathAbs), rooted)
		if err != nil {
			return "", fmt.Errorf("resolve symlink target %q: %w", target, err)
		}
		return rel, nil
	}

	resolved := filepath.Clean(filepath.Join(filepath.Dir(linkPathAbs), target))
	if !withinBase(destAbs, resolved) {
		return "", fmt.Errorf("unsafe symlink target (escapes destination): %q", target)
	}
	return target, nil
}
