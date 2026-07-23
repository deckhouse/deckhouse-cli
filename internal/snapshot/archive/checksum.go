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
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ErrChecksumMismatch is returned when the recomputed checksum differs from
// the value recorded in snapshot.yaml.
var ErrChecksumMismatch = errors.New("checksum mismatch")

// ErrSnapshotYAMLMissing is returned when snapshot.yaml does not exist in a node directory.
var ErrSnapshotYAMLMissing = errors.New("snapshot.yaml not found")

// ComputeNodeChecksum computes a deterministic SHA-256 digest over the node's own files.
//
// Covered files (in sorted-relpath order):
//   - manifests/*.yaml
//   - data.bin[.<ext>] (block volume, single-volume flat layout, if present)
//   - data.tar (filesystem volume, single-volume flat layout, if present)
//   - data/<pvc>.bin[.<ext>] / data/<pvc>.tar (multi-volume layout, if data/ present)
//
// Excluded: snapshot.yaml itself and the snapshots/ child directory.
//
// Each file contributes its relative path (null-terminated) followed by its
// raw content to an independent per-file SHA-256. All per-file digests are
// then fed in sorted order into a final SHA-256 to produce the node checksum.
func ComputeNodeChecksum(nodeDir string) (NodeChecksum, error) {
	paths, err := collectNodeFiles(nodeDir)
	if err != nil {
		return NodeChecksum{}, err
	}

	sort.Strings(paths)

	final := sha256.New()

	for _, relPath := range paths {
		absPath := filepath.Join(nodeDir, relPath)

		fh, err := computeFileHash(relPath, absPath)
		if err != nil {
			return NodeChecksum{}, fmt.Errorf("hash file %s: %w", relPath, err)
		}

		final.Write(fh)
	}

	sum := final.Sum(nil)
	hexStr := fmt.Sprintf("%x", sum)

	return NodeChecksum{
		Algorithm: ChecksumAlgorithmSHA256,
		Hex:       hexStr,
		Short:     ShortChecksum(hexStr),
	}, nil
}

// ShortChecksum returns the first 8 hex characters of hex.
// The short form is used as a suffix when a node directory name already exists
// with a different checksum, preventing silent data overwrite.
func ShortChecksum(hex string) string {
	if len(hex) >= 8 {
		return hex[:8]
	}

	return hex
}

// VerifyNode recomputes the checksum for nodeDir and compares it with the value
// stored in snapshot.yaml. Returns ErrSnapshotYAMLMissing if snapshot.yaml is absent,
// ErrChecksumMismatch if the digests differ.
func VerifyNode(nodeDir string) error {
	sy, err := ReadSnapshotYAML(nodeDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%s: %w", nodeDir, ErrSnapshotYAMLMissing)
		}

		return err
	}

	got, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		return err
	}

	if got.Hex != sy.Checksum.Hex {
		return fmt.Errorf("node %s: stored %q computed %q: %w",
			nodeDir, sy.Checksum.Hex, got.Hex, ErrChecksumMismatch)
	}

	return nil
}

// ValidateNodeMetadata reads nodeDir's snapshot.yaml and strictly validates its metadata via
// ValidateSnapshotYAML, deriving the node's data-payload flags from the directory itself
// (ClassifyBlockPayload for data.bin[.<ext>], os.Stat for data.tar). It complements VerifyNode:
// VerifyNode checks the integrity digest over the node's files, while snapshot.yaml — excluded
// from that digest — is validated here. Returns ErrSnapshotYAMLMissing when snapshot.yaml is
// absent, and propagates ClassifyBlockPayload's ErrInvalidBlockPayload for a malformed payload.
func ValidateNodeMetadata(nodeDir string) error {
	sy, err := ReadSnapshotYAML(nodeDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%s: %w", nodeDir, ErrSnapshotYAMLMissing)
		}

		return err
	}

	_, hasBlock, err := ClassifyBlockPayload(nodeDir)
	if err != nil {
		return fmt.Errorf("%s: %w", nodeDir, err)
	}

	hasFS := false

	if _, statErr := os.Stat(filepath.Join(nodeDir, FsTarName)); statErr == nil {
		hasFS = true
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("stat %s in %s: %w", FsTarName, nodeDir, statErr)
	}

	if err := ValidateSnapshotYAML(sy, hasBlock, hasFS); err != nil {
		return fmt.Errorf("%s: %w", nodeDir, err)
	}

	return nil
}

// collectNodeFiles returns the relative paths of all files in nodeDir that
// contribute to the node checksum. The returned paths are not sorted; callers
// must sort them before computing the digest.
func collectNodeFiles(nodeDir string) ([]string, error) {
	var paths []string

	manifestsDir := filepath.Join(nodeDir, ManifestsDirName)
	entries, err := os.ReadDir(manifestsDir)

	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read %s: %w", ManifestsDirName, err)
	}

	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".yaml") {
			paths = append(paths, filepath.Join(ManifestsDirName, e.Name()))
		}
	}

	blockPayload, blockFound, findErr := ClassifyBlockPayload(nodeDir)
	if findErr != nil {
		return nil, fmt.Errorf("classify block payload in %s: %w", nodeDir, findErr)
	}

	if blockFound {
		rel, relErr := filepath.Rel(nodeDir, blockPayload.Path)
		if relErr != nil {
			return nil, relErr
		}

		paths = append(paths, rel)
	}

	// Single-volume filesystem tar (data.tar).
	if _, statErr := os.Stat(filepath.Join(nodeDir, FsTarName)); statErr == nil {
		paths = append(paths, FsTarName)
	}

	dataDir := filepath.Join(nodeDir, DataDirName)
	info, err := os.Stat(dataDir)

	if err == nil && info.IsDir() {
		walkErr := filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				// Skip staging directories (names ending in ".d"):
				// data/<pvc>.bin.d/ (block chunks) and data/<pvc>.tar.d/ (FS raw files).
				if strings.HasSuffix(d.Name(), ".d") {
					return filepath.SkipDir
				}

				return nil
			}

			rel, relErr := filepath.Rel(nodeDir, path)
			if relErr != nil {
				return relErr
			}

			paths = append(paths, rel)

			return nil
		})
		if walkErr != nil {
			return nil, fmt.Errorf("walk %s: %w", DataDirName, walkErr)
		}
	}

	return paths, nil
}

// computeFileHash computes a SHA-256 digest over relPath (null-terminated) followed
// by the raw content of absPath.  Using a per-file hash before folding into the
// final digest prevents length-extension and path/content confusion.
func computeFileHash(relPath, absPath string) ([]byte, error) {
	h := sha256.New()
	h.Write([]byte(relPath))
	h.Write([]byte{0})

	f, err := os.Open(absPath)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", absPath, err)
	}

	defer func() { _ = f.Close() }()

	if _, err := io.Copy(h, f); err != nil {
		return nil, fmt.Errorf("read %s: %w", absPath, err)
	}

	return h.Sum(nil), nil
}
