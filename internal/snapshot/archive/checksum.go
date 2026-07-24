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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	sigsyaml "sigs.k8s.io/yaml"
)

// ErrChecksumMismatch is returned when the recomputed checksum differs from
// the value recorded in snapshot.yaml.
var ErrChecksumMismatch = errors.New("checksum mismatch")

// ErrSnapshotYAMLMissing is returned when snapshot.yaml does not exist in a node directory.
var ErrSnapshotYAMLMissing = errors.New("snapshot.yaml not found")

// ErrVerifiedArchiveChanged is returned when a file or directory differs from the exact
// identity and content captured by archive verification.
var ErrVerifiedArchiveChanged = errors.New("verified archive view changed")

// VerifiedArchive retains one rooted archive descriptor for the upload lifetime. Individual
// payload handles are opened only for active workers, keeping descriptor use bounded.
type VerifiedArchive struct {
	root *RootedSource
	path string
}

// VerifiedNode is the immutable verification result for one archive node.
type VerifiedNode struct {
	checksum       NodeChecksum
	snapshotDigest [sha256.Size]byte
	snapshotInfo   os.FileInfo
	files          map[string]*VerifiedFile
}

// VerifiedFile records the identity and content of one checksum-covered archive file.
type VerifiedFile struct {
	archive     *VerifiedArchive
	archivePath string
	digest      [sha256.Size]byte
	info        os.FileInfo
}

// VerifiedHandle is one exact regular-file descriptor whose identity and bytes were verified.
// The handle must be closed before its VerifiedArchive.
type VerifiedHandle struct {
	file     *os.File
	expected *VerifiedFile
}

// OpenVerifiedArchive pins root for planning, verification, upload, and final readiness.
func OpenVerifiedArchive(root string) (*VerifiedArchive, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve archive root %s: %w", root, err)
	}

	source, err := OpenRootedSource(absolute)
	if err != nil {
		return nil, err
	}

	return &VerifiedArchive{root: source, path: absolute}, nil
}

// Close releases the pinned archive root. All VerifiedHandles must already be closed.
func (a *VerifiedArchive) Close() error {
	return a.root.Close()
}

// RootSource returns the pinned source used to build the plan. The caller must not close it.
func (a *VerifiedArchive) RootSource() *RootedSource {
	return a.root
}

// VerifyNode captures a node's checksum-covered file identities and bytes from the pinned root.
func (a *VerifiedArchive) VerifyNode(ctx context.Context, nodeDir string) (*VerifiedNode, error) {
	source, closeNode, err := a.openNode(nodeDir)
	if err != nil {
		return nil, err
	}

	defer closeNode()

	snapshotFile, err := source.OpenRegularFile(SnapshotYAMLName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%s: %w", nodeDir, ErrSnapshotYAMLMissing)
		}

		return nil, err
	}

	snapshotInfoBefore, err := snapshotFile.Stat()
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("inspect %s: %w", filepath.Join(nodeDir, SnapshotYAMLName), err),
			wrapArchiveCloseError(filepath.Join(nodeDir, SnapshotYAMLName), snapshotFile.Close()),
		)
	}

	snapshotData, snapshotDigest, snapshotErr := readAndHashContext(ctx, snapshotFile)
	snapshotInfoAfter, snapshotStatErr := snapshotFile.Stat()
	closeSnapshotErr := snapshotFile.Close()

	if snapshotErr != nil || snapshotStatErr != nil || closeSnapshotErr != nil {
		return nil, errors.Join(
			wrapArchiveReadError(filepath.Join(nodeDir, SnapshotYAMLName), snapshotErr),
			wrapArchiveReadError(filepath.Join(nodeDir, SnapshotYAMLName), snapshotStatErr),
			wrapArchiveCloseError(filepath.Join(nodeDir, SnapshotYAMLName), closeSnapshotErr),
		)
	}

	if !sameVerifiedInfo(snapshotInfoBefore, snapshotInfoAfter) {
		return nil, fmt.Errorf("%s changed while being verified: %w",
			filepath.Join(nodeDir, SnapshotYAMLName), ErrVerifiedArchiveChanged)
	}

	var metadata SnapshotYAML
	if err := sigsyaml.Unmarshal(snapshotData, &metadata); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", filepath.Join(nodeDir, SnapshotYAMLName), err)
	}

	paths, err := collectNodeFiles(source)
	if err != nil {
		return nil, err
	}

	sort.Strings(paths)

	files := make(map[string]*VerifiedFile, len(paths))
	final := sha256.New()

	for _, relPath := range paths {
		file, nodeDigest, err := a.captureFile(ctx, source, nodeDir, relPath)
		if err != nil {
			return nil, fmt.Errorf("verify file %s: %w", relPath, err)
		}

		files[relPath] = file
		_, _ = final.Write(nodeDigest)
	}

	sum := final.Sum(nil)
	hexString := fmt.Sprintf("%x", sum)
	checksum := NodeChecksum{
		Algorithm: ChecksumAlgorithmSHA256,
		Hex:       hexString,
		Short:     ShortChecksum(hexString),
	}

	if checksum.Hex != metadata.Checksum.Hex {
		return nil, fmt.Errorf("node %s: stored %q computed %q: %w",
			nodeDir, metadata.Checksum.Hex, checksum.Hex, ErrChecksumMismatch)
	}

	hasBlock := false
	hasFS := false

	for relPath := range files {
		base := filepath.Base(relPath)
		hasBlock = hasBlock || strings.HasPrefix(base, DataBlockBase)
		hasFS = hasFS || relPath == FsTarName
	}

	if err := ValidateSnapshotYAML(metadata, hasBlock, hasFS); err != nil {
		return nil, fmt.Errorf("%s: %w", nodeDir, err)
	}

	return &VerifiedNode{
		checksum:       checksum,
		snapshotDigest: snapshotDigest,
		snapshotInfo:   snapshotInfoBefore,
		files:          files,
	}, nil
}

// Checksum returns the recomputed node checksum.
func (n *VerifiedNode) Checksum() NodeChecksum {
	return n.checksum
}

// SnapshotDigestMatches reports whether digest identifies the verified snapshot.yaml bytes.
func (n *VerifiedNode) SnapshotDigestMatches(digest [sha256.Size]byte) bool {
	return digest == n.snapshotDigest
}

// SnapshotIdentityMatches reports whether info is the same snapshot.yaml identity verified.
func (n *VerifiedNode) SnapshotIdentityMatches(info os.FileInfo) bool {
	return sameVerifiedInfo(n.snapshotInfo, info)
}

// File returns a verified checksum-covered file by node-relative path.
func (n *VerifiedNode) File(relPath string) (*VerifiedFile, bool) {
	file, ok := n.files[filepath.Clean(relPath)]

	return file, ok
}

// DigestMatches reports whether digest identifies the verified file bytes.
func (f *VerifiedFile) DigestMatches(digest [sha256.Size]byte) bool {
	return digest == f.digest
}

// IdentityMatches reports whether info is the same archive file identity verified.
func (f *VerifiedFile) IdentityMatches(info os.FileInfo) bool {
	return sameVerifiedInfo(f.info, info)
}

// OpenVerifiedFile opens, verifies, and rewinds the exact descriptor later consumed by upload.
func (a *VerifiedArchive) OpenVerifiedFile(ctx context.Context, expected *VerifiedFile) (*VerifiedHandle, error) {
	if expected == nil || expected.archive != a {
		return nil, fmt.Errorf("verified file does not belong to this archive: %w", ErrVerifiedArchiveChanged)
	}

	file, err := a.root.OpenRegularPath(expected.archivePath)
	if err != nil {
		return nil, err
	}

	handle := &VerifiedHandle{file: file, expected: expected}
	if err := handle.Verify(ctx); err != nil {
		return nil, errors.Join(err, wrapArchiveCloseError(expected.archivePath, file.Close()))
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, errors.Join(
			fmt.Errorf("rewind verified archive file %s: %w", expected.archivePath, err),
			wrapArchiveCloseError(expected.archivePath, file.Close()),
		)
	}

	return handle, nil
}

// Read implements io.Reader on the pinned descriptor.
func (h *VerifiedHandle) Read(p []byte) (int, error) {
	return h.file.Read(p)
}

// ReadAt implements io.ReaderAt on the pinned descriptor.
func (h *VerifiedHandle) ReadAt(p []byte, offset int64) (int, error) {
	return h.file.ReadAt(p, offset)
}

// Seek implements io.Seeker on the pinned descriptor.
func (h *VerifiedHandle) Seek(offset int64, whence int) (int64, error) {
	return h.file.Seek(offset, whence)
}

// Stat returns metadata for the pinned descriptor.
func (h *VerifiedHandle) Stat() (os.FileInfo, error) {
	return h.file.Stat()
}

// Verify proves both the pinned descriptor bytes and the current rooted namespace entry still
// match verification. It preserves the descriptor offset.
func (h *VerifiedHandle) Verify(ctx context.Context) error {
	info, err := h.file.Stat()
	if err != nil {
		return fmt.Errorf("inspect pinned archive file %s: %w", h.expected.archivePath, err)
	}

	if !sameVerifiedInfo(h.expected.info, info) {
		return fmt.Errorf("%s identity or metadata changed: %w",
			h.expected.archivePath, ErrVerifiedArchiveChanged)
	}

	digest, err := hashReaderAtContext(ctx, h.file, info.Size())
	if err != nil {
		return fmt.Errorf("rehash pinned archive file %s: %w", h.expected.archivePath, err)
	}

	if digest != h.expected.digest {
		return fmt.Errorf("%s content changed: %w", h.expected.archivePath, ErrVerifiedArchiveChanged)
	}

	current, err := h.expected.archive.root.OpenRegularPath(h.expected.archivePath)
	if err != nil {
		return fmt.Errorf("reopen current archive entry %s: %w", h.expected.archivePath, err)
	}

	currentInfo, statErr := current.Stat()
	closeErr := current.Close()

	if statErr != nil || closeErr != nil {
		return errors.Join(
			wrapArchiveReadError(h.expected.archivePath, statErr),
			wrapArchiveCloseError(h.expected.archivePath, closeErr),
		)
	}

	if !sameVerifiedInfo(h.expected.info, currentInfo) {
		return fmt.Errorf("%s was replaced after verification: %w",
			h.expected.archivePath, ErrVerifiedArchiveChanged)
	}

	return nil
}

// Close releases the pinned payload descriptor.
func (h *VerifiedHandle) Close() error {
	return h.file.Close()
}

func (a *VerifiedArchive) openNode(nodeDir string) (*RootedSource, func(), error) {
	relative, err := filepath.Rel(a.path, nodeDir)
	if err != nil {
		return nil, func() {}, fmt.Errorf("resolve node %s beneath archive root: %w", nodeDir, err)
	}

	relative = filepath.Clean(relative)
	if !filepath.IsLocal(relative) {
		return nil, func() {}, fmt.Errorf("node %s is outside archive root %s: %w",
			nodeDir, a.path, ErrNonRegularArchiveArtifact)
	}

	if relative == "." {
		return a.root, func() {}, nil
	}

	components := strings.Split(relative, string(filepath.Separator))
	opened := make([]*RootedSource, 0, len(components))
	current := a.root

	for _, component := range components {
		child, openErr := current.OpenDirectory(component)
		if openErr != nil {
			for index := len(opened) - 1; index >= 0; index-- {
				_ = opened[index].Close()
			}

			return nil, func() {}, openErr
		}

		opened = append(opened, child)
		current = child
	}

	closeNode := func() {
		for index := len(opened) - 1; index >= 0; index-- {
			_ = opened[index].Close()
		}
	}

	return current, closeNode, nil
}

func (a *VerifiedArchive) captureFile(
	ctx context.Context,
	source *RootedSource,
	nodeDir, relPath string,
) (*VerifiedFile, []byte, error) {
	file, err := source.OpenRegularPath(relPath)
	if err != nil {
		return nil, nil, err
	}

	infoBefore, err := file.Stat()
	if err != nil {
		return nil, nil, errors.Join(
			fmt.Errorf("inspect %s: %w", filepath.Join(nodeDir, relPath), err),
			wrapArchiveCloseError(filepath.Join(nodeDir, relPath), file.Close()),
		)
	}

	contentHash := sha256.New()
	nodeHash := sha256.New()
	_, _ = nodeHash.Write([]byte(relPath))
	_, _ = nodeHash.Write([]byte{0})

	copyErr := copyContext(ctx, io.MultiWriter(contentHash, nodeHash), file)
	infoAfter, statErr := file.Stat()
	closeErr := file.Close()

	if copyErr != nil || statErr != nil || closeErr != nil {
		return nil, nil, errors.Join(
			wrapArchiveReadError(filepath.Join(nodeDir, relPath), copyErr),
			wrapArchiveReadError(filepath.Join(nodeDir, relPath), statErr),
			wrapArchiveCloseError(filepath.Join(nodeDir, relPath), closeErr),
		)
	}

	if !sameVerifiedInfo(infoBefore, infoAfter) {
		return nil, nil, fmt.Errorf("%s changed while being verified: %w",
			filepath.Join(nodeDir, relPath), ErrVerifiedArchiveChanged)
	}

	nodeRelative, err := filepath.Rel(a.path, nodeDir)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve verified file path: %w", err)
	}

	var digest [sha256.Size]byte
	copy(digest[:], contentHash.Sum(nil))

	return &VerifiedFile{
		archive:     a,
		archivePath: filepath.Clean(filepath.Join(nodeRelative, relPath)),
		digest:      digest,
		info:        infoBefore,
	}, nodeHash.Sum(nil), nil
}

func readAndHashContext(ctx context.Context, reader io.Reader) ([]byte, [sha256.Size]byte, error) {
	var data bytes.Buffer

	hash := sha256.New()
	if err := copyContext(ctx, io.MultiWriter(&data, hash), reader); err != nil {
		return nil, [sha256.Size]byte{}, err
	}

	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))

	return data.Bytes(), digest, nil
}

func hashReaderAtContext(ctx context.Context, reader io.ReaderAt, size int64) ([sha256.Size]byte, error) {
	if size < 0 {
		return [sha256.Size]byte{}, fmt.Errorf("negative archive file size %d", size)
	}

	hash := sha256.New()
	if err := copyContext(ctx, hash, io.NewSectionReader(reader, 0, size)); err != nil {
		return [sha256.Size]byte{}, err
	}

	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))

	return digest, nil
}

func copyContext(ctx context.Context, writer io.Writer, reader io.Reader) error {
	buffer := make([]byte, 32*1024)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		count, readErr := reader.Read(buffer)
		if count > 0 {
			written, writeErr := writer.Write(buffer[:count])
			if writeErr != nil {
				return writeErr
			}

			if written != count {
				return io.ErrShortWrite
			}
		}

		if errors.Is(readErr, io.EOF) {
			return nil
		}

		if readErr != nil {
			return readErr
		}

		if count == 0 {
			return io.ErrNoProgress
		}
	}
}

func sameVerifiedInfo(expected, actual os.FileInfo) bool {
	return os.SameFile(expected, actual) &&
		expected.Mode() == actual.Mode() &&
		expected.Size() == actual.Size() &&
		expected.ModTime().Equal(actual.ModTime())
}

func wrapArchiveReadError(path string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("read archive file %s: %w", path, err)
}

func wrapArchiveCloseError(path string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("close archive file %s: %w", path, err)
}

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
	source, err := OpenRootedSource(nodeDir)
	if err != nil {
		return NodeChecksum{}, err
	}

	defer func() { _ = source.Close() }()

	return computeNodeChecksum(source)
}

func computeNodeChecksum(source *RootedSource) (NodeChecksum, error) {
	paths, err := collectNodeFiles(source)
	if err != nil {
		return NodeChecksum{}, err
	}

	sort.Strings(paths)

	final := sha256.New()

	for _, relPath := range paths {
		fh, err := computeFileHash(source, relPath)
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
	source, err := OpenRootedSource(nodeDir)
	if err != nil {
		return err
	}

	defer func() { _ = source.Close() }()

	sy, err := readSnapshotYAML(source)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%s: %w", nodeDir, ErrSnapshotYAMLMissing)
		}

		return err
	}

	got, err := computeNodeChecksum(source)
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
// (ClassifyBlockPayload for data.bin[.<ext>], OpenRegularFile for data.tar). It complements VerifyNode:
// VerifyNode checks the integrity digest over the node's files, while snapshot.yaml — excluded
// from that digest — is validated here. Returns ErrSnapshotYAMLMissing when snapshot.yaml is
// absent, and propagates ClassifyBlockPayload's ErrInvalidBlockPayload for a malformed payload.
func ValidateNodeMetadata(nodeDir string) error {
	source, err := OpenRootedSource(nodeDir)
	if err != nil {
		return err
	}

	defer func() { _ = source.Close() }()

	sy, err := readSnapshotYAML(source)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%s: %w", nodeDir, ErrSnapshotYAMLMissing)
		}

		return err
	}

	_, hasBlock, err := ClassifyBlockPayloadIn(source)
	if err != nil {
		return fmt.Errorf("%s: %w", nodeDir, err)
	}

	hasFS := false

	tarFile, statErr := source.OpenRegularFile(FsTarName)
	if statErr == nil {
		_ = tarFile.Close()
		hasFS = true
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("inspect %s in %s: %w", FsTarName, nodeDir, statErr)
	}

	if err := ValidateSnapshotYAML(sy, hasBlock, hasFS); err != nil {
		return fmt.Errorf("%s: %w", nodeDir, err)
	}

	return nil
}

// collectNodeFiles returns the relative paths of all files in nodeDir that
// contribute to the node checksum. The returned paths are not sorted; callers
// must sort them before computing the digest.
func collectNodeFiles(source *RootedSource) ([]string, error) {
	var paths []string

	manifests, err := source.OpenDirectory(ManifestsDirName)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("read %s: %w", ManifestsDirName, err)
		}
	} else {
		defer func() { _ = manifests.Close() }()
	}

	var entries []os.DirEntry
	if manifests != nil {
		entries, err = manifests.ReadDirectory()
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", ManifestsDirName, err)
		}
	}

	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}

		relPath := filepath.Join(ManifestsDirName, e.Name())

		file, openErr := source.OpenRegularPath(relPath)
		if openErr != nil {
			return nil, fmt.Errorf("inspect manifest %s: %w", relPath, openErr)
		}

		_ = file.Close()

		paths = append(paths, relPath)
	}

	blockPayload, blockFound, findErr := ClassifyBlockPayloadIn(source)
	if findErr != nil {
		return nil, fmt.Errorf("classify block payload in %s: %w", source.Path(), findErr)
	}

	if blockFound {
		rel, relErr := filepath.Rel(source.Path(), blockPayload.Path)
		if relErr != nil {
			return nil, relErr
		}

		paths = append(paths, rel)
	}

	// Single-volume filesystem tar (data.tar).
	tarFile, statErr := source.OpenRegularFile(FsTarName)
	if statErr == nil {
		_ = tarFile.Close()

		paths = append(paths, FsTarName)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect %s: %w", filepath.Join(source.Path(), FsTarName), statErr)
	}

	dataDir, err := source.OpenDirectory(DataDirName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return paths, nil
		}

		return nil, fmt.Errorf("walk %s: %w", DataDirName, err)
	}

	defer func() { _ = dataDir.Close() }()

	dataPaths, err := collectLegacyDataFiles(dataDir, DataDirName)
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", DataDirName, err)
	}

	paths = append(paths, dataPaths...)

	return paths, nil
}

func collectLegacyDataFiles(dir *RootedSource, relDir string) ([]string, error) {
	entries, err := dir.ReadDirectory()
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			if strings.HasSuffix(entry.Name(), ".d") {
				continue
			}

			child, openErr := dir.OpenDirectory(entry.Name())
			if openErr != nil {
				return nil, openErr
			}

			childPaths, collectErr := collectLegacyDataFiles(child, filepath.Join(relDir, entry.Name()))
			closeErr := child.Close()

			if collectErr != nil {
				return nil, collectErr
			}

			if closeErr != nil {
				return nil, fmt.Errorf("close archive directory %s: %w", child.Path(), closeErr)
			}

			paths = append(paths, childPaths...)

			continue
		}

		file, openErr := dir.OpenRegularFile(entry.Name())
		if openErr != nil {
			return nil, openErr
		}

		_ = file.Close()

		paths = append(paths, filepath.Join(relDir, entry.Name()))
	}

	return paths, nil
}

// computeFileHash computes a SHA-256 digest over relPath (null-terminated) followed
// by the raw content of absPath.  Using a per-file hash before folding into the
// final digest prevents length-extension and path/content confusion.
func computeFileHash(source *RootedSource, relPath string) ([]byte, error) {
	h := sha256.New()
	h.Write([]byte(relPath))
	h.Write([]byte{0})

	f, err := source.OpenRegularPath(relPath)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", filepath.Join(source.Path(), relPath), err)
	}

	defer func() { _ = f.Close() }()

	if _, err := io.Copy(h, f); err != nil {
		return nil, fmt.Errorf("read %s: %w", filepath.Join(source.Path(), relPath), err)
	}

	return h.Sum(nil), nil
}
