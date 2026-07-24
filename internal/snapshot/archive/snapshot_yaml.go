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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/resource"
	sigsyaml "sigs.k8s.io/yaml"
)

// ChecksumAlgorithmSHA256 is the only checksum algorithm the archive uses; it is the value
// ComputeNodeChecksum records and ValidateSnapshotYAML requires in NodeChecksum.Algorithm.
const ChecksumAlgorithmSHA256 = "sha256"

// sha256HexLen is the length of a hex-encoded SHA-256 digest (32 bytes → 64 hex chars).
const sha256HexLen = 64

// Volume modes recorded in VolumeInfo.VolumeMode. They mirror the corev1.PersistentVolumeMode
// values written by the download side (see volume.nodeDataToVolumeInfo) and MUST agree with
// the on-disk payload kind: a block payload (data.bin[.<ext>]) is "Block", a filesystem
// payload (data.tar) is "Filesystem". ValidateSnapshotYAML enforces that agreement.
const (
	VolumeModeBlock      = "Block"
	VolumeModeFilesystem = "Filesystem"
)

// ErrNonRegularArchiveArtifact marks an archive path whose host-filesystem type is unsafe.
var ErrNonRegularArchiveArtifact = errors.New("non-regular archive artifact")

// ErrArchiveMountBoundaryUnsupported marks a platform or runtime that cannot prove an opened
// archive descendant remained on its parent's mount. Upload traversal fails closed in this case.
var ErrArchiveMountBoundaryUnsupported = errors.New("archive mount-boundary verification unsupported")

// ErrInvalidSnapshotYAML is returned by ValidateSnapshotYAML/ValidateNodeMetadata when a
// node's snapshot.yaml violates a structural metadata invariant. snapshot.yaml is EXCLUDED
// from the integrity digest (ComputeNodeChecksum/VerifyNode), so these invariants are not
// covered by the checksum and must be validated separately before the archive is trusted.
var ErrInvalidSnapshotYAML = errors.New("invalid snapshot.yaml")

// SnapshotYAML is the per-node file written at <nodeDir>/snapshot.yaml.
// It records the snapshot CR identity and the locally-computed integrity checksum.
// sigs.k8s.io/yaml uses json struct tags for marshaling and unmarshaling.
type SnapshotYAML struct {
	// APIVersion is the apiVersion of the snapshot CR (e.g. "state-snapshotter.deckhouse.io/v1alpha1").
	APIVersion string `json:"apiVersion"`
	// Kind is the kind of the snapshot CR (e.g. "Snapshot", "DemoVirtualDiskSnapshot").
	Kind string `json:"kind"`
	// Name is the metadata.name of the snapshot CR.
	Name string `json:"name"`
	// Namespace is the namespace of the snapshot CR. Omitted for cluster-scoped resources.
	Namespace string `json:"namespace,omitempty"`
	// UID is the metadata.uid of the snapshot CR. It is the identity component the resume
	// scan matches (matchesIdentity), tying a node directory to the exact snapshot CR
	// (including UID) rather than to the source-object name. Does not affect
	// ComputeNodeChecksum because snapshot.yaml is excluded from the integrity digest.
	UID string `json:"uid,omitempty"`
	// SourceName is the metadata.name of the original captured source object
	// (status.sourceRef.name), recorded for readability. Omitted when the node has no
	// source (e.g. some import nodes). It is NOT an identity component (resume uses UID)
	// and does not affect ComputeNodeChecksum because snapshot.yaml is excluded from the
	// integrity digest.
	SourceName string `json:"sourceName,omitempty"`
	// SourceObjectRef carries the structured spec.sourceRef from a domain snapshot CR
	// ({apiVersion,kind,name} of the source object). Absent for core Snapshot nodes and
	// CSI VolumeSnapshot data leaves. Does not affect ComputeNodeChecksum because
	// snapshot.yaml is excluded from the integrity digest.
	SourceObjectRef *SourceObjectRef `json:"sourceObjectRef,omitempty"`
	// Checksum is the locally-computed node integrity digest.
	Checksum NodeChecksum `json:"checksum"`
	// Volumes lists the captured PVC volumes owned by this node.
	//
	//   - A node that captured its own volume (namespaced status.data present) carries
	//     exactly one VolumeInfo (Variant A, cardinality ≤1) — this covers both
	//     non-aggregator domain nodes and orphan leaf volume nodes.
	//   - Aggregator snapshot nodes and purely-manifest nodes carry no volumes
	//     and the field is omitted (omitempty).
	//
	// snapshot.yaml is excluded from ComputeNodeChecksum/VerifyNode, so this
	// field does not affect the integrity digest.
	Volumes []VolumeInfo `json:"volumes,omitempty"`
}

// SourceObjectRef is the structured spec.sourceRef from a domain snapshot CR, persisted
// in snapshot.yaml so the import side can recreate the CR in import mode. The fields
// mirror the domain CR's spec.sourceRef (apiVersion/kind/name of the source object).
// Omitted for core Snapshot nodes and CSI VolumeSnapshot data leaves.
type SourceObjectRef struct {
	// APIVersion is the apiVersion of the source object (e.g. "demo.deckhouse.io/v1alpha1").
	APIVersion string `json:"apiVersion"`
	// Kind is the kind of the source object (e.g. "DemoVirtualDisk").
	Kind string `json:"kind"`
	// Name is the metadata.name of the source object.
	Name string `json:"name"`
}

// VolumeObjectRef is a reference to a Kubernetes object stored in the volume block
// of snapshot.yaml. It captures the identity fields needed to correlate the archive
// entry with live cluster resources.
type VolumeObjectRef struct {
	// APIVersion is the apiVersion of the referenced object.
	APIVersion string `json:"apiVersion"`
	// Kind is the kind of the referenced object.
	Kind string `json:"kind"`
	// Name is the metadata.name of the referenced object.
	Name string `json:"name"`
	// Namespace is the namespace of the referenced object. Omitted for cluster-scoped objects.
	Namespace string `json:"namespace,omitempty"`
	// UID is the metadata.uid of the referenced object. Omitted when unknown.
	UID string `json:"uid,omitempty"`
}

// VolumeInfo describes the captured volume associated with a volume node.
// It is written into the volume block of snapshot.yaml so the archive is self-describing.
type VolumeInfo struct {
	// Target is the source PVC that was captured (its apiVersion/kind/name/namespace/uid).
	Target VolumeObjectRef `json:"target"`
	// Artifact is the VolumeSnapshotContent that held the durable data artifact at capture
	// time. Recorded for provenance/debugging; the re-import path no longer consumes it.
	Artifact VolumeObjectRef `json:"artifact"`
	// VolumeMode records the source volume mode (Block or Filesystem). On re-import it is sent
	// as the PopulateData DataImport's spec.storageParams.volumeMode (optional).
	VolumeMode string `json:"volumeMode,omitempty"`
	// StorageClassName records the source StorageClass of the captured volume. On re-import it
	// is sent as the PopulateData DataImport's spec.storageParams.storageClassName (required).
	StorageClassName string `json:"storageClassName,omitempty"`
	// Size records the real allocated size of the captured volume (e.g. "10Gi"), taken from
	// VolumeSnapshotContent.status.restoreSize. On re-import it is sent as the PopulateData
	// DataImport's spec.storageParams.size (required).
	Size string `json:"size,omitempty"`
}

// NodeChecksum is the locally-computed integrity digest for one node directory.
// The digest covers the node's own files (manifests and volume data) but excludes
// snapshot.yaml itself and the snapshots/ child directory.
type NodeChecksum struct {
	// Algorithm is always "sha256".
	Algorithm string `json:"algorithm"`
	// Hex is the full lowercase hex-encoded SHA-256 digest.
	Hex string `json:"hex"`
	// Short is the first 8 characters of Hex, used as a collision-suffix when
	// a node directory with the same name already exists with a different checksum.
	Short string `json:"short"`
}

// WriteSnapshotYAML is WriteSnapshotYAMLContext with a non-cancellable context.
func WriteSnapshotYAML(nodeDir string, sy SnapshotYAML) error {
	return WriteSnapshotYAMLContext(context.Background(), nodeDir, sy)
}

// WriteSnapshotYAMLContext serialises sy to YAML and writes it atomically to
// <nodeDir>/snapshot.yaml. An existing file at that path is replaced.
func WriteSnapshotYAMLContext(ctx context.Context, nodeDir string, sy SnapshotYAML) error {
	data, err := sigsyaml.Marshal(sy)
	if err != nil {
		return fmt.Errorf("marshal snapshot.yaml: %w", err)
	}

	path := filepath.Join(nodeDir, SnapshotYAMLName)

	return WriteFileAtomicContext(ctx, path, bytes.NewReader(data))
}

// RootedSource pins one verified archive directory and opens descendants relative to its
// descriptor or handle. Retaining the source prevents path replacement from redirecting later
// opens. Every child source also verifies its retained parent chain before use, so replacing an
// already-validated archive directory fails closed instead of silently continuing through a
// detached tree.
type RootedSource struct {
	dir         *os.File
	path        string
	parent      *RootedSource
	name        string
	hook        OpenBoundaryHook
	lockBinding *rootedLockBinding
}

// PinnedDirectory is a descriptor-relative directory beneath a RootedSource.
// It remains confined to the originally opened root even if path components are
// replaced after opening.
type PinnedDirectory struct {
	dir           *os.File
	path          string
	hook          OpenBoundaryHook
	verifyBinding func() error
}

type archiveDirectory interface {
	archiveClose() error
	archiveOpenDirectory(string) (archiveDirectory, error)
	archiveOpenDirectoryPath(string) (archiveDirectory, error)
	archiveOpenRegularFile(string) (*os.File, error)
	archiveOpenRegularPath(string) (*os.File, error)
	archivePath() string
	archiveReadDirectory() ([]os.DirEntry, error)
}

type rootedLockBinding struct {
	mu      sync.RWMutex
	claimed bool
	verify  func() error
}

// OpenBoundaryHook runs immediately before a rooted descendant or enumeration open. It exists
// to make adversarial replacement tests deterministic; production callers pass nil.
type OpenBoundaryHook func(path string)

// OpenRootedSource opens path as a real directory without following its final component and
// pins the resulting descriptor or handle. Callers must close the returned source.
func OpenRootedSource(path string) (*RootedSource, error) {
	return OpenRootedSourceWithHook(path, nil)
}

// OpenRootedSourceWithHook is OpenRootedSource with a deterministic boundary hook.
func OpenRootedSourceWithHook(path string, hook OpenBoundaryHook) (*RootedSource, error) {
	dir, err := openArchiveRoot(path)
	if err != nil {
		return nil, err
	}

	source := &RootedSource{
		dir:         dir,
		path:        path,
		hook:        hook,
		lockBinding: &rootedLockBinding{},
	}
	if err := source.verifyCurrent(); err != nil {
		_ = dir.Close()

		return nil, err
	}

	return source, nil
}

// Close releases the directory descriptor or handle retained by source.
func (s *RootedSource) Close() error {
	return s.dir.Close()
}

// Path returns the diagnostic host path represented by source.
func (s *RootedSource) Path() string {
	return s.path
}

// OpenDirectory opens one real child directory relative to source. The parent source must
// remain open until the child is closed.
func (s *RootedSource) OpenDirectory(name string) (*RootedSource, error) {
	if err := validateArchiveComponent(name); err != nil {
		return nil, fmt.Errorf("open archive directory %s: %w", filepath.Join(s.path, name), err)
	}

	path := filepath.Join(s.path, name)
	s.runHook(path)

	if err := s.verifyCurrent(); err != nil {
		return nil, err
	}

	dir, err := openArchiveDirectoryAt(s.dir, name, path)
	if err != nil {
		return nil, err
	}

	return &RootedSource{
		dir:         dir,
		path:        path,
		parent:      s,
		name:        name,
		hook:        s.hook,
		lockBinding: s.lockBinding,
	}, nil
}

// OpenDirectoryPath securely descends through a relative directory path while
// retaining at most two transient descendant descriptors.
func (s *RootedSource) OpenDirectoryPath(path string) (*PinnedDirectory, error) {
	clean := filepath.Clean(filepath.FromSlash(path))
	if !filepath.IsLocal(clean) {
		return nil, fmt.Errorf("invalid rooted directory path %q: %w", path, ErrNonRegularArchiveArtifact)
	}

	s.runHook(s.path)

	if err := s.verifyCurrent(); err != nil {
		return nil, err
	}

	components := []string{"."}
	if clean != "." {
		components = strings.Split(clean, string(filepath.Separator))
	}

	var (
		owned       *os.File
		parent      = s.dir
		currentPath = s.path
	)

	for _, component := range components {
		if err := validateArchiveComponent(component); err != nil && component != "." {
			if owned != nil {
				_ = owned.Close()
			}

			return nil, fmt.Errorf("open rooted directory %s: %w", filepath.Join(currentPath, component), err)
		}

		if component != "." {
			currentPath = filepath.Join(currentPath, component)
		}

		s.runHook(currentPath)

		child, err := openArchiveDirectoryAt(parent, component, currentPath)
		if err != nil {
			if owned != nil {
				_ = owned.Close()
			}

			return nil, err
		}

		if owned != nil {
			_ = owned.Close()
		}

		owned = child
		parent = child
	}

	return &PinnedDirectory{
		dir:           owned,
		path:          currentPath,
		hook:          s.hook,
		verifyBinding: s.verifyCurrent,
	}, nil
}

// ReadDirectory reads source through a fresh descriptor so repeated enumeration is stable.
func (s *RootedSource) ReadDirectory() ([]os.DirEntry, error) {
	s.runHook(s.path)

	if err := s.verifyCurrent(); err != nil {
		return nil, err
	}

	dir, err := openArchiveDirectoryAt(s.dir, ".", s.path)
	if err != nil {
		return nil, err
	}

	defer func() { _ = dir.Close() }()

	entries, err := dir.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("read directory %s: %w", s.path, err)
	}

	return entries, nil
}

// OpenRegularFile opens one regular child file without following a final symlink or reparse
// point. Ordinary hard links remain supported because they are regular files; host link count
// does not imply archive/tar hard-link semantics.
func (s *RootedSource) OpenRegularFile(name string) (*os.File, error) {
	if err := validateArchiveComponent(name); err != nil {
		return nil, fmt.Errorf("open archive file %s: %w", filepath.Join(s.path, name), err)
	}

	path := filepath.Join(s.path, name)
	s.runHook(path)

	if err := s.verifyCurrent(); err != nil {
		return nil, err
	}

	return openArchiveRegularAt(s.dir, name, path)
}

// OpenRegularPath descends through real directories beneath source and opens the final regular
// file without following links at any component.
func (s *RootedSource) OpenRegularPath(path string) (*os.File, error) {
	clean := filepath.Clean(path)
	if !filepath.IsLocal(clean) || clean == "." {
		return nil, fmt.Errorf("invalid archive relative path %q: %w", path, ErrNonRegularArchiveArtifact)
	}

	components := strings.Split(clean, string(filepath.Separator))
	current := s

	opened := make([]*RootedSource, 0, len(components)-1)

	defer func() {
		for i := len(opened) - 1; i >= 0; i-- {
			_ = opened[i].Close()
		}
	}()

	for _, component := range components[:len(components)-1] {
		child, err := current.OpenDirectory(component)
		if err != nil {
			return nil, err
		}

		opened = append(opened, child)
		current = child
	}

	return current.OpenRegularFile(components[len(components)-1])
}

func (s *RootedSource) archiveClose() error {
	return s.Close()
}

func (s *RootedSource) archiveOpenDirectory(name string) (archiveDirectory, error) {
	return s.OpenDirectory(name)
}

func (s *RootedSource) archiveOpenDirectoryPath(path string) (archiveDirectory, error) {
	return s.OpenDirectoryPath(path)
}

func (s *RootedSource) archiveOpenRegularFile(name string) (*os.File, error) {
	return s.OpenRegularFile(name)
}

func (s *RootedSource) archiveOpenRegularPath(path string) (*os.File, error) {
	return s.OpenRegularPath(path)
}

func (s *RootedSource) archivePath() string {
	return s.Path()
}

func (s *RootedSource) archiveReadDirectory() ([]os.DirEntry, error) {
	return s.ReadDirectory()
}

// Close releases the descriptor retained by directory.
func (d *PinnedDirectory) Close() error {
	return d.dir.Close()
}

// Path returns the diagnostic host path represented by directory.
func (d *PinnedDirectory) Path() string {
	return d.path
}

// ReadDirectory reads the next bounded batch from the pinned directory.
func (d *PinnedDirectory) ReadDirectory(count int) ([]os.DirEntry, error) {
	d.runHook(d.path)

	if err := d.verifyCurrentBinding(); err != nil {
		return nil, err
	}

	entries, err := d.dir.ReadDir(count)
	if err != nil {
		return entries, fmt.Errorf("read directory %s: %w", d.path, err)
	}

	return entries, nil
}

// OpenDirectory opens one real child directory relative to the pinned descriptor.
func (d *PinnedDirectory) OpenDirectory(name string) (*PinnedDirectory, error) {
	if err := validateArchiveComponent(name); err != nil {
		return nil, fmt.Errorf("open rooted directory %s: %w", filepath.Join(d.path, name), err)
	}

	path := filepath.Join(d.path, name)
	d.runHook(path)

	if err := d.verifyCurrentBinding(); err != nil {
		return nil, err
	}

	dir, err := openArchiveDirectoryAt(d.dir, name, path)
	if err != nil {
		return nil, err
	}

	return &PinnedDirectory{
		dir:           dir,
		path:          path,
		hook:          d.hook,
		verifyBinding: d.verifyBinding,
	}, nil
}

// OpenRegularFile opens one regular child without following links.
func (d *PinnedDirectory) OpenRegularFile(name string) (*os.File, error) {
	if err := validateArchiveComponent(name); err != nil {
		return nil, fmt.Errorf("open rooted file %s: %w", filepath.Join(d.path, name), err)
	}

	path := filepath.Join(d.path, name)
	d.runHook(path)

	if err := d.verifyCurrentBinding(); err != nil {
		return nil, err
	}

	return openArchiveRegularAt(d.dir, name, path)
}

func (d *PinnedDirectory) archiveClose() error {
	return d.Close()
}

func (d *PinnedDirectory) archiveOpenDirectory(name string) (archiveDirectory, error) {
	return d.OpenDirectory(name)
}

func (d *PinnedDirectory) archiveOpenDirectoryPath(path string) (archiveDirectory, error) {
	clean := filepath.Clean(filepath.FromSlash(path))
	if !filepath.IsLocal(clean) {
		return nil, fmt.Errorf("invalid rooted directory path %q: %w", path, ErrNonRegularArchiveArtifact)
	}

	if clean == "." {
		dir, err := openArchiveDirectoryAt(d.dir, ".", d.path)
		if err != nil {
			return nil, err
		}

		return &PinnedDirectory{
			dir:           dir,
			path:          d.path,
			hook:          d.hook,
			verifyBinding: d.verifyBinding,
		}, nil
	}

	components := strings.Split(clean, string(filepath.Separator))

	var current *PinnedDirectory

	parent := d
	for _, component := range components {
		child, err := parent.OpenDirectory(component)
		if err != nil {
			if current != nil {
				_ = current.Close()
			}

			return nil, err
		}

		if current != nil {
			_ = current.Close()
		}

		current = child
		parent = child
	}

	return current, nil
}

func (d *PinnedDirectory) archiveOpenRegularFile(name string) (*os.File, error) {
	return d.OpenRegularFile(name)
}

func (d *PinnedDirectory) archiveOpenRegularPath(path string) (*os.File, error) {
	clean := filepath.Clean(path)
	if !filepath.IsLocal(clean) || clean == "." {
		return nil, fmt.Errorf("invalid archive relative path %q: %w", path, ErrNonRegularArchiveArtifact)
	}

	components := strings.Split(clean, string(filepath.Separator))
	if len(components) == 1 {
		return d.OpenRegularFile(components[0])
	}

	var current *PinnedDirectory

	parent := d
	for _, component := range components[:len(components)-1] {
		child, err := parent.OpenDirectory(component)
		if err != nil {
			if current != nil {
				_ = current.Close()
			}

			return nil, err
		}

		if current != nil {
			_ = current.Close()
		}

		current = child
		parent = child
	}

	file, err := current.OpenRegularFile(components[len(components)-1])

	closeErr := current.Close()
	if err != nil || closeErr != nil {
		return nil, errors.Join(err, closeErr)
	}

	return file, nil
}

func (d *PinnedDirectory) archivePath() string {
	return d.Path()
}

func (d *PinnedDirectory) archiveReadDirectory() ([]os.DirEntry, error) {
	return d.ReadDirectory(-1)
}

func (d *PinnedDirectory) runHook(path string) {
	if d.hook != nil {
		d.hook(path)
	}
}

func (s *RootedSource) verifyCurrent() error {
	if err := s.verifyNamespaceCurrent(); err != nil {
		return err
	}

	return s.lockBinding.verifyCurrent()
}

func (s *RootedSource) verifyNamespaceCurrent() error {
	if s.parent == nil {
		return verifyArchiveRoot(s.path, s.dir)
	}

	if err := s.parent.verifyNamespaceCurrent(); err != nil {
		return err
	}

	current, err := openArchiveDirectoryAt(s.parent.dir, s.name, s.path)
	if err != nil {
		return err
	}

	defer func() { _ = current.Close() }()

	expectedInfo, err := s.dir.Stat()
	if err != nil {
		return fmt.Errorf("inspect pinned archive directory %s: %w", s.path, err)
	}

	currentInfo, err := current.Stat()
	if err != nil {
		return fmt.Errorf("inspect current archive directory %s: %w", s.path, err)
	}

	if !os.SameFile(expectedInfo, currentInfo) {
		return fmt.Errorf("%s changed after validation: %w", s.path, ErrNonRegularArchiveArtifact)
	}

	return nil
}

func (s *RootedSource) claimLock() error {
	s.lockBinding.mu.Lock()
	defer s.lockBinding.mu.Unlock()

	if s.lockBinding.claimed {
		return errors.New("archive root already has a bound lock")
	}

	s.lockBinding.claimed = true

	return nil
}

func (s *RootedSource) bindLock(verify func() error) error {
	s.lockBinding.mu.Lock()
	defer s.lockBinding.mu.Unlock()

	if !s.lockBinding.claimed || s.lockBinding.verify != nil {
		return errors.New("archive root lock claim is invalid")
	}

	s.lockBinding.verify = verify

	return nil
}

func (s *RootedSource) unbindLock() {
	s.lockBinding.mu.Lock()
	s.lockBinding.claimed = false
	s.lockBinding.verify = nil
	s.lockBinding.mu.Unlock()
}

func (b *rootedLockBinding) verifyCurrent() error {
	b.mu.RLock()
	claimed := b.claimed
	verify := b.verify
	b.mu.RUnlock()

	if !claimed {
		return nil
	}

	if verify == nil {
		return fmt.Errorf("%w: lock acquisition is incomplete", ErrArchiveLockChanged)
	}

	return verify()
}

func (d *PinnedDirectory) verifyCurrentBinding() error {
	if d.verifyBinding == nil {
		return nil
	}

	return d.verifyBinding()
}

func (s *RootedSource) runHook(path string) {
	if s.hook != nil {
		s.hook(path)
	}
}

func validateArchiveComponent(name string) error {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name {
		return fmt.Errorf("invalid archive path component %q: %w", name, ErrNonRegularArchiveArtifact)
	}

	return nil
}

// OpenRegularFile rejects unsafe path components and opens the final regular file relative to
// a pinned descriptor for its parent directory.
func OpenRegularFile(path string) (*os.File, error) {
	parent, err := OpenRootedSource(filepath.Dir(path))
	if err != nil {
		return nil, err
	}

	defer func() { _ = parent.Close() }()

	return parent.OpenRegularFile(filepath.Base(path))
}

// ReadDirectory returns entries from a pinned descriptor opened as a real directory.
func ReadDirectory(path string) ([]os.DirEntry, error) {
	dir, err := OpenRootedSource(path)
	if err != nil {
		return nil, err
	}

	defer func() { _ = dir.Close() }()

	return dir.ReadDirectory()
}

func verifyArchiveRoot(path string, dir *os.File) error {
	current, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect archive root %s: %w", path, err)
	}

	if !current.Mode().IsDir() {
		return archiveModeError(path, current.Mode(), true)
	}

	opened, err := dir.Stat()
	if err != nil {
		return fmt.Errorf("inspect pinned archive root %s: %w", path, err)
	}

	if !opened.Mode().IsDir() || !os.SameFile(current, opened) {
		return fmt.Errorf("%s changed after validation: %w", path, ErrNonRegularArchiveArtifact)
	}

	return nil
}

func classifyArchiveOpenError(path string, wantDir bool, openErr error) error {
	current, err := os.Lstat(path)
	if err == nil && !archiveModeMatches(current.Mode(), wantDir) {
		return archiveModeError(path, current.Mode(), wantDir)
	}

	return fmt.Errorf("open archive path %s: %w", path, openErr)
}

func archiveModeMatches(mode os.FileMode, wantDir bool) bool {
	if wantDir {
		return mode.IsDir()
	}

	return mode.IsRegular()
}

func archiveModeError(path string, mode os.FileMode, wantDir bool) error {
	want := "regular file"
	if wantDir {
		want = "directory"
	}

	return fmt.Errorf("%s has mode %s, want %s: %w", path, mode, want, ErrNonRegularArchiveArtifact)
}

// ReadSnapshotYAML reads and deserialises <nodeDir>/snapshot.yaml.
// Returns an error wrapping os.ErrNotExist when the file is absent.
func ReadSnapshotYAML(nodeDir string) (SnapshotYAML, error) {
	source, err := OpenRootedSource(nodeDir)
	if err != nil {
		return SnapshotYAML{}, fmt.Errorf("read snapshot.yaml: %w", err)
	}

	defer func() { _ = source.Close() }()

	return readSnapshotYAML(source)
}

func readSnapshotYAML(source archiveDirectory) (SnapshotYAML, error) {
	file, err := source.archiveOpenRegularFile(SnapshotYAMLName)
	if err != nil {
		return SnapshotYAML{}, fmt.Errorf("read snapshot.yaml: %w", err)
	}

	defer func() { _ = file.Close() }()

	data, err := io.ReadAll(file)
	if err != nil {
		return SnapshotYAML{}, fmt.Errorf("read snapshot.yaml: %w", err)
	}

	var sy SnapshotYAML
	if err := sigsyaml.Unmarshal(data, &sy); err != nil {
		return SnapshotYAML{}, fmt.Errorf("unmarshal snapshot.yaml: %w", err)
	}

	return sy, nil
}

// ValidateSnapshotYAML strictly validates the snapshot.yaml metadata that
// ComputeNodeChecksum/VerifyNode intentionally do NOT cover. Because snapshot.yaml is
// excluded from the integrity digest, a corrupt or mismatched metadata block would pass the
// checksum check unnoticed, so the import path validates it explicitly before any cluster
// mutation. It does NOT claim the checksum covers snapshot.yaml (it does not); it validates
// the excluded metadata as a separate, standalone check.
//
// hasBlockData and hasFilesystemData report the node's on-disk volume payload
// (data.bin[.<ext>] and data.tar respectively); ValidateNodeMetadata derives them from the
// directory. A node is a data node when it carries either payload. The rules:
//
//   - apiVersion, kind and name are required.
//   - checksum.algorithm is "sha256", checksum.hex is 64 lowercase hex chars, and
//     checksum.short is the first 8 chars of hex (ShortChecksum).
//   - sourceObjectRef is all-or-nothing: omitted, or all of apiVersion/kind/name set.
//   - at most one volume (Variant A cardinality, decision #9).
//   - a data node carries exactly one volume with a complete target and artifact identity
//     (apiVersion/kind/name each), a storageClassName, a positive parseable size, and a
//     volumeMode that agrees with the payload kind (Block for data.bin, Filesystem for
//     data.tar).
//   - a non-data node carries no volume.
//
// Authenticated/versioned snapshot.yaml evolution (signing, schema version) is a separate
// concern and out of scope here.
func ValidateSnapshotYAML(sy SnapshotYAML, hasBlockData, hasFilesystemData bool) error {
	if sy.APIVersion == "" || sy.Kind == "" || sy.Name == "" {
		return fmt.Errorf("apiVersion/kind/name are required (got apiVersion=%q kind=%q name=%q): %w",
			sy.APIVersion, sy.Kind, sy.Name, ErrInvalidSnapshotYAML)
	}

	if err := validateChecksum(sy.Checksum); err != nil {
		return err
	}

	if ref := sy.SourceObjectRef; ref != nil {
		if ref.APIVersion == "" || ref.Kind == "" || ref.Name == "" {
			return fmt.Errorf("sourceObjectRef must set all of apiVersion/kind/name or be omitted (got %+v): %w",
				*ref, ErrInvalidSnapshotYAML)
		}
	}

	if len(sy.Volumes) > 1 {
		return fmt.Errorf("a node carries at most one volume, got %d: %w", len(sy.Volumes), ErrInvalidSnapshotYAML)
	}

	if !hasBlockData && !hasFilesystemData {
		if len(sy.Volumes) != 0 {
			return fmt.Errorf("non-data node carries %d volume(s) but has no data payload: %w",
				len(sy.Volumes), ErrInvalidSnapshotYAML)
		}

		return nil
	}

	if len(sy.Volumes) != 1 {
		return fmt.Errorf("data node must carry exactly one volume, got %d: %w", len(sy.Volumes), ErrInvalidSnapshotYAML)
	}

	return validateDataVolume(sy.Volumes[0], hasBlockData)
}

// validateChecksum enforces the algorithm/hex/short consistency of a recorded NodeChecksum.
func validateChecksum(c NodeChecksum) error {
	if c.Algorithm != ChecksumAlgorithmSHA256 {
		return fmt.Errorf("checksum.algorithm must be %q, got %q: %w",
			ChecksumAlgorithmSHA256, c.Algorithm, ErrInvalidSnapshotYAML)
	}

	if len(c.Hex) != sha256HexLen || !isLowerHex(c.Hex) {
		return fmt.Errorf("checksum.hex must be %d lowercase hex characters, got %q: %w",
			sha256HexLen, c.Hex, ErrInvalidSnapshotYAML)
	}

	if want := ShortChecksum(c.Hex); c.Short != want {
		return fmt.Errorf("checksum.short %q is inconsistent with hex (want %q): %w",
			c.Short, want, ErrInvalidSnapshotYAML)
	}

	return nil
}

// validateDataVolume enforces the data-node volume invariants: complete target/artifact
// identity, a storageClassName, a positive parseable size, and a volumeMode that agrees with
// the on-disk payload kind (hasBlockData selects Block, otherwise Filesystem).
func validateDataVolume(v VolumeInfo, hasBlockData bool) error {
	if v.Target.APIVersion == "" || v.Target.Kind == "" || v.Target.Name == "" {
		return fmt.Errorf("data volume target identity is incomplete, apiVersion/kind/name required (got %+v): %w",
			v.Target, ErrInvalidSnapshotYAML)
	}

	if v.Artifact.APIVersion == "" || v.Artifact.Kind == "" || v.Artifact.Name == "" {
		return fmt.Errorf("data volume artifact identity is incomplete, apiVersion/kind/name required (got %+v): %w",
			v.Artifact, ErrInvalidSnapshotYAML)
	}

	if v.StorageClassName == "" {
		return fmt.Errorf("data volume storageClassName is required: %w", ErrInvalidSnapshotYAML)
	}

	q, err := resource.ParseQuantity(v.Size)
	if err != nil {
		return fmt.Errorf("data volume size %q is not a valid quantity: %w", v.Size, errors.Join(ErrInvalidSnapshotYAML, err))
	}

	if q.Sign() <= 0 {
		return fmt.Errorf("data volume size %q must be positive: %w", v.Size, ErrInvalidSnapshotYAML)
	}

	want := VolumeModeFilesystem
	if hasBlockData {
		want = VolumeModeBlock
	}

	if v.VolumeMode != want {
		return fmt.Errorf("data volume volumeMode %q disagrees with the on-disk payload (want %q): %w",
			v.VolumeMode, want, ErrInvalidSnapshotYAML)
	}

	return nil
}

// isLowerHex reports whether s consists solely of lowercase hexadecimal digits.
func isLowerHex(s string) bool {
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}

	return true
}
