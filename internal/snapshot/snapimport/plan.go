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

// Package snapimport implements the `d8 snapshot upload` command: it reconstructs a
// snapshot tree in a target namespace from a local archive produced by
// `d8 snapshot download`, walking the tree bottom-up and, per node, creating an
// import-mode CR, importing volume data for data leaves (via SVDM DataImport), and
// POSTing the node's manifests plus its direct child refs to the state-snapshotter
// manifests-and-children-refs-upload aggregated subresource.
package snapimport

import (
	gotar "archive/tar"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

const (
	defaultPlanMaxDepth              = 64
	defaultPlanMaxNodes              = 10_000
	defaultPlanMaxManifestBytes      = 16 << 20
	defaultPlanMaxTotalMetadataBytes = 256 << 20
	defaultPlanMaxManifestsPerNode   = 10_000

	// maxReportedSkippedDirs bounds how many skipped-directory paths BuildPlan's
	// reporting variant retains for the caller's warning log. skippedNodeDirs.Total
	// is never truncated, only the Paths sample used for the human-readable message.
	maxReportedSkippedDirs = 32
)

// ErrPlanBudget is returned when archive planning exceeds a configured resource limit.
var ErrPlanBudget = errors.New("snapshot import plan budget exceeded")

// ChildRef is a direct-child reference for a manifests-and-children-refs-upload payload.
// The child namespace is implicit (it is always the upload target namespace), mirroring
// the server-side SnapshotChildRef shape.
type ChildRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

// PlannedNode is one archive node resolved for import. Nodes are returned by BuildPlan
// in post-order (deepest descendants first, root last) so that data leaves and child
// SnapshotContents materialise before their parents reference them.
type PlannedNode struct {
	// Dir is the absolute path of the node directory in the archive.
	Dir string
	// APIVersion/Kind/Name identify the snapshot CR for this node (from snapshot.yaml).
	APIVersion string
	Kind       string
	Name       string
	// SourceNamespace is the namespace recorded in the archive (informational; the import
	// always targets the user-supplied namespace).
	SourceNamespace string
	// Manifests are the node's own captured manifests (from manifests/), the same shape
	// the server returned from manifests-download.
	Manifests []unstructured.Unstructured
	// Children are the direct child snapshot refs (from snapshots/<child>/snapshot.yaml).
	Children []ChildRef
	// DataFile is the absolute path to the node's single-volume block data file
	// (data.bin[.<ext>]) when present; empty when the node carries no importable
	// block volume data.
	DataFile string
	// Ext is DataFile's codec extension, resolved by
	// archive.ClassifyBlockPayload alongside DataFile: "" for the raw/none
	// codec, ".zst", ".gz", or ".lz4" — matching compress.Codec.Ext. Callers
	// MUST use this field instead of filepath.Ext(DataFile): filepath.Ext on
	// the raw name "data.bin" returns ".bin", not "" (see
	// archive.BlockPayload.Ext's doc comment). Empty when HasBlockData() is
	// false.
	Ext string
	// FilesystemData is true when the node carries filesystem-volume data (data.tar).
	FilesystemData bool
	// TarFile is the absolute path to the node's filesystem-volume data file (data.tar).
	// It is always set when FilesystemData is true.
	TarFile string
	// SourceObjectRef carries the structured spec.sourceRef from a domain snapshot CR
	// ({apiVersion,kind,name} of the source object), read from snapshot.yaml. Nil for
	// core Snapshot nodes and CSI VolumeSnapshot data leaves.
	SourceObjectRef *archive.SourceObjectRef
	// StorageClassName/Size/VolumeMode are the captured scratch-volume parameters of this
	// leaf's volume, read from snapshot.yaml Volumes[0]. They feed the PopulateData
	// DataImport spec.storageParams on re-import (storageClassName and size are required by
	// the DataImport CRD; volumeMode is optional). Empty for structural/aggregator nodes
	// that own no volume data.
	StorageClassName string
	Size             string
	VolumeMode       string
	// NodeChecksum is the full checksum verified by the archive integrity preflight.
	NodeChecksum string
	// SizeBytes is Size parsed once into its canonical byte count before cluster mutation.
	SizeBytes int64
	// PayloadKind and Codec are the classified on-disk upload representation.
	PayloadKind string
	Codec       string
	// DataImportIdentity is the versioned full content identity used to qualify and
	// validate the shared DataImport object.
	DataImportIdentity string

	snapshotDigest [sha256.Size]byte
	snapshotInfo   os.FileInfo
	manifestFiles  []plannedManifest
	archiveView    *archive.VerifiedArchive
	payloadFile    *archive.VerifiedFile
	payloadInfo    os.FileInfo
}

type planTopology struct {
	nodes   map[string]int
	parents map[string]int
}

// skippedNodeDirs reports archive child directories BuildPlan's reporting variant skipped
// because they carry no snapshot.yaml — leftovers of an interrupted-and-resumed download
// redirected to a collision path (see archive.CollisionNodeDir), not importable nodes.
// Paths is truncated to maxReportedSkippedDirs; Total counts every skip regardless.
type skippedNodeDirs struct {
	Paths []string
	Total int
}

type plannedManifest struct {
	name   string
	digest [sha256.Size]byte
	info   os.FileInfo
}

// PlanLimits bounds archive traversal and metadata retained by BuildPlanWithLimits.
// The root is at depth zero and counts toward MaxNodes. MaxManifestBytes applies
// independently to snapshot.yaml and each manifest file.
type PlanLimits struct {
	MaxDepth              int
	MaxNodes              int
	MaxManifestBytes      int64
	MaxTotalMetadataBytes int64
	MaxManifestsPerNode   int
}

type planBuilder struct {
	limits              PlanLimits
	snapshotReadOptions archive.SnapshotYAMLReadOptions
	nodeCount           int
	metadataBytes       int64
	skippedDirs         []string
	skippedDirsTotal    int
}

// recordSkippedDir records a child directory skipped for carrying no snapshot.yaml. The
// sample kept for the caller's warning log is bounded by maxReportedSkippedDirs; the total
// count is not, so a warning can always report exactly how many were skipped.
func (b *planBuilder) recordSkippedDir(path string) {
	b.skippedDirsTotal++

	if len(b.skippedDirs) < maxReportedSkippedDirs {
		b.skippedDirs = append(b.skippedDirs, path)
	}
}

// skips returns the skipped-directory report accumulated over the traversal so far.
func (b *planBuilder) skips() skippedNodeDirs {
	return skippedNodeDirs{Paths: b.skippedDirs, Total: b.skippedDirsTotal}
}

// Ref returns the node's aggregated-API node ref (target namespace applied by the caller).
func (n PlannedNode) Ref(namespace string) aggapi.NodeRef {
	return aggapi.NodeRef{
		APIVersion: n.APIVersion,
		Kind:       n.Kind,
		Name:       n.Name,
		Namespace:  namespace,
	}
}

// HasBlockData reports whether the node carries a single-volume block data file.
func (n PlannedNode) HasBlockData() bool {
	return n.DataFile != ""
}

// isDomainDataLeaf reports whether the node is a domain data leaf: it carries volume data
// (block or filesystem) and is neither a core Snapshot nor a CSI VolumeSnapshot leaf.
// Domain data leaves (e.g. DemoVirtualDiskSnapshot) and CSI leaves both stream their volume
// content through a PopulateData DataImport; the server-side reverse-lookup matches the leaf
// against the DataImport's spec.snapshotRef (apiVersion/kind/name).
func (n PlannedNode) isDomainDataLeaf() bool {
	return !n.isStructural() && !n.isVolumeSnapshotLeaf() && (n.HasBlockData() || n.FilesystemData)
}

// BuildPlan walks the archive rooted at rootDir and returns its nodes in post-order
// (leaves first, root last). Each node's own manifests, direct child refs, and volume
// data file (if any) are resolved.
func BuildPlan(rootDir string) ([]PlannedNode, error) {
	return BuildPlanWithLimits(rootDir, DefaultPlanLimits())
}

// BuildPlanWithOptions builds an import plan under an explicit snapshot.yaml compatibility policy.
func BuildPlanWithOptions(
	rootDir string,
	options archive.SnapshotYAMLReadOptions,
) ([]PlannedNode, error) {
	return BuildPlanWithLimitsAndOptions(rootDir, DefaultPlanLimits(), options)
}

// BuildPlanWithLimits builds an import plan subject to explicit traversal and metadata limits.
func BuildPlanWithLimits(rootDir string, limits PlanLimits) ([]PlannedNode, error) {
	return BuildPlanWithLimitsAndOptions(rootDir, limits, archive.SnapshotYAMLReadOptions{})
}

// BuildPlanWithLimitsAndOptions builds an import plan with explicit resource and compatibility policy.
func BuildPlanWithLimitsAndOptions(
	rootDir string,
	limits PlanLimits,
	options archive.SnapshotYAMLReadOptions,
) ([]PlannedNode, error) {
	return buildPlanWithLimits(rootDir, nil, limits, options)
}

func buildPlanFromVerifiedArchive(view *archive.VerifiedArchive) ([]PlannedNode, error) {
	return buildPlanFromVerifiedArchiveWithOptions(view, archive.SnapshotYAMLReadOptions{})
}

func buildPlanFromVerifiedArchiveWithOptions(
	view *archive.VerifiedArchive,
	options archive.SnapshotYAMLReadOptions,
) ([]PlannedNode, error) {
	plan, _, err := buildPlanFromVerifiedArchiveReportingSkips(view, options)

	return plan, err
}

// buildPlanFromVerifiedArchiveReportingSkips builds an import plan exactly like
// buildPlanFromVerifiedArchiveWithOptions, additionally reporting archive child directories
// skipped for carrying no snapshot.yaml (see hasSnapshotYAML and planBuilder.recordSkippedDir).
func buildPlanFromVerifiedArchiveReportingSkips(
	view *archive.VerifiedArchive,
	options archive.SnapshotYAMLReadOptions,
) ([]PlannedNode, skippedNodeDirs, error) {
	builder, err := newPlanBuilder(DefaultPlanLimits(), options)
	if err != nil {
		return nil, skippedNodeDirs{}, err
	}

	var plan []PlannedNode
	if _, err := builder.appendPostOrder(view.RootSource(), &plan, 0); err != nil {
		return nil, skippedNodeDirs{}, err
	}

	if _, err := indexPlanTopology(plan); err != nil {
		return nil, skippedNodeDirs{}, err
	}

	return plan, builder.skips(), nil
}

func buildPlan(rootDir string, hook archive.OpenBoundaryHook) ([]PlannedNode, error) {
	return buildPlanWithLimits(
		rootDir,
		hook,
		DefaultPlanLimits(),
		archive.SnapshotYAMLReadOptions{},
	)
}

func buildPlanWithLimits(
	rootDir string,
	hook archive.OpenBoundaryHook,
	limits PlanLimits,
	options archive.SnapshotYAMLReadOptions,
) ([]PlannedNode, error) {
	builder, err := newPlanBuilder(limits, options)
	if err != nil {
		return nil, err
	}

	rootDir, err = filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("resolve archive path: %w", err)
	}

	source, err := archive.OpenRootedSourceWithHook(rootDir, hook)
	if err != nil {
		return nil, fmt.Errorf("inspect archive root %s: %w", rootDir, err)
	}

	defer func() { _ = source.Close() }()

	var plan []PlannedNode
	if _, err := builder.appendPostOrder(source, &plan, 0); err != nil {
		return nil, err
	}

	if _, err := indexPlanTopology(plan); err != nil {
		return nil, err
	}

	return plan, nil
}

// DefaultPlanLimits returns the limits used by BuildPlan. Planning visits at most
// 10,000 nodes and 64 child directories below the root. It retains at most
// 256 MiB of raw metadata, accepts at most 10,000 manifests per node, and reads
// at most 16 MiB from snapshot.yaml or one manifest.
func DefaultPlanLimits() PlanLimits {
	return PlanLimits{
		MaxDepth:              defaultPlanMaxDepth,
		MaxNodes:              defaultPlanMaxNodes,
		MaxManifestBytes:      defaultPlanMaxManifestBytes,
		MaxTotalMetadataBytes: defaultPlanMaxTotalMetadataBytes,
		MaxManifestsPerNode:   defaultPlanMaxManifestsPerNode,
	}
}

func newPlanBuilder(
	limits PlanLimits,
	options archive.SnapshotYAMLReadOptions,
) (*planBuilder, error) {
	switch {
	case limits.MaxDepth < 0:
		return nil, fmt.Errorf("snapshot import plan maxDepth must be non-negative: %w", ErrPlanBudget)
	case limits.MaxNodes <= 0:
		return nil, fmt.Errorf("snapshot import plan maxNodes must be positive: %w", ErrPlanBudget)
	case limits.MaxManifestBytes <= 0 || limits.MaxManifestBytes == math.MaxInt64:
		return nil, fmt.Errorf("snapshot import plan maxManifestBytes must be positive and less than %d: %w",
			int64(math.MaxInt64), ErrPlanBudget)
	case limits.MaxTotalMetadataBytes <= 0:
		return nil, fmt.Errorf("snapshot import plan maxTotalMetadataBytes must be positive: %w", ErrPlanBudget)
	case limits.MaxManifestsPerNode < 0:
		return nil, fmt.Errorf("snapshot import plan maxManifestsPerNode must be non-negative: %w", ErrPlanBudget)
	}

	return &planBuilder{limits: limits, snapshotReadOptions: options}, nil
}

// indexPlanTopology validates canonical node identities and physical parent-child
// relationships before any caller indexes or filters the plan.
func indexPlanTopology(plan []PlannedNode) (planTopology, error) {
	nodeOccurrences := make(map[string][]int, len(plan))
	childParents := make(map[string][]int)
	childRefs := make(map[string]ChildRef)
	duplicateChildren := make(map[string]map[string]int)

	for i := range plan {
		key := nodeKey(plan[i])
		nodeOccurrences[key] = append(nodeOccurrences[key], i)

		childCounts := make(map[string]int, len(plan[i].Children))
		for _, child := range plan[i].Children {
			childKey := refKey(child.APIVersion, child.Kind, child.Name)

			childCounts[childKey]++
			if _, known := childRefs[childKey]; !known {
				childRefs[childKey] = child
			}
		}

		for childKey, count := range childCounts {
			childParents[childKey] = append(childParents[childKey], i)

			if count > 1 {
				if duplicateChildren[key] == nil {
					duplicateChildren[key] = make(map[string]int)
				}

				duplicateChildren[key][childKey] = count
			}
		}
	}

	issues := make([]string, 0)

	nodeKeys := sortedMapKeys(nodeOccurrences)
	for _, key := range nodeKeys {
		indices := nodeOccurrences[key]
		if len(indices) < 2 {
			continue
		}

		paths := make([]string, 0, len(indices))
		for _, index := range indices {
			paths = append(paths, plan[index].Dir)
		}

		sort.Strings(paths)

		issues = append(issues, fmt.Sprintf(
			"canonical identity %s appears in multiple directories: %s",
			nodeIdentity(plan[indices[0]]), strings.Join(paths, ", ")))
	}

	parentKeys := sortedMapKeys(duplicateChildren)
	for _, parentKey := range parentKeys {
		childKeys := sortedMapKeys(duplicateChildren[parentKey])
		parentIndex := nodeOccurrences[parentKey][0]

		for _, childKey := range childKeys {
			child := childRefs[childKey]
			issues = append(issues, fmt.Sprintf(
				"parent %s at %s references child %s %d times",
				nodeIdentity(plan[parentIndex]), plan[parentIndex].Dir,
				refIdentity(child), duplicateChildren[parentKey][childKey]))
		}
	}

	childKeys := sortedMapKeys(childParents)
	for _, childKey := range childKeys {
		parentIndices := childParents[childKey]
		if len(parentIndices) > 1 {
			parents := make([]string, 0, len(parentIndices))
			for _, parentIndex := range parentIndices {
				parents = append(parents, fmt.Sprintf(
					"%s at %s", nodeIdentity(plan[parentIndex]), plan[parentIndex].Dir))
			}

			sort.Strings(parents)

			issues = append(issues, fmt.Sprintf(
				"child %s has multiple physical parents: %s",
				refIdentity(childRefs[childKey]), strings.Join(parents, ", ")))
		}

		if _, ok := nodeOccurrences[childKey]; !ok {
			issues = append(issues, fmt.Sprintf(
				"child %s is referenced but has no node directory",
				refIdentity(childRefs[childKey])))
		}
	}

	if len(issues) > 0 {
		return planTopology{}, fmt.Errorf("invalid archive plan topology: %s", strings.Join(issues, "; "))
	}

	topology := planTopology{
		nodes:   make(map[string]int, len(nodeOccurrences)),
		parents: make(map[string]int, len(childParents)),
	}

	for key, indices := range nodeOccurrences {
		topology.nodes[key] = indices[0]
	}

	for key, indices := range childParents {
		topology.parents[key] = indices[0]
	}

	return topology, nil
}

func sortedMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

func nodeIdentity(node PlannedNode) string {
	return fmt.Sprintf("%s %s/%s", node.APIVersion, node.Kind, node.Name)
}

func refIdentity(ref ChildRef) string {
	return fmt.Sprintf("%s %s/%s", ref.APIVersion, ref.Kind, ref.Name)
}

// appendPostOrder visits children first (sorted for determinism), then the node itself.
func (b *planBuilder) appendPostOrder(
	source *archive.RootedSource,
	plan *[]PlannedNode,
	depth int,
) (PlannedNode, error) {
	if depth > b.limits.MaxDepth {
		return PlannedNode{}, fmt.Errorf(
			"snapshot import plan maxDepth %d exceeded at %s (depth %d; root depth is 0): %w",
			b.limits.MaxDepth,
			source.Path(),
			depth,
			ErrPlanBudget,
		)
	}

	if b.nodeCount >= b.limits.MaxNodes {
		return PlannedNode{}, fmt.Errorf(
			"snapshot import plan maxNodes %d exceeded while adding %s: %w",
			b.limits.MaxNodes,
			source.Path(),
			ErrPlanBudget,
		)
	}

	b.nodeCount++

	node, err := b.readNode(source)
	if err != nil {
		return PlannedNode{}, err
	}

	childNames, snapshotsDir, err := childNodeNames(source)
	if err != nil {
		return PlannedNode{}, err
	}

	if snapshotsDir != nil {
		defer func() { _ = snapshotsDir.Close() }()
	}

	for _, childName := range childNames {
		child, openErr := snapshotsDir.OpenDirectory(childName)
		if openErr != nil {
			return PlannedNode{}, fmt.Errorf("inspect child node directory %s: %w",
				filepath.Join(snapshotsDir.Path(), childName), openErr)
		}

		// A child directory with no snapshot.yaml is never a finalized node: it is a
		// collision/resume directory archive.CollisionNodeDir left behind for an
		// interrupted-and-resumed download (see archive/resume.go), authenticated as
		// absent from the parent's ChildrenChecksum by computeNodeChildrenChecksum's
		// identical tolerance. Skip it rather than fail the whole plan; a non-regular
		// snapshot.yaml (symlink, directory) still fails hard, since that is not the
		// leftover shape this tolerance exists for.
		present, checkErr := hasSnapshotYAML(child)
		if checkErr != nil {
			return PlannedNode{}, errors.Join(
				fmt.Errorf("check snapshot.yaml presence in %s: %w", child.Path(), checkErr),
				child.Close(),
			)
		}

		if !present {
			b.recordSkippedDir(child.Path())

			if closeErr := child.Close(); closeErr != nil {
				return PlannedNode{}, fmt.Errorf("close skipped child node directory %s: %w",
					child.Path(), closeErr)
			}

			continue
		}

		childNode, appendErr := b.appendPostOrder(child, plan, depth+1)
		closeErr := child.Close()

		if appendErr != nil {
			return PlannedNode{}, appendErr
		}

		if closeErr != nil {
			return PlannedNode{}, fmt.Errorf("close child node directory %s: %w", child.Path(), closeErr)
		}

		node.Children = append(node.Children, ChildRef{
			APIVersion: childNode.APIVersion,
			Kind:       childNode.Kind,
			Name:       childNode.Name,
		})
	}

	*plan = append(*plan, node)

	return node, nil
}

// hasSnapshotYAML reports whether source's directory holds a regular snapshot.yaml file.
// os.ErrNotExist is the only tolerated outcome (false, nil); any other error — including a
// non-regular snapshot.yaml such as a symlink or a directory — is fatal, so this check cannot
// be widened beyond the orphaned collision/resume directories it exists to tolerate.
func hasSnapshotYAML(source *archive.RootedSource) (bool, error) {
	file, err := source.OpenRegularFile(archive.SnapshotYAMLName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}

		return false, err
	}

	if closeErr := file.Close(); closeErr != nil {
		return false, fmt.Errorf("close %s: %w",
			filepath.Join(source.Path(), archive.SnapshotYAMLName), closeErr)
	}

	return true, nil
}

// readNode reads a single node directory's snapshot.yaml, own manifests and data file.
func (b *planBuilder) readNode(source *archive.RootedSource) (PlannedNode, error) {
	dir := source.Path()

	snapshotFile, err := source.OpenRegularFile(archive.SnapshotYAMLName)
	if err != nil {
		return PlannedNode{}, fmt.Errorf("read node %s: %w", dir, err)
	}

	snapshotPath := filepath.Join(dir, archive.SnapshotYAMLName)

	snapshotData, snapshotInfoBefore, err := b.readMetadataFile(snapshotFile, snapshotPath)
	if err != nil {
		return PlannedNode{}, fmt.Errorf("read node %s snapshot.yaml: %w", dir, err)
	}

	sy, err := archive.UnmarshalSnapshotYAML(snapshotData, b.snapshotReadOptions)
	if err != nil {
		return PlannedNode{}, fmt.Errorf("unmarshal node %s snapshot.yaml: %w", dir, err)
	}

	if sy.Kind == "" || sy.Name == "" || sy.APIVersion == "" {
		return PlannedNode{}, fmt.Errorf("node %s: snapshot.yaml missing apiVersion/kind/name", dir)
	}

	manifests, manifestFiles, err := b.readManifests(source)
	if err != nil {
		return PlannedNode{}, fmt.Errorf("node %s: %w", dir, err)
	}

	legacyData, openErr := source.OpenDirectory(archive.DataDirName)
	if openErr == nil {
		_ = legacyData.Close()
	} else if !errors.Is(openErr, os.ErrNotExist) {
		return PlannedNode{}, fmt.Errorf("node %s: inspect legacy data directory: %w", dir, openErr)
	}

	node := PlannedNode{
		Dir:             dir,
		APIVersion:      sy.APIVersion,
		Kind:            sy.Kind,
		Name:            sy.Name,
		SourceNamespace: sy.Namespace,
		Manifests:       manifests,
		SourceObjectRef: sy.SourceObjectRef,
		NodeChecksum:    sy.Checksum.Hex,
		snapshotDigest:  sha256.Sum256(snapshotData),
		snapshotInfo:    snapshotInfoBefore,
		manifestFiles:   manifestFiles,
	}

	// Data leaves carry exactly one volume; lift its captured scratch-volume parameters onto
	// the node so EnsureDataImport can send them as the PopulateData DataImport's
	// spec.storageParams. Structural/aggregator nodes have no Volumes and leave these empty.
	if len(sy.Volumes) > 0 {
		v := sy.Volumes[0]
		node.StorageClassName = v.StorageClassName
		node.Size = v.Size
		node.VolumeMode = v.VolumeMode
	}

	blockPayload, found, err := archive.ClassifyBlockPayloadIn(source)
	if err != nil {
		return PlannedNode{}, fmt.Errorf("node %s: %w", dir, err)
	}

	if found {
		node.DataFile = blockPayload.Path
		node.Ext = blockPayload.Ext
		node.PayloadKind = dataImportPayloadBlock
		node.Codec = codecName(blockPayload.Ext)

		payload, openErr := source.OpenRegularFile(filepath.Base(blockPayload.Path))
		if openErr != nil {
			return PlannedNode{}, fmt.Errorf("node %s: reopen block payload: %w", dir, openErr)
		}

		node.payloadInfo, err = statAndClosePlanFile(payload, blockPayload.Path)
		if err != nil {
			return PlannedNode{}, err
		}
	}

	tarPath := filepath.Join(dir, archive.FsTarName)

	tarFile, statErr := source.OpenRegularFile(archive.FsTarName)
	if statErr == nil {
		node.payloadInfo, err = statAndClosePlanFile(tarFile, tarPath)
		if err != nil {
			return PlannedNode{}, err
		}

		node.FilesystemData = true
		node.TarFile = tarPath
		node.PayloadKind = dataImportPayloadFilesystem
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return PlannedNode{}, fmt.Errorf("node %s: inspect filesystem payload: %w", dir, statErr)
	}

	if (node.HasBlockData() || node.FilesystemData) &&
		node.NodeChecksum != "" && node.VolumeMode != "" && node.StorageClassName != "" && node.Size != "" {
		if node.FilesystemData {
			codec, codecErr := classifyTarCodec(source)
			if codecErr != nil {
				return PlannedNode{}, fmt.Errorf("node %s: %w", dir, codecErr)
			}

			node.Codec = codec
		}

		size, parseErr := resource.ParseQuantity(node.Size)
		if parseErr != nil {
			return node, nil
		}

		node.SizeBytes = size.Value()
		if node.SizeBytes > 0 {
			node.DataImportIdentity = dataImportIdentity(node)
		}
	}

	return node, nil
}

func codecName(ext string) string {
	switch ext {
	case ".zst":
		return "zstd"
	case ".gz":
		return "gzip"
	case ".lz4":
		return "lz4"
	default:
		return "none"
	}
}

func classifyTarCodec(source *archive.RootedSource) (string, error) {
	file, err := source.OpenRegularFile(archive.FsTarName)
	if err != nil {
		return "", fmt.Errorf("open filesystem payload: %w", err)
	}

	defer func() { _ = file.Close() }()

	reader := gotar.NewReader(file)
	codec := ""

	for {
		header, nextErr := reader.Next()
		if nextErr != nil {
			if nextErr == io.EOF {
				break
			}

			return "", fmt.Errorf("read filesystem payload: %w", nextErr)
		}

		if header.Typeflag != gotar.TypeReg && header.Typeflag != 0 {
			continue
		}

		metadata, metadataErr := archive.ParseFSMetadata(header)
		if metadataErr != nil {
			return "", fmt.Errorf("parse filesystem payload entry %q: %w", header.Name, metadataErr)
		}

		entryCodec := metadata.Codec
		if codec == "" {
			codec = entryCodec

			continue
		}

		if codec != entryCodec {
			return "", fmt.Errorf("filesystem payload mixes codecs %q and %q", codec, entryCodec)
		}
	}

	if codec == "" {
		return "none", nil
	}

	return codec, nil
}

func dataImportIdentity(node PlannedNode) string {
	encoded := make([]byte, 0, 256)

	for _, field := range []string{
		dataImportIdentityVersion,
		node.APIVersion,
		node.Kind,
		node.Name,
		node.NodeChecksum,
		node.VolumeMode,
		node.StorageClassName,
		strconv.FormatInt(node.SizeBytes, 10),
		node.PayloadKind,
		node.Codec,
	} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))

		encoded = append(encoded, length[:]...)
		encoded = append(encoded, field...)
	}

	sum := sha256.Sum256(encoded)

	return hex.EncodeToString(sum[:])
}

// readManifests parses every <dir>/manifests/*.yaml file into an unstructured object.
func (b *planBuilder) readManifests(
	source *archive.RootedSource,
) ([]unstructured.Unstructured, []plannedManifest, error) {
	manifestsDir, err := source.OpenDirectory(archive.ManifestsDirName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil
		}

		return nil, nil, fmt.Errorf("read manifests dir: %w", err)
	}

	defer func() { _ = manifestsDir.Close() }()

	entries, err := manifestsDir.ReadDirectory()
	if err != nil {
		return nil, nil, fmt.Errorf("read manifests dir: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".yaml" {
			continue
		}

		names = append(names, e.Name())
	}

	sort.Strings(names)

	if len(names) > b.limits.MaxManifestsPerNode {
		return nil, nil, fmt.Errorf(
			"snapshot import plan maxManifestsPerNode %d exceeded at %s (%d manifests): %w",
			b.limits.MaxManifestsPerNode,
			source.Path(),
			len(names),
			ErrPlanBudget,
		)
	}

	manifests := make([]unstructured.Unstructured, 0, len(names))
	files := make([]plannedManifest, 0, len(names))

	for _, name := range names {
		file, openErr := manifestsDir.OpenRegularFile(name)
		if openErr != nil {
			return nil, nil, fmt.Errorf("open manifest %s: %w", name, openErr)
		}

		path := filepath.Join(manifestsDir.Path(), name)

		data, infoBefore, readErr := b.readMetadataFile(file, path)
		if readErr != nil {
			return nil, nil, fmt.Errorf("read manifest %s: %w", name, readErr)
		}

		var obj map[string]interface{}
		if err := sigsyaml.Unmarshal(data, &obj); err != nil {
			return nil, nil, fmt.Errorf("unmarshal manifest %s: %w", name, err)
		}

		manifests = append(manifests, unstructured.Unstructured{Object: obj})
		files = append(files, plannedManifest{name: name, digest: sha256.Sum256(data), info: infoBefore})
	}

	return manifests, files, nil
}

func (b *planBuilder) readMetadataFile(file *os.File, path string) ([]byte, os.FileInfo, error) {
	infoBefore, statBeforeErr := file.Stat()
	if statBeforeErr != nil {
		return nil, nil, errors.Join(
			wrapPlanFileError("inspect before read", path, statBeforeErr),
			wrapPlanFileError("close", path, file.Close()),
		)
	}

	if infoBefore.Size() > b.limits.MaxManifestBytes {
		closeErr := file.Close()
		budgetErr := fmt.Errorf(
			"snapshot import plan maxManifestBytes %d exceeded by %s (%d bytes): %w",
			b.limits.MaxManifestBytes,
			path,
			infoBefore.Size(),
			ErrPlanBudget,
		)

		return nil, nil, errors.Join(budgetErr, wrapPlanFileError("close", path, closeErr))
	}

	data, readErr := io.ReadAll(io.LimitReader(file, b.limits.MaxManifestBytes+1))
	infoAfter, statAfterErr := file.Stat()
	closeErr := file.Close()

	if readErr != nil || statAfterErr != nil || closeErr != nil {
		return nil, nil, errors.Join(
			wrapPlanFileError("read", path, readErr),
			wrapPlanFileError("inspect after read", path, statAfterErr),
			wrapPlanFileError("close", path, closeErr),
		)
	}

	if !samePlanFileInfo(infoBefore, infoAfter) {
		return nil, nil, fmt.Errorf("metadata file %s changed while planning: %w",
			path, archive.ErrVerifiedArchiveChanged)
	}

	if int64(len(data)) > b.limits.MaxManifestBytes {
		return nil, nil, fmt.Errorf(
			"snapshot import plan maxManifestBytes %d exceeded by %s: %w",
			b.limits.MaxManifestBytes,
			path,
			ErrPlanBudget,
		)
	}

	if err := b.accountMetadata(path, int64(len(data))); err != nil {
		return nil, nil, err
	}

	return data, infoBefore, nil
}

func (b *planBuilder) accountMetadata(path string, size int64) error {
	if size > b.limits.MaxTotalMetadataBytes-b.metadataBytes {
		return fmt.Errorf(
			"snapshot import plan maxTotalMetadataBytes %d exceeded while adding %s (%d bytes already retained): %w",
			b.limits.MaxTotalMetadataBytes,
			path,
			b.metadataBytes,
			ErrPlanBudget,
		)
	}

	b.metadataBytes += size

	return nil
}

func statAndClosePlanFile(file *os.File, path string) (os.FileInfo, error) {
	info, statErr := file.Stat()
	closeErr := file.Close()

	if statErr != nil || closeErr != nil {
		return nil, errors.Join(
			wrapPlanFileError("inspect", path, statErr),
			wrapPlanFileError("close", path, closeErr),
		)
	}

	return info, nil
}

func samePlanFileInfo(expected, actual os.FileInfo) bool {
	return os.SameFile(expected, actual) &&
		expected.Mode() == actual.Mode() &&
		expected.Size() == actual.Size() &&
		expected.ModTime().Equal(actual.ModTime())
}

func wrapPlanFileError(operation, path string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%s archive file %s: %w", operation, path, err)
}

// childNodeNames returns sorted direct child names and a pinned snapshots directory.
func childNodeNames(source *archive.RootedSource) ([]string, *archive.RootedSource, error) {
	snapshotsDir, err := source.OpenDirectory(archive.SnapshotsDirName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil
		}

		return nil, nil, fmt.Errorf("read snapshots dir %s: %w",
			filepath.Join(source.Path(), archive.SnapshotsDirName), err)
	}

	entries, err := snapshotsDir.ReadDirectory()
	if err != nil {
		_ = snapshotsDir.Close()

		return nil, nil, fmt.Errorf("read snapshots dir %s: %w", snapshotsDir.Path(), err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}

	sort.Strings(names)

	return names, snapshotsDir, nil
}
