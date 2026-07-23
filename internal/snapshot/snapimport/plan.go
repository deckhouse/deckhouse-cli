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

// Package snapimport implements the `d8 snapshot import` command: it reconstructs a
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
}

type planTopology struct {
	nodes   map[string]int
	parents map[string]int
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
	return buildPlan(rootDir, nil)
}

func buildPlan(rootDir string, hook archive.OpenBoundaryHook) ([]PlannedNode, error) {
	rootDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("resolve archive path: %w", err)
	}

	source, err := archive.OpenRootedSourceWithHook(rootDir, hook)
	if err != nil {
		return nil, fmt.Errorf("inspect archive root %s: %w", rootDir, err)
	}

	defer func() { _ = source.Close() }()

	var plan []PlannedNode
	if _, err := appendPostOrder(source, &plan); err != nil {
		return nil, err
	}

	if _, err := indexPlanTopology(plan); err != nil {
		return nil, err
	}

	return plan, nil
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
func appendPostOrder(source *archive.RootedSource, plan *[]PlannedNode) (PlannedNode, error) {
	node, err := readNode(source)
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

		childNode, appendErr := appendPostOrder(child, plan)
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

// readNode reads a single node directory's snapshot.yaml, own manifests and data file.
func readNode(source *archive.RootedSource) (PlannedNode, error) {
	dir := source.Path()

	snapshotFile, err := source.OpenRegularFile(archive.SnapshotYAMLName)
	if err != nil {
		return PlannedNode{}, fmt.Errorf("read node %s: %w", dir, err)
	}

	snapshotData, readErr := io.ReadAll(snapshotFile)
	closeErr := snapshotFile.Close()

	if readErr != nil {
		return PlannedNode{}, fmt.Errorf("read node %s snapshot.yaml: %w", dir, readErr)
	}

	if closeErr != nil {
		return PlannedNode{}, fmt.Errorf("close node %s snapshot.yaml: %w", dir, closeErr)
	}

	var sy archive.SnapshotYAML
	if err := sigsyaml.Unmarshal(snapshotData, &sy); err != nil {
		return PlannedNode{}, fmt.Errorf("unmarshal node %s snapshot.yaml: %w", dir, err)
	}

	if sy.Kind == "" || sy.Name == "" || sy.APIVersion == "" {
		return PlannedNode{}, fmt.Errorf("node %s: snapshot.yaml missing apiVersion/kind/name", dir)
	}

	manifests, err := readManifests(source)
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
	}

	tarPath := filepath.Join(dir, archive.FsTarName)

	tarFile, statErr := source.OpenRegularFile(archive.FsTarName)
	if statErr == nil {
		_ = tarFile.Close()
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
func readManifests(source *archive.RootedSource) ([]unstructured.Unstructured, error) {
	manifestsDir, err := source.OpenDirectory(archive.ManifestsDirName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("read manifests dir: %w", err)
	}

	defer func() { _ = manifestsDir.Close() }()

	entries, err := manifestsDir.ReadDirectory()
	if err != nil {
		return nil, fmt.Errorf("read manifests dir: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".yaml" {
			continue
		}

		names = append(names, e.Name())
	}

	sort.Strings(names)

	manifests := make([]unstructured.Unstructured, 0, len(names))
	for _, name := range names {
		file, openErr := manifestsDir.OpenRegularFile(name)
		if openErr != nil {
			return nil, fmt.Errorf("open manifest %s: %w", name, openErr)
		}

		data, readErr := io.ReadAll(file)
		closeErr := file.Close()

		if readErr != nil {
			return nil, fmt.Errorf("read manifest %s: %w", name, readErr)
		}

		if closeErr != nil {
			return nil, fmt.Errorf("close manifest %s: %w", name, closeErr)
		}

		var obj map[string]interface{}
		if err := sigsyaml.Unmarshal(data, &obj); err != nil {
			return nil, fmt.Errorf("unmarshal manifest %s: %w", name, err)
		}

		manifests = append(manifests, unstructured.Unstructured{Object: obj})
	}

	return manifests, nil
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
