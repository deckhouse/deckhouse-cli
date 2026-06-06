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

package listing

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

type Options struct {
	Namespace    string
	SnapshotName string
	ArchiveDir   string
	NodeFilter   string
	WithObjects  bool
}

type Source struct {
	Kind       string `json:"kind"` // "cluster" or "archive"
	Cluster    string `json:"cluster,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	Snapshot   string `json:"snapshot,omitempty"`
	ArchiveDir string `json:"archiveDir,omitempty"`
	ArchiveID  string `json:"archiveId,omitempty"`
}

type ObjectView struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Namespace  string `json:"namespace,omitempty"`
	Digest     string `json:"digest,omitempty"`
	Size       int64  `json:"size,omitempty"`
}

type NodeView struct {
	ID                       string       `json:"id"`
	APIVersion               string       `json:"apiVersion"`
	Kind                     string       `json:"kind"`
	Name                     string       `json:"name"`
	Namespace                string       `json:"namespace,omitempty"`
	ParentID                 string       `json:"parentId,omitempty"`
	BoundSnapshotContentName string       `json:"boundSnapshotContentName,omitempty"`
	ObjectCount              int          `json:"objectCount"` // -1 = unknown (cluster, no --objects)
	Objects                  []ObjectView `json:"objects,omitempty"`
	Children                 []*NodeView  `json:"children,omitempty"`
}

type Tree struct {
	Source    Source                `json:"source"`
	Selection archive.SelectionMode `json:"selection"`
	Complete  bool                  `json:"complete,omitempty"`
	Root      *NodeView             `json:"root"`
}

var (
	buildTreeFunc      = source.BuildTree
	fetchManifestsFunc = source.FetchManifests
)

func SetBuildTreeFunc(f func(ctx context.Context, client ctrlrtclient.Client, namespace, name string) (*source.Node, error)) {
	buildTreeFunc = f
}

func SetFetchManifestsFunc(f source.ManifestFetcher) {
	fetchManifestsFunc = f
}

func ResetFuncs() {
	buildTreeFunc = source.BuildTree
	fetchManifestsFunc = source.FetchManifests
}

func BuildFromCluster(ctx context.Context, sc *safeClient.SafeClient, rtClient ctrlrtclient.Client, opts Options, log *slog.Logger) (*Tree, error) {
	log.Debug("building snapshot tree from cluster", "namespace", opts.Namespace, "snapshot", opts.SnapshotName)

	root, err := buildTreeFunc(ctx, rtClient, opts.Namespace, opts.SnapshotName)
	if err != nil {
		return nil, err
	}

	selected, err := source.SelectSubtree(root, source.TreeOptions{NodeFilter: opts.NodeFilter})
	if err != nil {
		return nil, err
	}

	mode := selectionModeFor(opts)

	serverHost := ""
	if sc != nil {
		serverHost = sc.ServerHost()
	}

	rootView := nodeToView(selected, opts.WithObjects)

	if opts.WithObjects {
		if err := attachClusterObjects(ctx, sc, selected, rootView, log); err != nil {
			return nil, err
		}
	}

	return &Tree{
		Source: Source{
			Kind:      "cluster",
			Cluster:   serverHost,
			Namespace: opts.Namespace,
			Snapshot:  opts.SnapshotName,
		},
		Selection: mode,
		Root:      rootView,
	}, nil
}

func BuildFromArchive(opts Options, log *slog.Logger) (*Tree, error) {
	log.Debug("building snapshot tree from archive", "dir", opts.ArchiveDir)

	reader, err := archive.OpenDir(opts.ArchiveDir)
	if err != nil {
		return nil, err
	}

	meta, err := reader.Meta()
	if err != nil {
		return nil, err
	}

	nodes, err := reader.Nodes()
	if err != nil {
		return nil, err
	}

	nodeMap := nodeViewsFromRecords(nodes)
	wireNodeChildren(nodeMap, nodes)

	if err := attachArchiveObjects(reader, nodeMap); err != nil {
		return nil, fmt.Errorf("read objects index: %w", err)
	}

	rootView, err := archiveRootView(meta, nodeMap, opts)
	if err != nil {
		return nil, err
	}

	dedupTree(rootView)

	if meta.Selection.Mode == archive.SelectionObject {
		pruneEmpty(rootView)
	}

	if !opts.WithObjects {
		clearObjects(rootView)
	}

	return &Tree{
		Source: Source{
			Kind:       "archive",
			ArchiveDir: opts.ArchiveDir,
			ArchiveID:  meta.ArchiveID,
		},
		Selection: meta.Selection.Mode,
		Complete:  archive.IsComplete(opts.ArchiveDir),
		Root:      rootView,
	}, nil
}

func attachClusterObjects(ctx context.Context, sc *safeClient.SafeClient, selected *source.Node, rootView *NodeView, log *slog.Logger) error {
	for _, n := range source.FlatNodes(selected) {
		nv := findNodeView(rootView, n.ID)
		if nv == nil {
			continue
		}

		rawObjects, err := fetchManifestsFunc(ctx, sc, n)
		if err != nil {
			return fmt.Errorf("fetch manifests for %s: %w", n.ID, err)
		}

		nv.Objects = objectViewsFromRaw(rawObjects, n.ID, log)
		nv.ObjectCount = len(nv.Objects)
	}

	dedupTree(rootView)

	return nil
}

func objectViewsFromRaw(rawObjects [][]byte, nodeID string, log *slog.Logger) []ObjectView {
	objs := make([]ObjectView, 0, len(rawObjects))

	for _, raw := range rawObjects {
		ov, err := objectViewFromJSON(raw)
		if err != nil {
			log.Debug("skip unparseable manifest", "node", nodeID, "err", err)

			continue
		}

		objs = append(objs, ov)
	}

	return objs
}

func nodeViewsFromRecords(nodes []archive.NodeRecord) map[string]*NodeView {
	nodeMap := make(map[string]*NodeView, len(nodes))

	for _, nr := range nodes {
		nodeMap[nr.ID] = &NodeView{
			ID:                       nr.ID,
			APIVersion:               nr.APIVersion,
			Kind:                     nr.Kind,
			Name:                     nr.Name,
			Namespace:                nr.Namespace,
			ParentID:                 nr.ParentID,
			BoundSnapshotContentName: nr.BoundSnapshotContentName,
			ObjectCount:              0,
		}
	}

	return nodeMap
}

func wireNodeChildren(nodeMap map[string]*NodeView, nodes []archive.NodeRecord) {
	for _, nr := range nodes {
		parent := nodeMap[nr.ID]
		if parent == nil {
			continue
		}

		for _, childID := range nr.Children {
			child := nodeMap[childID]
			if child == nil {
				continue
			}

			parent.Children = append(parent.Children, child)
		}
	}
}

func attachArchiveObjects(reader *archive.DirReader, nodeMap map[string]*NodeView) error {
	objsByNode, err := objectsByNode(reader)
	if err != nil {
		return err
	}

	for id, nv := range nodeMap {
		objs := objsByNode[id]
		nv.ObjectCount = len(objs)
		nv.Objects = objs
	}

	return nil
}

func objectsByNode(reader *archive.DirReader) (map[string][]ObjectView, error) {
	result := make(map[string][]ObjectView)

	err := reader.ForEachObject(func(or archive.ObjectRecord) error {
		result[or.NodeID] = append(result[or.NodeID], ObjectView{
			APIVersion: or.APIVersion,
			Kind:       or.Kind,
			Name:       or.Name,
			Namespace:  or.Namespace,
			Digest:     or.Digest,
			Size:       or.Size,
		})

		return nil
	})

	return result, err
}

func archiveRootView(meta archive.Meta, nodeMap map[string]*NodeView, opts Options) (*NodeView, error) {
	rootView := nodeMap[meta.Selection.RootNodeID]
	if rootView == nil {
		return nil, fmt.Errorf("root node %q not found in nodes.jsonl", meta.Selection.RootNodeID)
	}

	if opts.NodeFilter == "" {
		return rootView, nil
	}

	filtered := findNodeView(rootView, opts.NodeFilter)
	if filtered == nil {
		return nil, fmt.Errorf("node %q not found in archive; check the ID in indexes/nodes.jsonl", opts.NodeFilter)
	}

	return filtered, nil
}

func selectionModeFor(opts Options) archive.SelectionMode {
	if opts.NodeFilter != "" {
		return archive.SelectionSubtree
	}

	return archive.SelectionFull
}

func nodeToView(n *source.Node, withObjects bool) *NodeView {
	objectCount := -1 // unknown until fetched
	if withObjects {
		objectCount = 0
	}

	nv := &NodeView{
		ID:                       n.ID,
		APIVersion:               n.APIVersion,
		Kind:                     n.Kind,
		Name:                     n.Name,
		Namespace:                n.Namespace,
		ParentID:                 n.ParentID,
		BoundSnapshotContentName: n.BoundSnapshotContentName,
		ObjectCount:              objectCount,
	}

	for _, child := range n.Children {
		nv.Children = append(nv.Children, nodeToView(child, withObjects))
	}

	return nv
}

func findNodeView(nv *NodeView, id string) *NodeView {
	if nv.ID == id {
		return nv
	}

	for _, c := range nv.Children {
		if found := findNodeView(c, id); found != nil {
			return found
		}
	}

	return nil
}

func objectKey(o ObjectView) string {
	return o.APIVersion + "|" + o.Kind + "|" + o.Namespace + "|" + o.Name
}

// dedupTree keeps each object only at the deepest node that captured it and
// resets ObjectCount to the number of retained objects. Returns the set of
// all object keys present anywhere in nv's subtree.
func dedupTree(nv *NodeView) map[string]struct{} {
	childKeys := make(map[string]struct{})
	subtreeKeys := make(map[string]struct{})

	for _, c := range nv.Children {
		for k := range dedupTree(c) {
			childKeys[k] = struct{}{}
			subtreeKeys[k] = struct{}{}
		}
	}

	kept := make([]ObjectView, 0, len(nv.Objects))

	for _, o := range nv.Objects {
		k := objectKey(o)
		subtreeKeys[k] = struct{}{}

		if _, dup := childKeys[k]; dup {
			continue
		}

		kept = append(kept, o)
	}

	nv.Objects = kept
	nv.ObjectCount = len(kept)

	return subtreeKeys
}

func clearObjects(nv *NodeView) {
	nv.Objects = nil

	for _, c := range nv.Children {
		clearObjects(c)
	}
}

// pruneEmpty removes child subtrees that contain zero objects (own count +
// all descendants). Returns the total object count in nv's subtree.
// The root node itself is never removed by this function.
func pruneEmpty(nv *NodeView) int {
	total := nv.ObjectCount
	kept := nv.Children[:0]

	for _, c := range nv.Children {
		if sub := pruneEmpty(c); sub > 0 {
			kept = append(kept, c)
			total += sub
		}
	}

	nv.Children = kept

	return total
}

func objectViewFromJSON(raw []byte) (ObjectView, error) {
	var m map[string]any

	if err := json.Unmarshal(raw, &m); err != nil {
		return ObjectView{}, err
	}

	apiVersion, _ := m["apiVersion"].(string)
	kind, _ := m["kind"].(string)

	var name, ns string

	if meta, ok := m["metadata"].(map[string]any); ok {
		name, _ = meta["name"].(string)
		ns, _ = meta["namespace"].(string)
	}

	return ObjectView{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       name,
		Namespace:  ns,
	}, nil
}
