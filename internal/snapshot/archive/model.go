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

// Package archive defines the on-disk format for a Deckhouse snapshot archive.
//
// Directory layout:
//
//	<output>/
//	  archive.json  - archive identity and selection metadata
//	  index.json    - capabilities, catalog paths, summary counts
//	  COMPLETE      - empty sentinel written last; absent means incomplete
//	  indexes/
//	    nodes.jsonl    - one line per snapshot tree node
//	    objects.jsonl  - one line per downloaded manifest object
//	  manifests/
//	    objects/
//	      <aa>/<bb>/o-<sha256hex>.json.gz  - content-addressed manifest blob
//	  data/         - reserved for future volume data
package archive

import (
	"sort"
	"time"
)

const (
	// Magic is the fixed string in archive.json that identifies the format.
	Magic = "deckhouse.snapshot.archive"

	// SchemaVersion is the current format version.
	SchemaVersion = "v1alpha1"
)

// SelectionMode describes which portion of the snapshot was downloaded.
type SelectionMode string

const (
	SelectionFull    SelectionMode = "full"
	SelectionSubtree SelectionMode = "subtree"
	SelectionObject  SelectionMode = "object"
)

// Meta is the top-level structure of archive.json.
type Meta struct {
	Magic         string    `json:"magic"`
	SchemaVersion string    `json:"schemaVersion"`
	ArchiveID     string    `json:"archiveId"`
	CreatedAt     time.Time `json:"createdAt"`
	CreatedBy     Creator   `json:"createdBy"`
	Source        Source    `json:"source"`
	Selection     Selection `json:"selection"`
}

// Creator records the tool that produced the archive.
type Creator struct {
	Tool    string `json:"tool"`
	Version string `json:"version"`
}

// Source describes the cluster and Snapshot that was downloaded.
type Source struct {
	Cluster             Cluster            `json:"cluster"`
	Namespace           string             `json:"namespace"`
	RootSnapshot        SnapshotRef        `json:"rootSnapshot"`
	RootSnapshotContent SnapshotContentRef `json:"rootSnapshotContent"`
}

// Cluster holds cluster identity fields available at download time.
type Cluster struct {
	Server string `json:"server"`
	UID    string `json:"uid,omitempty"`
}

// SnapshotRef points to the Kubernetes Snapshot object that was the source.
type SnapshotRef struct {
	APIVersion      string `json:"apiVersion"`
	Kind            string `json:"kind"`
	Resource        string `json:"resource"`
	Name            string `json:"name"`
	UID             string `json:"uid,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

// SnapshotContentRef points to the SnapshotContent bound to the root Snapshot.
type SnapshotContentRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

// Selection describes which part of the snapshot was included.
type Selection struct {
	Mode       SelectionMode `json:"mode"`
	RootNodeID string        `json:"rootNodeId"`
	// ObjectFilter is the --object flag value that was used, if any.
	ObjectFilter    string   `json:"objectFilter,omitempty"`
	SelectedNodeIDs []string `json:"selectedNodeIds"`
}

// Index is the top-level structure of index.json.
type Index struct {
	SchemaVersion string             `json:"schemaVersion"`
	Capabilities  IndexCapabilities  `json:"capabilities"`
	ManifestModel IndexManifestModel `json:"manifestModel"`
	Catalogs      IndexCatalogs      `json:"catalogs"`
	Paths         IndexPaths         `json:"paths"`
	Summary       IndexSummary       `json:"summary"`
}

// IndexCapabilities enumerates features present in this archive.
type IndexCapabilities struct {
	Manifests            bool `json:"manifests"`
	Volumes              bool `json:"volumes"`
	RestoreFromArchive   bool `json:"restoreFromArchive"`
	UploadableAsSnapshot bool `json:"uploadableAsSnapshot"`
	PartialSelection     bool `json:"partialSelection"`
	Resumable            bool `json:"resumable"`
}

// IndexManifestModel describes how manifests are encoded.
type IndexManifestModel struct {
	Format      string `json:"format"`
	Compression string `json:"compression"`
	SourceKind  string `json:"sourceKind"`
}

// IndexCatalogs lists the relative paths of the JSONL index files.
type IndexCatalogs struct {
	Nodes   string `json:"nodes"`
	Objects string `json:"objects"`
}

// IndexPaths lists the base directories for blobs.
type IndexPaths struct {
	ManifestsRoot string `json:"manifestsRoot"`
	DataRoot      string `json:"dataRoot"`
}

// IndexSummary holds aggregate counts written by Finalize.
type IndexSummary struct {
	Nodes    int  `json:"nodes"`
	Objects  int  `json:"objects"`
	Volumes  int  `json:"volumes"`
	Complete bool `json:"complete"`
}

// VolumeDataRef is a single volume data binding persisted in NodeRecord.
type VolumeDataRef struct {
	// VSCName is the cluster-scoped VolumeSnapshotContent name.
	VSCName string `json:"vscName"`
	// PVCName is the name of the original PVC.
	PVCName string `json:"pvcName,omitempty"`
	// PVCNamespace is the namespace of the original PVC.
	PVCNamespace string `json:"pvcNamespace,omitempty"`
}

// NodeRecord is one line in indexes/nodes.jsonl.
type NodeRecord struct {
	ID                       string          `json:"id"`
	APIVersion               string          `json:"apiVersion"`
	Kind                     string          `json:"kind"`
	Name                     string          `json:"name"`
	Namespace                string          `json:"namespace,omitempty"`
	ParentID                 string          `json:"parentId,omitempty"`
	Children                 []string        `json:"children"`
	BoundSnapshotContentName string          `json:"boundSnapshotContentName,omitempty"`
	DataRefs                 []VolumeDataRef `json:"dataRefs,omitempty"`
	HasData                  bool            `json:"hasData"`
}

// ObjectRecord is one line in indexes/objects.jsonl.
type ObjectRecord struct {
	NodeID     string `json:"nodeId"`
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Namespace  string `json:"namespace,omitempty"`
	Digest     string `json:"digest"`
	Size       int64  `json:"size"`
	Blob       string `json:"blob"`
}

// ProgressRecord is one line in indexes/progress.jsonl.
// It is written durably after a node's blobs are flushed and fsync'd.
// Finalize regenerates nodes.jsonl and objects.jsonl from the accumulated records.
type ProgressRecord struct {
	NodeID     string         `json:"nodeId"`
	ContentRef string         `json:"contentRef"` // boundSnapshotContentName at download time
	Objects    []ObjectRecord `json:"objects"`
}

// VolumeProgressRecord is one line in indexes/volumes.jsonl.
// It is appended after a single volume download completes (complete=true) or
// after a partial block download (complete=false, bytesDone records the offset).
// Key for resume: NodeID + "/" + VSCName.
type VolumeProgressRecord struct {
	NodeID     string `json:"nodeId"`
	VSCName    string `json:"vscName"`
	PVCName    string `json:"pvcName,omitempty"`
	VolumeMode string `json:"volumeMode"` // "Block" or "Filesystem"
	// BytesDone is the number of bytes written so far (block mode only; 0 for filesystem).
	BytesDone int64 `json:"bytesDone,omitempty"`
	// BytesTotal is the total size in bytes (block mode; may be 0 if unknown).
	BytesTotal int64 `json:"bytesTotal,omitempty"`
	Complete   bool  `json:"complete"`
}

// ArchiveIdentity is the minimal set of fields used to recognise whether an
// existing archive covers the same download target as a new invocation.
// UID is intentionally excluded so that a re-created snapshot is treated as
// the same target with updated content rather than a different one.
type ArchiveIdentity struct {
	Namespace       string        `json:"namespace"`
	Snapshot        string        `json:"snapshot"`
	Mode            SelectionMode `json:"mode"`
	RootNodeID      string        `json:"rootNodeId"`
	ObjectFilter    string        `json:"objectFilter,omitempty"`
	SelectedNodeIDs []string      `json:"selectedNodeIds"`
}

// IdentityOf extracts an ArchiveIdentity from a Meta.
// SelectedNodeIDs are sorted so that Equal is order-independent.
func IdentityOf(m Meta) ArchiveIdentity {
	ids := append([]string(nil), m.Selection.SelectedNodeIDs...)
	sort.Strings(ids)

	return ArchiveIdentity{
		Namespace:       m.Source.Namespace,
		Snapshot:        m.Source.RootSnapshot.Name,
		Mode:            m.Selection.Mode,
		RootNodeID:      m.Selection.RootNodeID,
		ObjectFilter:    m.Selection.ObjectFilter,
		SelectedNodeIDs: ids,
	}
}

// Equal reports whether two identities cover the same snapshot selection.
func (a ArchiveIdentity) Equal(b ArchiveIdentity) bool {
	if a.Namespace != b.Namespace ||
		a.Snapshot != b.Snapshot ||
		a.Mode != b.Mode ||
		a.RootNodeID != b.RootNodeID ||
		a.ObjectFilter != b.ObjectFilter ||
		len(a.SelectedNodeIDs) != len(b.SelectedNodeIDs) {
		return false
	}

	for i := range a.SelectedNodeIDs {
		if a.SelectedNodeIDs[i] != b.SelectedNodeIDs[i] {
			return false
		}
	}

	return true
}
