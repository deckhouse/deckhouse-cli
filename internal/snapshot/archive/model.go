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
	"sort"
	"time"
)

const (
	Magic         = "deckhouse.snapshot.archive"
	SchemaVersion = "v1alpha1"
)

type SelectionMode string

const (
	SelectionFull    SelectionMode = "full"
	SelectionSubtree SelectionMode = "subtree"
	SelectionObject  SelectionMode = "object"
)

type Meta struct {
	Magic         string    `json:"magic"`
	SchemaVersion string    `json:"schemaVersion"`
	ArchiveID     string    `json:"archiveId"`
	CreatedAt     time.Time `json:"createdAt"`
	CreatedBy     Creator   `json:"createdBy"`
	Source        Source    `json:"source"`
	Selection     Selection `json:"selection"`
}

type Creator struct {
	Tool    string `json:"tool"`
	Version string `json:"version"`
}

type Source struct {
	Cluster             Cluster            `json:"cluster"`
	Namespace           string             `json:"namespace"`
	RootSnapshot        SnapshotRef        `json:"rootSnapshot"`
	RootSnapshotContent SnapshotContentRef `json:"rootSnapshotContent"`
}

type Cluster struct {
	Server string `json:"server"`
	UID    string `json:"uid,omitempty"`
}

type SnapshotRef struct {
	APIVersion      string `json:"apiVersion"`
	Kind            string `json:"kind"`
	Resource        string `json:"resource"`
	Name            string `json:"name"`
	UID             string `json:"uid,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

type SnapshotContentRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

type Selection struct {
	Mode       SelectionMode `json:"mode"`
	RootNodeID string        `json:"rootNodeId"`

	ObjectFilter    string   `json:"objectFilter,omitempty"`
	SelectedNodeIDs []string `json:"selectedNodeIds"`
}

type Index struct {
	SchemaVersion string             `json:"schemaVersion"`
	Capabilities  IndexCapabilities  `json:"capabilities"`
	ManifestModel IndexManifestModel `json:"manifestModel"`
	VolumeModel   IndexVolumeModel   `json:"volumeModel"`
	Catalogs      IndexCatalogs      `json:"catalogs"`
	Paths         IndexPaths         `json:"paths"`
	Summary       IndexSummary       `json:"summary"`
}

type IndexCapabilities struct {
	Manifests            bool `json:"manifests"`
	Volumes              bool `json:"volumes"`
	RestoreFromArchive   bool `json:"restoreFromArchive"`
	UploadableAsSnapshot bool `json:"uploadableAsSnapshot"`
	PartialSelection     bool `json:"partialSelection"`
	Resumable            bool `json:"resumable"`
}

type IndexManifestModel struct {
	Format      string `json:"format"`
	Compression string `json:"compression"`
	SourceKind  string `json:"sourceKind"`
}

type IndexVolumeModel struct {
	Format      string `json:"format"`
	Compression string `json:"compression"`
}

type IndexCatalogs struct {
	Nodes   string `json:"nodes"`
	Objects string `json:"objects"`
}

type IndexPaths struct {
	ManifestsRoot string `json:"manifestsRoot"`
	DataRoot      string `json:"dataRoot"`
}

type IndexSummary struct {
	Nodes    int  `json:"nodes"`
	Objects  int  `json:"objects"`
	Volumes  int  `json:"volumes"`
	Complete bool `json:"complete"`
}

type VolumeDataRef struct {
	VSCName      string `json:"vscName"`
	PVCName      string `json:"pvcName,omitempty"`
	PVCNamespace string `json:"pvcNamespace,omitempty"`
}

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

// It is written durably after a node's blobs are flushed and fsync'd.
// Finalize regenerates nodes.jsonl and objects.jsonl from the accumulated records.
type ProgressRecord struct {
	NodeID     string         `json:"nodeId"`
	ContentRef string         `json:"contentRef"` // boundSnapshotContentName at download time
	Objects    []ObjectRecord `json:"objects"`
}

// Key for resume: NodeID + "/" + VSCName.
type VolumeProgressRecord struct {
	NodeID     string `json:"nodeId"`
	VSCName    string `json:"vscName"`
	PVCName    string `json:"pvcName,omitempty"`
	VolumeMode string `json:"volumeMode"` // "Block" or "Filesystem"
	// Compression is "gzip" or "none". Empty means "none" for backwards compat.
	Compression string `json:"compression,omitempty"`
	// BytesDone is the number of uncompressed source bytes downloaded so far
	// (block mode only; 0 for filesystem).
	BytesDone int64 `json:"bytesDone,omitempty"`
	// BytesTotal is the total uncompressed size in bytes (block mode; may be 0 if unknown).
	BytesTotal int64 `json:"bytesTotal,omitempty"`
	// CompressedBytes is the number of compressed bytes durably written to disk
	// (block gzip mode only). Used to truncate a half-written trailing gzip member
	// on resume. 0 for filesystem or raw block.
	CompressedBytes int64 `json:"compressedBytes,omitempty"`
	Complete        bool  `json:"complete"`
}

// UID is intentionally excluded so that a re-created snapshot is treated as
// the same target with updated content rather than a different one.
type Identity struct {
	Namespace       string        `json:"namespace"`
	Snapshot        string        `json:"snapshot"`
	Mode            SelectionMode `json:"mode"`
	RootNodeID      string        `json:"rootNodeId"`
	ObjectFilter    string        `json:"objectFilter,omitempty"`
	SelectedNodeIDs []string      `json:"selectedNodeIds"`
}

// SelectedNodeIDs are sorted so that Equal is order-independent.
func IdentityOf(m Meta) Identity {
	ids := append([]string(nil), m.Selection.SelectedNodeIDs...)
	sort.Strings(ids)

	return Identity{
		Namespace:       m.Source.Namespace,
		Snapshot:        m.Source.RootSnapshot.Name,
		Mode:            m.Selection.Mode,
		RootNodeID:      m.Selection.RootNodeID,
		ObjectFilter:    m.Selection.ObjectFilter,
		SelectedNodeIDs: ids,
	}
}

func (a Identity) Equal(b Identity) bool {
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
