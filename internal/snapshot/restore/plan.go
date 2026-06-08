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

package restore

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// Mode controls which parts of the archive are restored.
type Mode int

const (
	// ModeAll restores both manifests and volume data (default).
	ModeAll Mode = iota
	// ModeManifestsOnly restores only Kubernetes manifests; volume data is skipped.
	ModeManifestsOnly
	// ModeDataOnly restores only volume data via DataImport; no manifests are applied.
	ModeDataOnly
)

// Options holds all restore settings.
type Options struct {
	// TargetNamespace is the namespace where namespaced objects are applied.
	TargetNamespace string
	// NodeFilter, when non-empty, restricts restore to the subtree rooted at this node ID.
	NodeFilter string
	// ObjectFilter restricts manifest restore to a single object: "apiVersion/Kind/name".
	ObjectFilter string
	// Mode controls what is restored (all, manifests-only, data-only).
	Mode Mode
	// DryRun causes the command to stop after the dry-run phase; nothing is applied.
	DryRun bool
	// NoEdit disables the editor on conflict; conflicts cause an immediate error.
	NoEdit bool
	// ForceConflicts applies manifests with --force-conflicts (steals field ownership).
	ForceConflicts bool
	// AllowIncomplete allows restoring from an archive that has no COMPLETE sentinel.
	AllowIncomplete bool
	// DataImportTTL is the TTL passed to created DataImport objects.
	DataImportTTL string
	// FieldManager is the SSA field manager name.
	FieldManager string
}

// ManifestOp is one Kubernetes object to be applied via SSA.
type ManifestOp struct {
	NodeID     string
	APIVersion string
	Kind       string
	Name       string
	Namespace  string // original namespace captured in archive
	Data       []byte // raw JSON
}

// VolumeOp describes one volume (PVC) to restore via DataImport + upload.
type VolumeOp struct {
	NodeID      string
	VSCName     string // VolumeSnapshotContent name (archive key)
	PVCName     string // target PVC name (may be empty for VD-backed)
	VolumeMode  string // "Block" or "Filesystem"
	Compression string // "gzip" or "none"
	// DataPath is the absolute path to the block image file or filesystem directory.
	DataPath string
	// PVCSpec is extracted from the captured PVC manifest; may be nil for non-PVC volumes.
	PVCSpec    *PVCSpec
	BytesTotal int64 // block only; 0 means unknown
}

// PVCSpec holds the fields from a captured PVC manifest needed to build a DataImport.
type PVCSpec struct {
	Name             string
	AccessModes      []string
	StorageClassName string
	VolumeMode       string
	StorageRequest   string
}

// RestorePlan is the output of Build: the ordered set of operations to perform.
type RestorePlan struct {
	ArchiveDir string
	Meta       archive.Meta
	Opts       Options
	Manifests  []ManifestOp
	Volumes    []VolumeOp
}

// Build constructs a RestorePlan from a local archive directory and options.
//
// Manifests and volumes from the archive are filtered by Options.NodeFilter and
// Options.ObjectFilter. PVCs that have corresponding volume data in the archive
// (Mode=All) are excluded from Manifests and moved to Volumes instead — the
// DataImport creates the PVC and populates it.
func Build(archiveDir string, opts Options) (*RestorePlan, error) {
	reader, err := archive.OpenDir(archiveDir)
	if err != nil {
		return nil, fmt.Errorf("open archive: %w", err)
	}

	if !opts.AllowIncomplete && !archive.IsComplete(archiveDir) {
		return nil, fmt.Errorf(
			"archive %q is incomplete (no COMPLETE file); use --allow-incomplete to restore anyway",
			archiveDir,
		)
	}

	meta, err := reader.Meta()
	if err != nil {
		return nil, fmt.Errorf("read archive metadata: %w", err)
	}

	nodes, err := reader.Nodes()
	if err != nil {
		return nil, fmt.Errorf("read node index: %w", err)
	}

	volProgress, err := reader.VolumeProgress()
	if err != nil {
		return nil, fmt.Errorf("read volume progress: %w", err)
	}

	// Apply node filter (subtree selection).
	selectedNodes := selectNodes(nodes, opts.NodeFilter)
	if opts.NodeFilter != "" && len(selectedNodes) == 0 {
		return nil, fmt.Errorf("node %q not found in archive; run `d8 snapshot tree --archive %s` to see available nodes",
			opts.NodeFilter, archiveDir)
	}

	selectedNodeSet := make(map[string]archive.NodeRecord, len(selectedNodes))
	for _, n := range selectedNodes {
		selectedNodeSet[n.ID] = n
	}

	// Parse object filter: <apiVersion>/<Kind>/<name>, e.g. "apps/v1/Deployment/my-app".
	// Split from the right so that group-scoped APIVersions ("apps/v1") are handled correctly.
	var filterAPIVersion, filterKind, filterName string
	if opts.ObjectFilter != "" {
		parts := strings.Split(opts.ObjectFilter, "/")
		if len(parts) < 3 {
			return nil, fmt.Errorf("--object must be <apiVersion>/<Kind>/<name>, got %q", opts.ObjectFilter)
		}
		filterName = parts[len(parts)-1]
		filterKind = parts[len(parts)-2]
		filterAPIVersion = strings.Join(parts[:len(parts)-2], "/")
	}

	// Pass 1: collect all objects into allOps and store PVC blobs for VolumeOp spec building.
	// pvcByNodeAndName stores captured PVC manifests keyed by "nodeID/pvcName".
	pvcByNodeAndName := make(map[string][]byte)

	type rawOp struct {
		rec  archive.ObjectRecord
		data []byte
	}

	var allOps []rawOp

	err = reader.ForEachObject(func(rec archive.ObjectRecord) error {
		if _, ok := selectedNodeSet[rec.NodeID]; !ok {
			return nil
		}

		// In DataOnly mode, only read PVC blobs (needed for VolumeOp spec).
		if opts.Mode == ModeDataOnly && rec.Kind != "PersistentVolumeClaim" {
			return nil
		}

		// Apply object filter.
		if opts.ObjectFilter != "" {
			if rec.APIVersion != filterAPIVersion || rec.Kind != filterKind || rec.Name != filterName {
				return nil
			}
		}

		data, blobErr := reader.ReadObjectBlob(rec)
		if blobErr != nil {
			return fmt.Errorf("read blob for %s/%s %s: %w", rec.APIVersion, rec.Kind, rec.Name, blobErr)
		}

		// Always store PVC manifests — needed to build VolumeOp.PVCSpec.
		if rec.Kind == "PersistentVolumeClaim" {
			pvcByNodeAndName[rec.NodeID+"/"+rec.Name] = data
		}

		allOps = append(allOps, rawOp{rec: rec, data: data})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("iterate objects: %w", err)
	}

	// Pass 2: build VolumeOps first so we know which PVCs are covered by DataImport.
	// VolumeOps are the ground truth — derived directly from nodes+volProgress.
	var volumeOps []VolumeOp
	if opts.Mode != ModeManifestsOnly {
		for _, node := range selectedNodes {
			if !node.HasData {
				continue
			}
			for _, dr := range node.DataRefs {
				vpKey := archive.VolumeProgressKey(node.ID, dr.VSCName)
				vpRec, ok := volProgress[vpKey]
				if !ok || !vpRec.Complete {
					continue
				}

				dataPath := volumeDataPath(archiveDir, node.ID, dr, vpRec)
				if dataPath == "" {
					continue
				}

				compression := vpRec.Compression
				if compression == "" {
					compression = "gzip"
				}

				pvcSpec := buildPVCSpec(pvcByNodeAndName, node.ID, dr.PVCName, dr.VSCName, vpRec)

				volumeOps = append(volumeOps, VolumeOp{
					NodeID:      node.ID,
					VSCName:     dr.VSCName,
					PVCName:     dr.PVCName,
					VolumeMode:  vpRec.VolumeMode,
					Compression: compression,
					DataPath:    dataPath,
					PVCSpec:     pvcSpec,
					BytesTotal:  vpRec.BytesTotal,
				})
			}
		}
	}

	// Build the exclusion set from volumeOps (the ground truth).
	// PVCs in this set will NOT be applied as manifests — DataImport provisions them.
	pvcHandledByDataImport := make(map[string]bool, len(volumeOps))
	for _, vol := range volumeOps {
		if vol.PVCName != "" {
			pvcHandledByDataImport[vol.NodeID+"/"+vol.PVCName] = true
		}
	}

	// Pass 3: build manifestOps from the collected objects, skipping PVCs
	// that will be handled by DataImport.
	var manifestOps []ManifestOp
	if opts.Mode != ModeDataOnly {
		for _, op := range allOps {
			rec := op.rec
			data := op.data

			// In ModeAll, PVCs with data are provisioned by DataImport — skip them here.
			if opts.Mode == ModeAll && rec.Kind == "PersistentVolumeClaim" {
				if pvcHandledByDataImport[rec.NodeID+"/"+rec.Name] {
					continue
				}
				// PVC without data: strip binding fields to avoid "Lost" state in the new cluster.
				data = stripPVCBindingFields(data)
			}

			manifestOps = append(manifestOps, ManifestOp{
				NodeID:     rec.NodeID,
				APIVersion: rec.APIVersion,
				Kind:       rec.Kind,
				Name:       rec.Name,
				Namespace:  rec.Namespace,
				Data:       data,
			})
		}
	}

	// Sort manifests so that prerequisite object kinds are applied first.
	sort.SliceStable(manifestOps, func(i, j int) bool {
		return kindOrder(manifestOps[i].Kind) < kindOrder(manifestOps[j].Kind)
	})

	return &RestorePlan{
		ArchiveDir: archiveDir,
		Meta:       meta,
		Opts:       opts,
		Manifests:  manifestOps,
		Volumes:    volumeOps,
	}, nil
}

// volumeDataPath returns the absolute path to the volume data file or directory.
func volumeDataPath(archiveDir, nodeID string, dr archive.VolumeDataRef, vpRec archive.VolumeProgressRecord) string {
	switch vpRec.VolumeMode {
	case "Block":
		if vpRec.Compression == "gzip" || vpRec.Compression == "" {
			return filepath.Join(archiveDir, "data", nodeID, dr.VSCName+".img.gz")
		}
		return filepath.Join(archiveDir, "data", nodeID, dr.VSCName+".img")
	case "Filesystem":
		dirName := dr.PVCName
		if dirName == "" {
			dirName = dr.VSCName
		}
		return filepath.Join(archiveDir, "data", nodeID, dirName)
	default:
		return ""
	}
}

// buildPVCSpec extracts PVC spec fields from the captured manifest, falling
// back to sensible defaults when the manifest is absent.
func buildPVCSpec(
	pvcByNodeAndName map[string][]byte,
	nodeID, pvcName, vscName string,
	vpRec archive.VolumeProgressRecord,
) *PVCSpec {
	// Choose PVC name: prefer original PVC name, else derive from VSC.
	name := pvcName
	if name == "" {
		name = "restore-" + vscName
	}

	if pvcName != "" {
		if data, ok := pvcByNodeAndName[nodeID+"/"+pvcName]; ok {
			spec := extractPVCSpec(data)
			spec.Name = name
			return spec
		}
	}

	// Fallback when no captured PVC manifest is available.
	return &PVCSpec{
		Name:           name,
		AccessModes:    []string{"ReadWriteOnce"},
		VolumeMode:     vpRec.VolumeMode,
		StorageRequest: BytesToStorage(vpRec.BytesTotal),
	}
}

// extractPVCSpec parses the storage fields from a captured PVC JSON manifest.
func extractPVCSpec(data []byte) *PVCSpec {
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		return &PVCSpec{AccessModes: []string{"ReadWriteOnce"}}
	}

	spec := &PVCSpec{}

	specMap, _ := obj["spec"].(map[string]any)
	if specMap == nil {
		spec.AccessModes = []string{"ReadWriteOnce"}
		return spec
	}

	if modes, ok := specMap["accessModes"].([]any); ok {
		for _, m := range modes {
			if s, ok := m.(string); ok {
				spec.AccessModes = append(spec.AccessModes, s)
			}
		}
	}
	if len(spec.AccessModes) == 0 {
		spec.AccessModes = []string{"ReadWriteOnce"}
	}

	spec.StorageClassName, _ = specMap["storageClassName"].(string)
	spec.VolumeMode, _ = specMap["volumeMode"].(string)

	if resources, ok := specMap["resources"].(map[string]any); ok {
		if requests, ok := resources["requests"].(map[string]any); ok {
			spec.StorageRequest, _ = requests["storage"].(string)
		}
	}

	return spec
}

// BytesToStorage converts an uncompressed byte count to a Kubernetes storage
// quantity string (e.g. "11Gi"), rounded up to the nearest GiB.
func BytesToStorage(bytes int64) string {
	if bytes <= 0 {
		return "10Gi"
	}

	const gib = int64(1024 * 1024 * 1024)
	gibs := (bytes + gib - 1) / gib
	if gibs == 0 {
		gibs = 1
	}
	return fmt.Sprintf("%dGi", gibs)
}

// selectNodes filters the full node list to the subtree rooted at nodeFilter.
// When nodeFilter is empty, the original list is returned unchanged.
func selectNodes(nodes []archive.NodeRecord, nodeFilter string) []archive.NodeRecord {
	if nodeFilter == "" {
		return nodes
	}

	nodeMap := make(map[string]archive.NodeRecord, len(nodes))
	for _, n := range nodes {
		nodeMap[n.ID] = n
	}

	if _, ok := nodeMap[nodeFilter]; !ok {
		return nil
	}

	var result []archive.NodeRecord
	visited := make(map[string]bool)
	queue := []string{nodeFilter}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if visited[cur] {
			continue
		}
		visited[cur] = true

		if n, ok := nodeMap[cur]; ok {
			result = append(result, n)
			queue = append(queue, n.Children...)
		}
	}

	return result
}

// kindOrder returns a numeric priority for a Kubernetes kind.
// Lower numbers are applied first so that prerequisites are ready.
var kindPriority = map[string]int{
	"Namespace":                0,
	"CustomResourceDefinition": 1,
	"ClusterRole":              2,
	"ClusterRoleBinding":       3,
	"Role":                     4,
	"RoleBinding":              4,
	"StorageClass":             5,
	"PersistentVolume":         6,
	"PersistentVolumeClaim":    10,
	"ConfigMap":                20,
	"Secret":                   20,
	"ServiceAccount":           20,
	"Service":                  30,
	"Deployment":               40,
	"StatefulSet":              40,
	"DaemonSet":                40,
	"Job":                      40,
	"CronJob":                  40,
	"VirtualMachine":           50,
	"VirtualDisk":              50,
	"VirtualDiskSnapshot":      50,
	"VirtualMachineSnapshot":   50,
}

func kindOrder(kind string) int {
	if p, ok := kindPriority[kind]; ok {
		return p
	}
	return 45
}

// bindingAnnotations are PVC annotations written by the volume binding controller
// that are cluster-specific and must be stripped when restoring to a new cluster.
var bindingAnnotations = []string{
	"pv.kubernetes.io/bind-completed",
	"pv.kubernetes.io/bound-by-controller",
	"volume.beta.kubernetes.io/storage-provisioner",
	"volume.kubernetes.io/storage-provisioner",
	"volume.kubernetes.io/selected-node",
}

// stripPVCBindingFields removes cluster-specific binding state from a PVC JSON
// manifest so that the PVC can be cleanly applied in a new cluster:
//   - spec.volumeName        (binds to a specific PV that won't exist in the target)
//   - spec.claimRef          (on PVs; harmless to strip from PVC too)
//   - binding annotations    (written by kube-controller-manager, cluster-specific)
//   - status                 (managed by the server; should not be applied)
//
// If the JSON cannot be parsed the original bytes are returned unchanged.
func stripPVCBindingFields(data []byte) []byte {
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		return data
	}

	if spec, ok := obj["spec"].(map[string]any); ok {
		delete(spec, "volumeName")
		delete(spec, "claimRef")
	}

	delete(obj, "status")

	if meta, ok := obj["metadata"].(map[string]any); ok {
		if annotations, ok := meta["annotations"].(map[string]any); ok {
			for _, key := range bindingAnnotations {
				delete(annotations, key)
			}
			if len(annotations) == 0 {
				delete(meta, "annotations")
			}
		}
	}

	cleaned, err := json.Marshal(obj)
	if err != nil {
		return data
	}

	return cleaned
}
