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

// Package upload implements the `d8 snapshot upload` sub-command.
package upload

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	snapshotlog "github.com/deckhouse/deckhouse-cli/internal/snapshot/log"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/restore"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdLong = `Upload a local snapshot archive to the cluster as a durable Ready Snapshot.

The command re-creates the full snapshot tree (Snapshot + SnapshotContent +
ManifestCheckpoint + VolumeSnapshotContent) from a local archive produced by
"d8 snapshot download", without running the source workloads.  Once complete,
the snapshot appears in "d8 snapshot list" as Ready.

Flow:
  1. Verify archive capabilities (UploadableAsSnapshot=true, COMPLETE sentinel).
  2. For each data node: upload volume bytes via DataImport → staging PVC.
  3. Create SnapshotImportManifestChunk objects (manifest transport).
  4. Create a SnapshotImportRequest — the state-snapshotter controller assembles
     the full tree and lights up Ready via the existing SnapshotContentController.
  5. Wait for the root Snapshot to become Ready.`

	cmdExample = `  # Upload snapshot from a local archive, naming the result "my-snap" in the default namespace
  d8 snapshot upload my-snap --archive ./ns-snap

  # Upload into a specific namespace
  d8 snapshot upload my-snap --archive ./ns-snap -n production

  # Apply a storage-class mapping (override archive storage class)
  d8 snapshot upload my-snap --archive ./ns-snap --storage-class-mapping fast=ultra-fast

  # Set a custom retention TTL on the resulting snapshot
  d8 snapshot upload my-snap --archive ./ns-snap --ttl 168h

  # Upload from an archive without a COMPLETE sentinel
  d8 snapshot upload my-snap --archive ./ns-snap --allow-incomplete`

	// maxChunkObjects is the maximum number of objects per SnapshotImportManifestChunk.
	maxChunkObjects = 100
	// pollInterval is the polling interval while waiting for Ready.
	pollInterval = 3 * time.Second
	// defaultUploadTTL is the TTL passed to DataImport during volume upload.
	defaultUploadTTL = "60m"
)

const (
	snapshotGroup   = "storage.deckhouse.io"
	snapshotVersion = "v1alpha1"
	snapshotKind    = "Snapshot"
)

// NewCommand returns the cobra command for "d8 snapshot upload".
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "upload <name>",
		Short:   "Upload a local snapshot archive as a durable Snapshot on the cluster",
		Long:    cmdLong,
		Example: cmdExample,
		Args:    cobra.ExactArgs(1),
		RunE:    run,
	}

	cmd.Flags().StringP("namespace", "n", "", "target namespace (default: current kubeconfig namespace)")
	cmd.Flags().String("archive", "", "path to a local archive directory created by `d8 snapshot download` (required)")
	_ = cmd.MarkFlagRequired("archive")
	cmd.Flags().StringArray("storage-class-mapping", nil, "storage-class override: <from>=<to> (repeatable)")
	cmd.Flags().String("ttl", "720h", "retention TTL for the root ObjectKeeper (e.g. 168h, 30d)")
	cmd.Flags().Bool("allow-incomplete", false, "allow uploading from an archive without a COMPLETE sentinel")

	return cmd
}

func run(cmd *cobra.Command, args []string) error {
	snapshotName := args[0]

	targetNS, _ := cmd.Flags().GetString("namespace")
	if targetNS == "" {
		targetNS = safeClient.DefaultNamespace()
	}

	archiveDir, _ := cmd.Flags().GetString("archive")
	scMappingRaw, _ := cmd.Flags().GetStringArray("storage-class-mapping")
	ttl, _ := cmd.Flags().GetString("ttl")
	allowIncomplete, _ := cmd.Flags().GetBool("allow-incomplete")

	scMapping, err := parseStorageClassMapping(scMappingRaw)
	if err != nil {
		return fmt.Errorf("--storage-class-mapping: %w", err)
	}

	log := snapshotlog.New()

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	errW := cmd.ErrOrStderr()

	// ── Step 1: verify archive ──────────────────────────────────────────────
	fmt.Fprintf(errW, "Opening archive %s …\n", archiveDir)

	reader, err := archive.OpenDir(archiveDir)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}

	idx, err := reader.Index()
	if err != nil {
		return fmt.Errorf("read archive index: %w", err)
	}

	if !allowIncomplete && !idx.Capabilities.UploadableAsSnapshot {
		return fmt.Errorf("archive does not support snapshot upload (UploadableAsSnapshot=false); " +
			"re-download with a version that supports upload or use --allow-incomplete to override")
	}

	if !allowIncomplete && !archive.IsComplete(archiveDir) {
		return fmt.Errorf("archive is incomplete (no COMPLETE sentinel); use --allow-incomplete to override")
	}

	nodes, err := reader.Nodes()
	if err != nil {
		return fmt.Errorf("read archive nodes: %w", err)
	}
	if len(nodes) == 0 {
		return fmt.Errorf("archive contains no nodes")
	}

	// ── Build per-node manifest blobs map ─────────────────────────────────
	// manifestsByNode maps nodeID → []raw-JSON-blob-per-object
	manifestsByNode := make(map[string][]json.RawMessage, len(nodes))
	if err := reader.ForEachObject(func(rec archive.ObjectRecord) error {
		blob, err := reader.ReadObjectBlob(rec)
		if err != nil {
			return fmt.Errorf("read blob for %s/%s/%s: %w", rec.APIVersion, rec.Kind, rec.Name, err)
		}
		manifestsByNode[rec.NodeID] = append(manifestsByNode[rec.NodeID], json.RawMessage(blob))
		return nil
	}); err != nil {
		return fmt.Errorf("read manifest blobs: %w", err)
	}

	// ── Step 2: build Kubernetes clients ──────────────────────────────────
	safeClient.SupportNoAuth = false

	sClient, err := safeClient.NewSafeClient(cmd.PersistentFlags())
	if err != nil {
		return fmt.Errorf("build Kubernetes client: %w", err)
	}

	rtClient, err := sClient.NewRTClient()
	if err != nil {
		return fmt.Errorf("build runtime client: %w", err)
	}

	// ── Step 3: upload volumes ─────────────────────────────────────────────

	// stagingPVCByNode maps nodeID → staging PVC name (set after DataImport completes).
	stagingPVCByNode := make(map[string]string)

	// Build VolumeOps for data nodes.
	opts := restore.Options{
		TargetNamespace: targetNS,
		Mode:            restore.ModeDataOnly,
		AllowIncomplete: true,
	}

	plan, err := restore.Build(archiveDir, opts)
	if err != nil {
		return fmt.Errorf("build volume plan: %w", err)
	}

	if len(plan.Volumes) > 0 {
		fmt.Fprintf(errW, "\nPhase 1: uploading %d volume(s) via DataImport …\n", len(plan.Volumes))

		volRestorer := &restore.VolumeRestorer{
			SafeClient: sClient,
			TTL:        defaultUploadTTL,
			Log:        log,
		}

		// Apply storage-class mapping.
		for i := range plan.Volumes {
			if plan.Volumes[i].PVCSpec != nil {
				if mapped, ok := scMapping[plan.Volumes[i].PVCSpec.StorageClassName]; ok {
					plan.Volumes[i].PVCSpec.StorageClassName = mapped
				}
			}
		}

		for j, vol := range plan.Volumes {
			fmt.Fprintf(errW, "  [%d/%d] %s (PVC: %s, mode: %s)\n",
				j+1, len(plan.Volumes), vol.VSCName, vol.PVCName, vol.VolumeMode)

			if err := volRestorer.Restore(ctx, vol, targetNS); err != nil {
				return fmt.Errorf("upload volume %s: %w", vol.VSCName, err)
			}

			// Resolve the staging PVC that DataImport created. The name is
			// deterministically derived from the DataImport PVC template, but we
			// do a live lookup to confirm existence and get the exact name.
			resolvedPVC, err := resolveStagingPVC(ctx, rtClient, vol, targetNS)
			if err != nil {
				return fmt.Errorf("resolve staging PVC for volume %s: %w", vol.VSCName, err)
			}

			stagingPVCByNode[vol.NodeID] = resolvedPVC
			fmt.Fprintf(errW, "    → staging PVC: %s\n", resolvedPVC)
		}
	}

	// ── Step 4: create manifest chunks ────────────────────────────────────
	fmt.Fprintf(errW, "\nPhase 2: uploading manifests …\n")

	sirName := importRequestName(snapshotName)

	for _, node := range nodes {
		blobs := manifestsByNode[node.ID]
		if len(blobs) == 0 {
			continue
		}

		chunks := splitBlobsIntoChunks(blobs, maxChunkObjects)
		for i, chunk := range chunks {
			data, err := gzipJSONArray(chunk)
			if err != nil {
				return fmt.Errorf("encode manifest chunk %d for node %s: %w", i, node.ID, err)
			}

			chunkName := fmt.Sprintf("%s-%s-%d", sirName, sanitize(node.ID), i)
			chunkObj := buildManifestChunkObject(chunkName, targetNS, sirName, node.ID, i, len(chunks), data, len(chunk))
			if err := rtClient.Create(ctx, chunkObj); err != nil {
				return fmt.Errorf("create manifest chunk %s: %w", chunkName, err)
			}

			fmt.Fprintf(errW, "  node %-25s chunk %d/%d (%d objects)\n",
				node.ID, i+1, len(chunks), len(chunk))
		}
	}

	// ── Step 5: create SnapshotImportRequest ──────────────────────────────
	fmt.Fprintf(errW, "\nPhase 3: creating SnapshotImportRequest …\n")

	importNodes := buildImportNodes(nodes)
	importVolumes := buildImportVolumes(plan.Volumes, stagingPVCByNode)

	sirObj := buildImportRequest(sirName, targetNS, snapshotName, importNodes, importVolumes, scMapping, ttl)
	if err := rtClient.Create(ctx, sirObj); err != nil {
		return fmt.Errorf("create SnapshotImportRequest %s: %w", sirName, err)
	}

	fmt.Fprintf(errW, "  Created SnapshotImportRequest %s/%s\n", targetNS, sirName)

	// ── Step 6: wait for Ready ─────────────────────────────────────────────
	fmt.Fprintf(errW, "\nPhase 4: waiting for Snapshot %s/%s to become Ready …\n", targetNS, snapshotName)

	if err := waitForSnapshotReady(ctx, rtClient, snapshotName, targetNS, errW); err != nil {
		return fmt.Errorf("wait for Snapshot Ready: %w", err)
	}

	fmt.Fprintf(errW, "\nUpload complete.  Snapshot %s/%s is Ready.\n", targetNS, snapshotName)

	return nil
}

// stagingPVCName returns the deterministic staging PVC name that `restore.buildPVCTemplate` uses.
// This must match the logic in `restore.buildPVCTemplate` so we can tell the import controller
// which PVC to capture.
func stagingPVCName(op restore.VolumeOp) string {
	if op.PVCSpec != nil && op.PVCSpec.Name != "" {
		return op.PVCSpec.Name
	}
	if op.PVCName != "" {
		return op.PVCName
	}
	return "restore-" + op.VSCName
}

// splitBlobsIntoChunks splits a flat slice of blobs into sub-slices of at most size n.
func splitBlobsIntoChunks(blobs []json.RawMessage, n int) [][]json.RawMessage {
	var chunks [][]json.RawMessage
	for i := 0; i < len(blobs); i += n {
		end := i + n
		if end > len(blobs) {
			end = len(blobs)
		}
		chunks = append(chunks, blobs[i:end])
	}
	return chunks
}

// gzipJSONArray encodes the slice of raw JSON messages as a gzip-compressed JSON array,
// base64-encoded. Format: base64(gzip(json[])).
func gzipJSONArray(blobs []json.RawMessage) (string, error) {
	// Encode as JSON array.
	raw, err := json.Marshal(blobs)
	if err != nil {
		return "", fmt.Errorf("marshal JSON array: %w", err)
	}

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(raw); err != nil {
		return "", fmt.Errorf("gzip write: %w", err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("gzip close: %w", err)
	}

	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// buildManifestChunkObject constructs an unstructured SnapshotImportManifestChunk.
func buildManifestChunkObject(name, namespace, sirName, nodeID string, index, total int, data string, objCount int) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
			"kind":       "SnapshotImportManifestChunk",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"importRequestName": sirName,
				"nodeId":            nodeID,
				"index":             int64(index),
				"total":             int64(total),
				"data":              data,
				"objectsCount":      int64(objCount),
			},
		},
	}
	return obj
}

// buildImportNodes converts archive NodeRecords into import request nodes.
func buildImportNodes(nodes []archive.NodeRecord) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(nodes))
	for _, n := range nodes {
		children := make([]interface{}, 0, len(n.Children))
		for _, c := range n.Children {
			children = append(children, c)
		}
		result = append(result, map[string]interface{}{
			"id":         n.ID,
			"apiVersion": n.APIVersion,
			"kind":       n.Kind,
			"name":       n.Name,
			"parentId":   n.ParentID,
			"children":   children,
			"hasData":    n.HasData,
		})
	}
	return result
}

// buildImportVolumes constructs the volumes slice for the SnapshotImportRequest spec.
func buildImportVolumes(vols []restore.VolumeOp, stagingPVCByNode map[string]string) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(vols))
	for _, v := range vols {
		stagingPVC := stagingPVCByNode[v.NodeID]
		if stagingPVC == "" {
			stagingPVC = stagingPVCName(v)
		}
		result = append(result, map[string]interface{}{
			"nodeId":         v.NodeID,
			"pvcName":        v.PVCName,
			"volumeMode":     v.VolumeMode,
			"stagingPVCName": stagingPVC,
		})
	}
	return result
}

// buildImportRequest constructs the SnapshotImportRequest unstructured object.
func buildImportRequest(
	sirName, namespace, rootSnapshotName string,
	nodes []map[string]interface{},
	volumes []map[string]interface{},
	scMapping map[string]string,
	ttl string,
) *unstructured.Unstructured {
	nodeSlice := make([]interface{}, 0, len(nodes))
	for _, n := range nodes {
		nodeSlice = append(nodeSlice, n)
	}
	volSlice := make([]interface{}, 0, len(volumes))
	for _, v := range volumes {
		volSlice = append(volSlice, v)
	}

	scMapInterface := map[string]interface{}{}
	for k, v := range scMapping {
		scMapInterface[k] = v
	}

	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
			"kind":       "SnapshotImportRequest",
			"metadata": map[string]interface{}{
				"name":      sirName,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"rootSnapshotName":    rootSnapshotName,
				"nodes":               nodeSlice,
				"volumes":             volSlice,
				"storageClassMapping": scMapInterface,
				"ttl":                 ttl,
			},
		},
	}
}

// resolveStagingPVC returns the name of the PVC created by the DataImport for the given
// VolumeOp. The PVC name is deterministically derived from the DataImport template, so we
// compute it locally and confirm the PVC exists on the cluster as a sanity check. If it
// does not yet exist (race after WaitUploadCompleted), a short retry loop is used.
func resolveStagingPVC(ctx context.Context, rtClient ctrlrtclient.Client, op restore.VolumeOp, namespace string) (string, error) {
	derived := stagingPVCName(op)
	key := types.NamespacedName{Namespace: namespace, Name: derived}

	pvc := &unstructured.Unstructured{}
	pvc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "PersistentVolumeClaim",
	})

	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := rtClient.Get(ctx, key, pvc); err == nil {
			return derived, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("PVC %s/%s not found after DataImport completed", namespace, derived)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// waitForSnapshotReady polls until the root Snapshot has Ready=True or the context is done.
func waitForSnapshotReady(ctx context.Context, rtClient ctrlrtclient.Client, name, namespace string, w io.Writer) error {
	key := types.NamespacedName{Namespace: namespace, Name: name}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}

		snap := &unstructured.Unstructured{}
		snap.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   snapshotGroup,
			Version: snapshotVersion,
			Kind:    snapshotKind,
		})
		if err := rtClient.Get(ctx, key, snap); err != nil {
			fmt.Fprintf(w, "  waiting for Snapshot (not found yet) …\n")
			continue
		}

		conds, ok, err := unstructured.NestedSlice(snap.Object, "status", "conditions")
		if err != nil || !ok {
			fmt.Fprintf(w, "  waiting for Snapshot conditions …\n")
			continue
		}

		for _, c := range conds {
			cond, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			if cond["type"] != "Ready" {
				continue
			}
			status, _ := cond["status"].(string)
			reason, _ := cond["reason"].(string)
			msg, _ := cond["message"].(string)

			if status == string(metav1.ConditionTrue) {
				return nil
			}

			fmt.Fprintf(w, "  Snapshot Ready=%s reason=%s: %s\n", status, reason, msg)
		}
	}
}

// importRequestName derives a stable SnapshotImportRequest name from the snapshot name.
func importRequestName(snapshotName string) string {
	name := "sir-" + sanitize(snapshotName)
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}

// sanitize replaces non-DNS-label characters with hyphens.
func sanitize(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	return b.String()
}

// parseStorageClassMapping parses `--storage-class-mapping old=new` flag values.
func parseStorageClassMapping(raw []string) (map[string]string, error) {
	m := make(map[string]string, len(raw))
	for _, entry := range raw {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid mapping %q: expected <from>=<to>", entry)
		}
		m[parts[0]] = parts[1]
	}
	return m, nil
}
