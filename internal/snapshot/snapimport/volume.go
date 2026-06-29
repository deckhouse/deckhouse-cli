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

package snapimport

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	dataImportKind        = "DataImport"
	volumeModeBlock       = "Block"
	conditionReady        = "Ready"
	conditionCompleted    = "Completed"
	conditionExpired      = "Expired"
	uploadBlockSubpath    = "api/v1/block"
	uploadFinishedSubpath = "api/v1/finished"
	volumeModeFilesystem  = "Filesystem"

	// The SVDM importer's CheckRequiredHeaders middleware rejects any PUT missing these
	// attribute headers. The block import handler ignores their values (it writes raw bytes
	// to the device), but they must be present and non-empty, so we send fixed defaults.
	blockAttrPermissions = "0644"
	blockAttrUID         = "0"
	blockAttrGID         = "0"
)

// dataImportGVR is the SVDM DataImport resource (storage.deckhouse.io/v1alpha1).
var dataImportGVR = schema.GroupVersionResource{Group: "storage.deckhouse.io", Version: "v1alpha1", Resource: "dataimports"}

// VolumeImporter imports a data leaf's volume bytes by creating an SVDM DataImport,
// waiting for the importer to be ready, streaming the archive bytes, finalising the
// upload, and waiting for the durable artifact to be produced. It is satisfied by
// clusterVolumeImporter and stubbed in tests.
type VolumeImporter interface {
	// DataImportName returns the deterministic DataImport name for the leaf (its own name).
	// The DataImport is created bottom-up immediately before its upload — its TTL is an idle
	// timer that starts at importer-pod start, so a freshly created importer must not sit
	// idle waiting for earlier siblings to finish.
	DataImportName(leaf PlannedNode) string
	// EnsureDataImport creates (idempotently) the DataImport for the leaf and returns its name.
	EnsureDataImport(ctx context.Context, leaf PlannedNode, namespace string) (string, error)
	// UploadVolumeData waits for the DataImport to become ready, streams the leaf's block
	// data, finalises, and waits for completion. onProgress, when non-nil, is called with
	// the number of bytes written after each chunk or file upload; nil disables progress
	// reporting and leaves upload behaviour unchanged.
	UploadVolumeData(ctx context.Context, leaf PlannedNode, diName, namespace string, onProgress func(int)) error
}

// clusterVolumeImporter is the live VolumeImporter backed by a dynamic client (DataImport
// CR lifecycle and status) and a SafeClient (authenticated HTTPS byte upload to the
// importer pod, trusting status.ca).
type clusterVolumeImporter struct {
	dyn  dynamic.Interface
	sc   *safeClient.SafeClient
	ttl  string
	poll time.Duration
	wait time.Duration
	log  *slog.Logger
	// tempDir is the directory for decompressed block-volume temporary files. When empty,
	// sendVolumeData defaults to filepath.Dir(leaf.DataFile) — next to the archive node
	// on the same filesystem as the compressed source. Override via --temp-dir.
	// Worst-case peak disk usage: Workers × (size of the largest decompressed volume).
	tempDir string
}

// NewClusterVolumeImporter builds the live VolumeImporter. ttl is the DataImport TTL,
// wait bounds the per-DataImport readiness/completion waits, poll is the polling cadence,
// and tempDir is the scratch directory for decompressed block-volume temp files (empty =
// auto-select filepath.Dir(leaf.DataFile), keeping temps on the same filesystem as the archive).
func NewClusterVolumeImporter(
	dyn dynamic.Interface,
	sc *safeClient.SafeClient,
	ttl string,
	wait, poll time.Duration,
	tempDir string,
	log *slog.Logger,
) VolumeImporter {
	if log == nil {
		log = slog.Default()
	}

	return &clusterVolumeImporter{dyn: dyn, sc: sc, ttl: ttl, poll: poll, wait: wait, tempDir: tempDir, log: log}
}

// DataImportName returns the deterministic DataImport name for the leaf (its own name).
func (c *clusterVolumeImporter) DataImportName(leaf PlannedNode) string {
	return leaf.Name
}

// EnsureDataImport upserts the Mode A DataImport targeting the leaf snapshot CR. The leaf's
// captured volume metadata (storageClassName/size/volumeMode, read back from the archive) is
// echoed into spec so the controller can provision the scratch PVC and produce the durable
// VolumeSnapshotContent artifact. The mode is identified server-side by targetRef.kind (a
// snapshot-leaf kind ⇒ Mode A); the discriminator value PersistentVolumeClaim is reserved for
// the standalone Mode B path driven by `d8 data import`.
func (c *clusterVolumeImporter) EnsureDataImport(ctx context.Context, leaf PlannedNode, namespace string) (string, error) {
	name := c.DataImportName(leaf)

	group, kind, err := leafTargetRef(leaf)
	if err != nil {
		return "", err
	}

	// storageClassName and size are mandatory for Mode A (enforced by the CRD CEL rules).
	// They originate from the captured SnapshotContent.status.dataRef and are carried through
	// the archive; a blank value means a malformed archive, so fail early with a clear message
	// rather than letting the API server reject an incomplete DataImport.
	if leaf.StorageClassName == "" || leaf.Size == "" {
		return "", fmt.Errorf("data leaf %s/%s is missing volume metadata in the archive "+
			"(storageClassName=%q, size=%q); re-download the snapshot with a current d8 version",
			leaf.Kind, leaf.Name, leaf.StorageClassName, leaf.Size)
	}

	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(dataImportGVR.GroupVersion().String())
	obj.SetKind(dataImportKind)
	obj.SetNamespace(namespace)
	obj.SetName(name)

	spec := map[string]interface{}{
		"ttl":              c.ttl,
		"storageClassName": leaf.StorageClassName,
		"size":             leaf.Size,
		"targetRef": map[string]interface{}{
			"group": group,
			"kind":  kind,
			"name":  leaf.Name,
		},
	}
	if leaf.VolumeMode != "" {
		spec["volumeMode"] = leaf.VolumeMode
	}

	if err := unstructured.SetNestedMap(obj.Object, spec, "spec"); err != nil {
		return "", fmt.Errorf("build DataImport spec: %w", err)
	}

	ri := c.dyn.Resource(dataImportGVR).Namespace(namespace)

	// Bounded reconcile loop so a create-vs-existing race converges deterministically: a
	// healthy existing import is reused; a terminally Expired one is deleted and recreated;
	// an AlreadyExists on create (lost race) loops back to re-read and re-evaluate Expired
	// rather than blindly treating the pre-existing object as good.
	for attempt := 0; attempt < 3; attempt++ {
		existing, err := ri.Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			if !conditionTrue(existing, conditionExpired) {
				// Align spec.ttl with the current run so retrying a stalled import with a
				// longer --ttl is honoured instead of keeping the first create's value.
				if tErr := c.alignDataImportTTL(ctx, ri, existing); tErr != nil {
					return "", tErr
				}

				return name, nil
			}

			c.log.Info("recreating expired DataImport", slog.String("namespace", namespace), slog.String("name", name))

			if dErr := c.deleteDataImportAndWait(ctx, ri, name); dErr != nil {
				return "", dErr
			}
		} else if !kubeerrors.IsNotFound(err) {
			return "", fmt.Errorf("get DataImport %s/%s: %w", namespace, name, err)
		}

		if _, err := ri.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
			if kubeerrors.IsAlreadyExists(err) {
				continue
			}

			return "", fmt.Errorf("create DataImport %s/%s: %w", namespace, name, err)
		}

		c.log.Info("created DataImport", slog.String("namespace", namespace), slog.String("name", name))

		return name, nil
	}

	return "", fmt.Errorf("data import %s/%s did not converge (repeated create/expire races)", namespace, name)
}

// alignDataImportTTL patches a reused DataImport's spec.ttl to the current run's TTL when it
// drifted, so increasing --ttl on a retry takes effect. No-op when already aligned.
func (c *clusterVolumeImporter) alignDataImportTTL(ctx context.Context, ri dynamic.ResourceInterface, existing *unstructured.Unstructured) error {
	cur, _, _ := unstructured.NestedString(existing.Object, "spec", "ttl")
	if cur == c.ttl {
		return nil
	}

	updated := existing.DeepCopy()
	if err := unstructured.SetNestedField(updated.Object, c.ttl, "spec", "ttl"); err != nil {
		return fmt.Errorf("set DataImport ttl: %w", err)
	}

	if _, err := ri.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("patch DataImport %s/%s ttl: %w", existing.GetNamespace(), existing.GetName(), err)
	}

	c.log.Info("aligned DataImport ttl",
		slog.String("namespace", existing.GetNamespace()), slog.String("name", existing.GetName()), slog.String("ttl", c.ttl))

	return nil
}

// deleteDataImportAndWait deletes the named DataImport and blocks until it is gone (bounded
// by the importer wait budget), so a fresh one can be created under the same name.
func (c *clusterVolumeImporter) deleteDataImportAndWait(ctx context.Context, ri dynamic.ResourceInterface, name string) error {
	if err := ri.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !kubeerrors.IsNotFound(err) {
		return fmt.Errorf("delete expired DataImport %s: %w", name, err)
	}

	deadline := time.Now().Add(c.wait)

	for {
		if _, err := ri.Get(ctx, name, metav1.GetOptions{}); kubeerrors.IsNotFound(err) {
			return nil
		} else if err != nil {
			return fmt.Errorf("await deletion of DataImport %s: %w", name, err)
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for expired DataImport %s to be deleted", name)
		}

		if !sleepCtx(ctx, c.poll) {
			return ctx.Err()
		}
	}
}

// UploadVolumeData waits for the DataImport endpoint, uploads the volume data
// (filesystem tar or raw block), finalises, and waits for the durable artifact.
func (c *clusterVolumeImporter) UploadVolumeData(ctx context.Context, leaf PlannedNode, diName, namespace string, onProgress func(int)) error {
	if !leaf.FilesystemData && !leaf.HasBlockData() {
		return fmt.Errorf("data leaf %s/%s has no volume data file in the archive", leaf.Kind, leaf.Name)
	}

	// Idempotent retry: if a prior run already produced this leaf's durable artifact, skip
	// the upload. A Completed importer typically has no live endpoint, so waiting for Ready
	// would just time out.
	done, err := c.dataImportCompleted(ctx, diName, namespace)
	if err != nil {
		return err
	}

	if done {
		c.log.Info("volume data already imported; skipping upload",
			slog.String("namespace", namespace), slog.String("dataimport", diName))

		return nil
	}

	di, err := c.waitDataImportReady(ctx, diName, namespace)
	if err != nil {
		return err
	}

	url, _, _ := unstructured.NestedString(di.Object, "status", "url")
	volumeMode, _, _ := unstructured.NestedString(di.Object, "status", "volumeMode")
	caB64, _, _ := unstructured.NestedString(di.Object, "status", "ca")

	httpClient, err := c.uploadClient(caB64)
	if err != nil {
		return err
	}

	if err := c.sendVolumeData(ctx, httpClient, url, volumeMode, leaf, namespace, diName, onProgress); err != nil {
		return err
	}

	return c.waitDataImportCompleted(ctx, diName, namespace)
}

// sendVolumeData streams the volume payload (FS tar or raw block data) to the importer
// and signals end-of-upload via postFinished. It does NOT poll for DataImport completion;
// the caller must invoke waitDataImportCompleted afterwards.
// Using leaf.TarFile (not leaf.DataFile) for the FS path is essential: DataFile holds the
// block-data glob result (data.bin*), which is always empty for FS-only leaves.
func (c *clusterVolumeImporter) sendVolumeData(ctx context.Context, httpClient httpDoer, url, volumeMode string, leaf PlannedNode, namespace, diName string, onProgress func(int)) error {
	switch volumeMode {
	case volumeModeFilesystem:
		c.log.Info("uploading filesystem data",
			slog.String("namespace", namespace),
			slog.String("dataimport", diName))

		if err := importFSFromTar(ctx, httpClient, url, leaf.TarFile, c.log, onProgress); err != nil {
			return fmt.Errorf("upload filesystem data for %s/%s: %w", namespace, diName, err)
		}

		if err := postFinished(ctx, httpClient, url); err != nil {
			return fmt.Errorf("finalise upload for %s/%s: %w", namespace, diName, err)
		}

	case volumeModeBlock:
		// Default the temp dir to the archive node directory — same filesystem as the
		// compressed source, so it always has room for at least one decompressed volume.
		effectiveTempDir := c.tempDir
		if effectiveTempDir == "" {
			effectiveTempDir = filepath.Dir(leaf.DataFile)
		}

		srcPath, size, cleanup, err := resolveBlockSource(leaf.DataFile, effectiveTempDir)
		if err != nil {
			return err
		}

		defer cleanup()

		blockURL, err := neturl.JoinPath(url, uploadBlockSubpath)
		if err != nil {
			return fmt.Errorf("build block upload URL: %w", err)
		}

		c.log.Info("uploading volume data",
			slog.String("namespace", namespace),
			slog.String("dataimport", diName),
			slog.Int64("bytes", size))

		if err := putBlock(ctx, httpClient, blockURL, srcPath, size, onProgress); err != nil {
			return fmt.Errorf("upload block data for %s/%s: %w", namespace, diName, err)
		}

		if err := postFinished(ctx, httpClient, url); err != nil {
			return fmt.Errorf("finalise upload for %s/%s: %w", namespace, diName, err)
		}

	default:
		return fmt.Errorf("data import %s/%s reports unsupported volumeMode %q", namespace, diName, volumeMode)
	}

	return nil
}

// uploadClient builds an isolated SafeClient that trusts the importer's status.ca.
func (c *clusterVolumeImporter) uploadClient(caB64 string) (*safeClient.SafeClient, error) {
	var ca []byte

	if caB64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(caB64)
		if err != nil {
			return nil, fmt.Errorf("decode DataImport status.ca: %w", err)
		}

		ca = decoded
	}

	sub := c.sc.Copy()
	sub.SetTLSCAData(ca)

	return sub, nil
}

// waitDataImportReady blocks until the DataImport reports Ready=True with a populated
// status.url and volumeMode.
func (c *clusterVolumeImporter) waitDataImportReady(ctx context.Context, name, namespace string) (*unstructured.Unstructured, error) {
	deadline := time.Now().Add(c.wait)

	for {
		di, err := c.dyn.Resource(dataImportGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get DataImport %s/%s: %w", namespace, name, err)
		}

		// A DataImport whose idle TTL elapses before the endpoint comes up never becomes
		// Ready; surface that terminal state instead of waiting out the whole timeout.
		if conditionTrue(di, conditionExpired) {
			return nil, fmt.Errorf("data import %s/%s expired before becoming Ready (idle TTL elapsed); increase --ttl or retry", namespace, name)
		}

		url, _, _ := unstructured.NestedString(di.Object, "status", "url")
		volumeMode, _, _ := unstructured.NestedString(di.Object, "status", "volumeMode")

		if conditionTrue(di, conditionReady) && url != "" && volumeMode != "" {
			return di, nil
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout waiting for DataImport %s/%s to become Ready", namespace, name)
		}

		if !sleepCtx(ctx, c.poll) {
			return nil, ctx.Err()
		}
	}
}

// dataImportCompleted reports whether the named DataImport already produced its durable
// artifact (Completed=True with a populated status.dataArtifactRef). A missing object is not
// an error: it simply means "not completed".
func (c *clusterVolumeImporter) dataImportCompleted(ctx context.Context, name, namespace string) (bool, error) {
	di, err := c.dyn.Resource(dataImportGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if kubeerrors.IsNotFound(err) {
			return false, nil
		}

		return false, fmt.Errorf("get DataImport %s/%s: %w", namespace, name, err)
	}

	_, hasArtifact, _ := unstructured.NestedMap(di.Object, "status", "dataArtifactRef")

	return conditionTrue(di, conditionCompleted) && hasArtifact, nil
}

// waitDataImportCompleted blocks until the DataImport produces its durable artifact
// (Completed=True with a populated status.dataArtifactRef).
func (c *clusterVolumeImporter) waitDataImportCompleted(ctx context.Context, name, namespace string) error {
	deadline := time.Now().Add(c.wait)

	for {
		done, err := c.dataImportCompleted(ctx, name, namespace)
		if err != nil {
			return err
		}

		if done {
			c.log.Info("volume data import completed", slog.String("namespace", namespace), slog.String("name", name))

			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for DataImport %s/%s to complete", namespace, name)
		}

		if !sleepCtx(ctx, c.poll) {
			return ctx.Err()
		}
	}
}

// leafTargetRef returns the DataImport targetRef {group, kind} for a data leaf. The
// state-snapshotter reverse-lookup matches the leaf by GroupKind directly, so no RESTMapper
// resource resolution is needed: the kind is the leaf's own kind and the group is parsed from
// its apiVersion. For CSI VolumeSnapshot leaves the group/kind are fixed constants.
func leafTargetRef(leaf PlannedNode) (string, string, error) {
	if leaf.isVolumeSnapshotLeaf() {
		return aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotKind, nil
	}

	if leaf.isDomainDataLeaf() {
		gv, parseErr := schema.ParseGroupVersion(leaf.APIVersion)
		if parseErr != nil {
			return "", "", fmt.Errorf("parse apiVersion %q for domain leaf %s/%s: %w", leaf.APIVersion, leaf.Kind, leaf.Name, parseErr)
		}

		return gv.Group, leaf.Kind, nil
	}

	return "", "", unsupportedNodeError(leaf)
}

// httpDoer is the minimal HTTP surface putBlock/postFinished need; *safeClient.SafeClient
// satisfies it, and tests stub it.
type httpDoer interface {
	HTTPDo(req *http.Request) (*http.Response, error)
}

// putBlock streams the raw block file to the importer's block endpoint, honouring the
// server-reported X-Next-Offset for resumable progress. onProgress, when non-nil, is
// called with the number of bytes accepted by the server after each PUT chunk.
func putBlock(ctx context.Context, httpClient httpDoer, url, filePath string, totalSize int64, onProgress func(int)) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Resume from the importer's recorded write offset: a reused (non-expired) DataImport
	// may already hold a partial upload, and the block handler rejects a PUT whose X-Offset
	// disagrees with its current offset (HTTP 409), so restarting at 0 would never converge.
	offset, err := headBlockOffset(ctx, httpClient, url)
	if err != nil {
		return err
	}

	// offset > totalSize means the importer already holds more bytes than this archive
	// provides: the DataImport was reused with data from a different/larger source. Refuse
	// to finalize a mismatched device instead of silently skipping the upload.
	if offset > totalSize {
		return fmt.Errorf("importer already holds %d bytes but the archive provides only %d; "+
			"the DataImport was reused with mismatched data — delete it and retry", offset, totalSize)
	}

	// offset == totalSize is a legitimate resume of an upload that fully transferred before
	// the previous run finalized it; the loop is skipped and the caller finalizes.

	for offset < totalSize {
		section := io.NewSectionReader(file, offset, totalSize-offset)

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, io.NopCloser(section))
		if err != nil {
			return err
		}

		req.Header.Set("X-Content-Length", strconv.FormatInt(totalSize, 10))
		req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))
		req.Header.Set("X-Attribute-Permissions", blockAttrPermissions)
		req.Header.Set("X-Attribute-Uid", blockAttrUID)
		req.Header.Set("X-Attribute-Gid", blockAttrGID)

		next, err := doBlockChunk(httpClient, req, offset, totalSize)
		if err != nil {
			return err
		}

		if onProgress != nil {
			onProgress(int(next - offset))
		}

		offset = next
	}

	return nil
}

// headBlockOffset asks the importer (HEAD) how many bytes it has already durably written so
// an interrupted upload can resume. A missing object or absent header means "start at 0".
func headBlockOffset(ctx context.Context, httpClient httpDoer, url string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, err
	}

	resp, err := httpClient.HTTPDo(req)
	if err != nil {
		return 0, err
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		next := resp.Header.Get("X-Next-Offset")
		if next == "" {
			return 0, nil
		}

		off, perr := strconv.ParseInt(next, 10, 64)
		if perr != nil || off < 0 {
			return 0, fmt.Errorf("invalid X-Next-Offset %q from %s", next, url)
		}

		return off, nil
	case http.StatusNotFound:
		return 0, nil
	default:
		return 0, fmt.Errorf("HEAD %s returned status %d (%s)", url, resp.StatusCode, resp.Status)
	}
}

// doBlockChunk performs one PUT and returns the next offset to resume from.
func doBlockChunk(httpClient httpDoer, req *http.Request, offset, totalSize int64) (int64, error) {
	resp, err := httpClient.HTTPDo(req)
	if err != nil {
		return 0, err
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return 0, fmt.Errorf("server error at offset %d: status %d (%s)", offset, resp.StatusCode, resp.Status)
	}

	nextStr := resp.Header.Get("X-Next-Offset")
	if nextStr == "" {
		return totalSize, nil
	}

	next, err := strconv.ParseInt(nextStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid X-Next-Offset %q: %w", nextStr, err)
	}

	if next <= offset {
		return 0, fmt.Errorf("server returned non-advancing X-Next-Offset (%d <= %d)", next, offset)
	}

	return next, nil
}

// postFinished signals end-of-upload to the importer (POST .../api/v1/finished).
func postFinished(ctx context.Context, httpClient httpDoer, baseURL string) error {
	finishedURL, err := neturl.JoinPath(baseURL, uploadFinishedSubpath)
	if err != nil {
		return fmt.Errorf("build finished URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, finishedURL, nil)
	if err != nil {
		return err
	}

	resp, err := httpClient.HTTPDo(req)
	if err != nil {
		return err
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("finished returned status %d (%s)", resp.StatusCode, resp.Status)
	}

	return nil
}
