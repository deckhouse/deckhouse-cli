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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/klauspost/compress/zstd"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	dataImportKind             = "DataImport"
	dataImportModePopulateData = "PopulateData"
	volumeModeBlock            = "Block"
	conditionReady             = "Ready"
	conditionCompleted         = "Completed"
	// reasonExpired is the Ready-condition reason the producer sets when a DataImport idle-expires. The
	// standalone "Expired" condition type was removed from the catalog in favour of this reason (plus a
	// terminal status.phase=Expired).
	reasonExpired         = "Expired"
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

// dataImportGVR is the storage-foundation DataImport resource
// (storage-foundation.deckhouse.io/v1alpha1).
var dataImportGVR = schema.GroupVersionResource{Group: "storage-foundation.deckhouse.io", Version: "v1alpha1", Resource: "dataimports"}

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
	// or filesystem data, finalises, and waits for completion. onProgress, when non-nil, is
	// called with the number of bytes written after each chunk or file upload; nil disables
	// progress reporting and leaves upload behaviour unchanged. setTotal, when non-nil, is
	// called with the expected total byte count: once, before any bytes are sent, on the
	// block path (the total is known up front from leaf.Size); progressively, with a
	// growing running sum as each file's exact size becomes known, on the filesystem path
	// (see sendVolumeData for why an accurate a-priori FS total is not free). activate, when
	// non-nil, is called at least once but ONLY when a real transfer occurs — never on a
	// leaf whose upload is entirely a server-side skip (offset==totalSize on the block HEAD,
	// or every file already done on the filesystem HEAD) — so the caller's progress stream
	// can distinguish a genuine transfer from a full resume-skip (see progress.Stream.Activate).
	UploadVolumeData(ctx context.Context, leaf PlannedNode, diName, namespace string, setTotal func(int64), onProgress func(int), activate func()) error
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
}

// NewClusterVolumeImporter builds the live VolumeImporter. ttl is the DataImport TTL,
// wait bounds the per-DataImport readiness/completion waits, and poll is the polling
// cadence. Block-volume uploads stream-decode directly into the PUT (see putBlock), so
// no scratch directory for decompressed temporary files is needed.
func NewClusterVolumeImporter(
	dyn dynamic.Interface,
	sc *safeClient.SafeClient,
	ttl string,
	wait, poll time.Duration,
	log *slog.Logger,
) VolumeImporter {
	if log == nil {
		log = slog.Default()
	}

	return &clusterVolumeImporter{dyn: dyn, sc: sc, ttl: ttl, poll: poll, wait: wait, log: log}
}

// DataImportName returns the deterministic DataImport name for the leaf (its own name).
func (c *clusterVolumeImporter) DataImportName(leaf PlannedNode) string {
	return leaf.Name
}

// EnsureDataImport upserts the PopulateData DataImport for the leaf snapshot node. The import
// stages the uploaded bytes into a transient scratch volume (spec.storageParams, sourced from
// the leaf's captured VolumeInfo in the archive) and captures them into a durable
// VolumeSnapshotContent bound to the leaf via spec.snapshotRef. The state-snapshotter
// reverse-lookup matches the leaf against spec.snapshotRef (apiVersion/kind/name); the
// DataImport controller itself does not read that ref.
func (c *clusterVolumeImporter) EnsureDataImport(ctx context.Context, leaf PlannedNode, namespace string) (string, error) {
	name := c.DataImportName(leaf)

	// storageParams.storageClassName and size are required by the DataImport CRD (CEL rule +
	// minLength). They originate from the leaf's captured VolumeInfo carried through the
	// archive; a blank value means a malformed or stale archive, so fail early with a clear
	// message rather than letting the API server reject an incomplete DataImport.
	if leaf.StorageClassName == "" || leaf.Size == "" {
		return "", fmt.Errorf("data leaf %s/%s is missing captured storage parameters in the archive "+
			"(storageClassName=%q size=%q); re-download the snapshot with a current d8 version",
			leaf.Kind, leaf.Name, leaf.StorageClassName, leaf.Size)
	}

	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(dataImportGVR.GroupVersion().String())
	obj.SetKind(dataImportKind)
	obj.SetNamespace(namespace)
	obj.SetName(name)

	storageParams := map[string]interface{}{
		"storageClassName": leaf.StorageClassName,
		"size":             leaf.Size,
	}

	// volumeMode is optional (the CRD defaults it to Filesystem); send it only when the
	// archive captured it so the scratch volume matches the source volume mode.
	if leaf.VolumeMode != "" {
		storageParams["volumeMode"] = leaf.VolumeMode
	}

	spec := map[string]interface{}{
		"ttl":  c.ttl,
		"mode": dataImportModePopulateData,
		"snapshotRef": map[string]interface{}{
			"apiVersion": leaf.APIVersion,
			"kind":       leaf.Kind,
			"name":       leaf.Name,
		},
		"storageParams": storageParams,
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
			if !conditionFalseWithReason(existing, conditionReady, reasonExpired) {
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
func (c *clusterVolumeImporter) UploadVolumeData(ctx context.Context, leaf PlannedNode, diName, namespace string, setTotal func(int64), onProgress func(int), activate func()) error {
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

	if err := c.sendVolumeData(ctx, httpClient, url, volumeMode, leaf, namespace, diName, setTotal, onProgress, activate); err != nil {
		return err
	}

	return c.waitDataImportCompleted(ctx, diName, namespace)
}

// sendVolumeData streams the volume payload (FS tar or raw block data) to the importer
// and signals end-of-upload via postFinished. It does NOT poll for DataImport completion;
// the caller must invoke waitDataImportCompleted afterwards.
// Using leaf.TarFile (not leaf.DataFile) for the FS path is essential: DataFile holds the
// block-data glob result (data.bin*), which is always empty for FS-only leaves.
func (c *clusterVolumeImporter) sendVolumeData(ctx context.Context, httpClient httpDoer, url, volumeMode string, leaf PlannedNode, namespace, diName string, setTotal func(int64), onProgress func(int), activate func()) error {
	switch volumeMode {
	case volumeModeFilesystem:
		c.log.Info("uploading filesystem data",
			slog.String("namespace", namespace),
			slog.String("dataimport", diName))

		// The FS-upload total is reported PROGRESSIVELY, not as a single a-priori value
		// like the block path's setTotal(totalSize) below: a tar header only records a
		// compressed entry's STORED length, so the true (decompressed) size of a LATER
		// file is not knowable until importFSFromTar has walked every entry before it.
		// Computing a full total up front would mean decompressing every entry just to
		// measure it -- exactly the extra work the two-pass streaming design exists to
		// avoid when a resume never needs it (see importFSFromTar). Instead, setTotal is
		// threaded straight through: importFSFromTar calls it with a running sum each
		// time a new file's exact size becomes known (a skipped/already-done file's size
		// from HEAD's Content-Length, or a not-done file's exact size from its measure
		// step / hdr.Size), so the bar's denominator grows as work is discovered rather
		// than staying at zero for the whole upload. Honest limitation: the total is not
		// complete until the LAST entry in the tar has been processed.
		if err := importFSFromTar(ctx, httpClient, url, leaf.TarFile, c.log, setTotal, onProgress, activate); err != nil {
			return fmt.Errorf("upload filesystem data for %s/%s: %w", namespace, diName, err)
		}

		if err := postFinished(ctx, httpClient, url); err != nil {
			return fmt.Errorf("finalise upload for %s/%s: %w", namespace, diName, err)
		}

	case volumeModeBlock:
		ext := filepath.Ext(leaf.DataFile)

		totalSize, err := blockTotalSize(leaf.DataFile, leaf.Size, ext)
		if err != nil {
			return err
		}

		blockURL, err := neturl.JoinPath(url, uploadBlockSubpath)
		if err != nil {
			return fmt.Errorf("build block upload URL: %w", err)
		}

		c.log.Info("uploading volume data",
			slog.String("namespace", namespace),
			slog.String("dataimport", diName),
			slog.Int64("bytes", totalSize))

		// totalSize is known up front (blockTotalSize never decompresses to measure it)
		// and matches the onProgress increments (each PUT chunk advances by next-offset),
		// so report it as the total before any bytes are sent.
		if setTotal != nil {
			setTotal(totalSize)
		}

		if err := putBlock(ctx, httpClient, blockURL, leaf.DataFile, ext, totalSize, c.log, onProgress, activate); err != nil {
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
		if conditionFalseWithReason(di, conditionReady, reasonExpired) {
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
// artifact (Completed=True with a populated status.data.artifactRef). A missing object is not
// an error: it simply means "not completed".
func (c *clusterVolumeImporter) dataImportCompleted(ctx context.Context, name, namespace string) (bool, error) {
	di, err := c.dyn.Resource(dataImportGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if kubeerrors.IsNotFound(err) {
			return false, nil
		}

		return false, fmt.Errorf("get DataImport %s/%s: %w", namespace, name, err)
	}

	_, hasArtifact, _ := unstructured.NestedMap(di.Object, "status", "data", "artifactRef")

	return conditionTrue(di, conditionCompleted) && hasArtifact, nil
}

// waitDataImportCompleted blocks until the DataImport produces its durable artifact
// (Completed=True with a populated status.data.artifactRef).
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

// httpDoer is the minimal HTTP surface putBlock/postFinished need; *safeClient.SafeClient
// satisfies it, and tests stub it.
type httpDoer interface {
	HTTPDo(req *http.Request) (*http.Response, error)
}

// blockTotalSize returns the exact decompressed byte count of a node's block-volume data
// file without decompressing it. A raw file (ext=="") carries its exact size on disk
// (os.Stat); a compressed file's decompressed size is definitionally the captured
// volume's real allocated size (size, a resource.Quantity string like "10Gi", sourced
// from VolumeSnapshotContent.status.restoreSize — see archive.VolumeInfo.Size) because a
// block-volume capture always reads exactly the device's provisioned byte size. Parsing
// size instead of decompressing avoids a full decompression pass purely to learn a byte
// count, which is the whole point of the streaming upload path.
func blockTotalSize(dataFile, size, ext string) (int64, error) {
	if ext == "" {
		info, err := os.Stat(dataFile)
		if err != nil {
			return 0, fmt.Errorf("stat volume data %s: %w", dataFile, err)
		}

		return info.Size(), nil
	}

	q, err := resource.ParseQuantity(size)
	if err != nil {
		return 0, fmt.Errorf("parsing captured volume size %q for %s: %w", size, dataFile, err)
	}

	return q.Value(), nil
}

// putBlock streams the block-volume payload at dataFile to the importer's block
// endpoint, honouring the server-reported X-Next-Offset for resumable progress. ext
// selects the decode codec via compress.NewReader ("" for raw/no codec, matching
// Codec.Ext); totalSize is the volume's exact decompressed byte count (see
// blockTotalSize). onProgress, when non-nil, is called with the number of bytes accepted
// by the server after each PUT. activate, when non-nil, is called at the start of every
// real transfer iteration (never when offset==totalSize short-circuits before any PUT is
// attempted), so the caller's progress stream is activated only on a genuine transfer.
func putBlock(ctx context.Context, httpClient httpDoer, url, dataFile, ext string, totalSize int64, log *slog.Logger, onProgress func(int), activate func()) error {
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
	// the previous run finalized it: nothing left to send, so skip building a decode reader
	// (or even opening the file) entirely and let the caller finalize.
	if offset == totalSize {
		return nil
	}

	if ext == "" {
		return putBlockRaw(ctx, httpClient, url, dataFile, offset, totalSize, onProgress, activate)
	}

	return putBlockCompressed(ctx, httpClient, url, dataFile, ext, offset, totalSize, log, onProgress, activate)
}

// putBlockRaw streams an uncompressed data.bin file starting at offset. This path is
// unchanged from before streaming decode was introduced: a fresh io.SectionReader per
// iteration is a cheap seek over the same file handle, so a server-reported partial
// acceptance (next < totalSize) simply resumes the loop with no extra I/O.
func putBlockRaw(ctx context.Context, httpClient httpDoer, url, filePath string, offset, totalSize int64, onProgress func(int), activate func()) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	for offset < totalSize {
		if activate != nil {
			activate()
		}

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

// putBlockCompressed streams a compressed data.bin.<ext> file starting at offset,
// decoding it on the fly via compress.NewReader instead of decompressing it into a
// temporary file first — the whole point of this path is to keep peak disk usage at one
// copy (the compressed archive) instead of two.
//
// RESUME STRATEGY dispatches on codec and offset (see resolveBlockDecodeReader):
//
//  1. offset == 0: no positioning needed — compress.NewReader(ext, f) from the start.
//  2. ext == ".zst" and offset > 0: NATIVE SEEK FAST PATH. volume.MergeBlockChunks
//     embeds a Zstandard Seekable Format seek table as a trailing skippable frame in
//     every merged zstd data file (github.com/SaveTheRbtz/zstd-seekable-format-go).
//     seekable.NewReader(f, dec).Seek(offset, io.SeekStart) looks the target chunk up in
//     that table and positions the reader to decode forward from there — the library
//     decodes at most the ONE frame containing offset on the first subsequent Read, never
//     the already-uploaded prefix. This library exposes no typed/sentinel error for "no
//     embedded seek table" (confirmed empirically in
//     compress/seekable_spike_test.go's TestSeekableFormat_NoSeekTableFailsGracefully), so
//     ANY failure to open or seek — e.g. an archive built by a d8 version predating this
//     feature, by a third-party tool, or any other non-seekable .zst — degrades to case 3
//     below, logged at Warn, never a hard failure.
//  3. gzip or lz4 with offset > 0, OR a zstd file whose case 2 fast path degraded:
//     PLAIN BYTE-ZERO FALLBACK, byte-for-byte the behavior that existed immediately
//     before the whole chunk-index/native-seek episode (commit 6423fb4's predecessor
//     state) — a fresh compress.NewReader(ext, f) from the start, discarding exactly
//     offset decoded bytes via io.CopyN before returning it. (The "none" codec never
//     reaches putBlockCompressed at all — putBlock routes it to putBlockRaw instead.)
//
// This is a DELIBERATE, DOCUMENTED regression for gzip/lz4 relative to the (now removed)
// chunk-index fast path: resume cost reverts to O(offset) discard. Justification: (a) no
// comparably mature/maintained Go library implements a seekable format with an embedded
// seek table for either codec, unlike zstd's community Seekable Format and this library
// (see tasks.json's notes_on_plan_switch, NATIVE-ZSTD-SEEKABLE-RESUME PIVOT entry); (b)
// both codecs are already excluded from user-facing --volume-compression selection
// (compress.UserSelectableNames() currently allows only "zstd"/"none"; see
// codec-user-selection-zstd-only — a list explicitly expected to change again as
// gzip/lz4 gain a seek-resume mechanism, NOT a permanent exclusion), so the blast
// radius is narrowed to archives produced by an older/third-party tool or a d8
// version whose allow-list included gzip/lz4 — an accepted tradeoff, not an
// oversight.
func putBlockCompressed(ctx context.Context, httpClient httpDoer, url, dataFile, ext string, offset, totalSize int64, log *slog.Logger, onProgress func(int), activate func()) error {
	f, err := os.Open(dataFile)
	if err != nil {
		return fmt.Errorf("open volume data %s: %w", dataFile, err)
	}

	defer func() { _ = f.Close() }()

	decodeReader, _, err := resolveBlockDecodeReader(f, dataFile, ext, offset, log)
	if err != nil {
		return err
	}

	defer func() { _ = decodeReader.Close() }()

	for offset < totalSize {
		if activate != nil {
			activate()
		}

		remain := totalSize - offset
		limited := io.LimitReader(decodeReader, remain)

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, io.NopCloser(limited))
		if err != nil {
			return err
		}

		// net/http only auto-detects Content-Length for *bytes.Buffer/*bytes.Reader/
		// *strings.Reader bodies; an io.LimitReader-wrapped decode stream needs it set
		// explicitly, or the request silently falls back to chunked transfer encoding.
		// Setting it also gives us the "too-large declared size" safety net for free: if
		// the decode stream runs dry before delivering remain bytes, net/http's transport
		// refuses to send a body shorter than its declared Content-Length and doBlockChunk
		// returns that error instead of silently truncating the request.
		req.ContentLength = remain

		req.Header.Set("X-Content-Length", strconv.FormatInt(totalSize, 10))
		req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))
		req.Header.Set("X-Attribute-Permissions", blockAttrPermissions)
		req.Header.Set("X-Attribute-Uid", blockAttrUID)
		req.Header.Set("X-Attribute-Gid", blockAttrGID)

		next, err := doBlockChunk(httpClient, req, offset, totalSize)
		if err != nil {
			return fmt.Errorf("%s: declared size %d bytes may not match the archive's actual decompressed content: %w", dataFile, totalSize, err)
		}

		if onProgress != nil {
			onProgress(int(next - offset))
		}

		offset = next
	}

	// Safety net: totalSize came from the archive's captured metadata (leaf.Size), never
	// from decoding, so verify it wasn't an UNDER-count. If the decode stream still has
	// bytes left after every declared byte was sent, the archive is corrupt or was built
	// by a mismatched version — fail loudly instead of silently truncating the transfer
	// and reporting success (an under-count would otherwise write a truncated device and
	// still exit clean).
	var probe [1]byte

	n, rerr := decodeReader.Read(probe[:])
	if n > 0 {
		return fmt.Errorf("%s: declared size %d bytes is smaller than the archive's actual decompressed content "+
			"(extra bytes found after the declared total); the archive may be corrupt or was built by a mismatched d8 version", dataFile, totalSize)
	}

	if rerr != nil && !errors.Is(rerr, io.EOF) {
		return fmt.Errorf("verifying end of archive %s after upload: %w", dataFile, rerr)
	}

	return nil
}

// resolveBlockDecodeReader returns a decode reader for f already positioned at the
// decompressed byte offset requested by a resumed upload — see putBlockCompressed's
// RESUME STRATEGY doc comment for the full three-way dispatch this implements. offset ==
// 0 needs no positioning at all. f must support Seek because the zstd fast path needs it
// (both to open the seekable reader and, on degradation, to rewind before the fallback
// decode); *os.File — putBlockCompressed's only caller — satisfies this directly. The
// second return value is the number of decoded bytes discarded via io.CopyN to reach
// offset on the plain byte-zero fallback path (discardFromStart); it is always 0 when
// offset == 0 or when the zstd native seek fast path is used — Seek is a seek-table
// lookup, not a decode-and-discard, so there is nothing to count there.
func resolveBlockDecodeReader(f io.ReadSeeker, dataFile, ext string, offset int64, log *slog.Logger) (io.ReadCloser, int64, error) {
	if offset == 0 {
		decodeReader, err := compress.NewReader(ext, f)
		if err != nil {
			return nil, 0, fmt.Errorf("open decompressor for %s: %w", dataFile, err)
		}

		return decodeReader, 0, nil
	}

	if ext == ".zst" {
		decodeReader, ok, err := seekZstdNativeReader(f, dataFile, offset, log)
		if err != nil {
			return nil, 0, err
		}

		if ok {
			return decodeReader, 0, nil
		}

		// seekZstdNativeReader may have moved f's read position while probing for a seek
		// table (the library's footer parse seeks near the end of the file); rewind
		// before falling back to a byte-zero decode over the same handle.
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, 0, fmt.Errorf("rewind %s after non-seekable zstd fallback: %w", dataFile, err)
		}
	}

	return discardFromStart(f, dataFile, ext, offset, log)
}

// seekableZstdReader wraps a *seekable.Reader together with the zstd decoder
// seekZstdNativeReader constructed for it, so a single Close call releases both.
// seekable.NewReader's doc comment is explicit that "the caller remains responsible for
// closing rs and decoder" — the seekable.Reader's own Close only releases its internal
// frame cache, never the decoder passed to NewReader. Leaving the decoder unclosed would
// leak its background decode-worker goroutines (see klauspost/compress/zstd.Decoder.Close's
// own doc comment).
type seekableZstdReader struct {
	*seekable.Reader
	dec *zstd.Decoder
}

// Close releases the seekable.Reader's own resources and its zstd decoder together.
func (s *seekableZstdReader) Close() error {
	err := s.Reader.Close()
	s.dec.Close()

	return err
}

// seekZstdNativeReader attempts the native zstd seekable-format fast path: open the
// merged data file's embedded seek table (a trailing skippable frame written by
// volume.MergeBlockChunks for every zstd volume) and seek directly to the decompressed
// offset. ok is false for ANY failure to open or seek the seekable reader — the library
// exposes no typed or sentinel error distinguishing "no embedded seek table" (an archive
// merged by a d8 version predating this feature, by a third-party tool, or any other
// non-seekable .zst) from any other failure, confirmed empirically in
// compress/seekable_spike_test.go's TestSeekableFormat_NoSeekTableFailsGracefully, so
// every such failure is treated identically: log at Warn and let the caller degrade to
// the byte-zero discard fallback. A failure constructing the plain klauspost decoder
// itself, before any source bytes are even touched, is a genuine unexpected error and is
// returned as such rather than folded into the graceful-fallback log line.
func seekZstdNativeReader(f io.ReadSeeker, dataFile string, offset int64, log *slog.Logger) (io.ReadCloser, bool, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, false, fmt.Errorf("create zstd decoder for seekable resume of %s: %w", dataFile, err)
	}

	r, err := seekable.NewReader(f, dec)
	if err != nil {
		dec.Close()

		log.Warn("no usable embedded zstd seek table, falling back to full discard",
			slog.String("file", dataFile),
			slog.String("error", err.Error()))

		return nil, false, nil
	}

	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		_ = r.Close()
		dec.Close()

		log.Warn("zstd seek-table lookup failed at resume offset, falling back to full discard",
			slog.String("file", dataFile),
			slog.Int64("offset", offset),
			slog.String("error", err.Error()))

		return nil, false, nil
	}

	return &seekableZstdReader{Reader: r, dec: dec}, true, nil
}

// discardFromStart is the plain byte-zero resume fallback: open a fresh decode reader at
// the start of f and discard exactly offset decoded bytes via io.CopyN before returning
// it, positioned for the caller to resume the PUT loop from offset. This is the resume
// behavior for gzip, lz4, and any zstd file with no usable embedded seek table — restored
// byte-for-byte from the state before the whole chunk-index/native-seek episode (commit
// 6423fb4's predecessor state). See putBlockCompressed's doc comment for why gzip/lz4 do
// not get a native-seek fast path.
func discardFromStart(f io.Reader, dataFile, ext string, offset int64, log *slog.Logger) (io.ReadCloser, int64, error) {
	decodeReader, err := compress.NewReader(ext, f)
	if err != nil {
		return nil, 0, fmt.Errorf("open decompressor for %s: %w", dataFile, err)
	}

	start := time.Now()

	skipped, err := io.CopyN(io.Discard, decodeReader, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("fast-forwarding %s to resume offset %d (got %d bytes): %w", dataFile, offset, skipped, err)
	}

	log.Info("discarded already-uploaded bytes from the start (no native seek fast path for this codec/file)",
		slog.String("file", dataFile),
		slog.Int64("bytes", skipped),
		slog.Duration("took", time.Since(start)))

	return decodeReader, skipped, nil
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
