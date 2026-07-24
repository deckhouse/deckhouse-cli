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
	"strconv"
	"strings"
	"sync"
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
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

	blockDiscardBufferSize             = 32 * 1024
	blockPutPayloadLimit               = 32 * 1024 * 1024
	maxConsecutiveBlockConflicts       = 8
	maxBlockConflictReplays            = 4 * maxConsecutiveBlockConflicts
	maxBlockReplayBytes          int64 = maxBlockConflictReplays * blockPutPayloadLimit

	uploadConnectTimeout        = 30 * time.Second
	uploadTLSHandshakeTimeout   = 10 * time.Second
	uploadResponseHeaderTimeout = 30 * time.Second
	uploadWriteIdleTimeout      = 30 * time.Second
	uploadReadIdleTimeout       = 15 * time.Second
	uploadResponseTotalTimeout  = 30 * time.Second
	uploadResponseByteLimit     = 1 * 1024 * 1024

	dataImportIdentityVersion  = "v1"
	dataImportIdentityIDLength = 16
	dataImportNameMaxLength    = 63
	sha256HexLength            = 64

	dataImportPayloadBlock      = "block"
	dataImportPayloadFilesystem = "filesystem"

	dataImportMetadataPrefix            = "snapshot.deckhouse.io/"
	dataImportIdentityLabel             = dataImportMetadataPrefix + "data-import-id"
	dataImportIdentityVersionAnnotation = dataImportMetadataPrefix + "data-import-identity-version"
	dataImportIdentityAnnotation        = dataImportMetadataPrefix + "data-import-identity"
	dataImportNodeChecksumAnnotation    = dataImportMetadataPrefix + "node-checksum"
	dataImportVolumeModeAnnotation      = dataImportMetadataPrefix + "volume-mode"
	dataImportStorageClassAnnotation    = dataImportMetadataPrefix + "storage-class-name"
	dataImportSizeBytesAnnotation       = dataImportMetadataPrefix + "size-bytes"
	dataImportPayloadKindAnnotation     = dataImportMetadataPrefix + "payload-kind"
	dataImportCodecAnnotation           = dataImportMetadataPrefix + "codec"
)

// dataImportGVR is the storage-foundation DataImport resource
// (storage-foundation.deckhouse.io/v1alpha1).
var dataImportGVR = schema.GroupVersionResource{Group: "storage-foundation.deckhouse.io", Version: "v1alpha1", Resource: "dataimports"}

// ErrForeignDataImport is returned when a shared DataImport name is occupied by an
// object whose content identity or normalized upload spec belongs to another archive.
var ErrForeignDataImport = errors.New("foreign DataImport collision")

// VolumeImporter imports a data leaf's volume bytes by creating an SVDM DataImport,
// waiting for the importer to be ready, streaming the archive bytes, finalising the
// upload, and waiting for the durable artifact to be produced. It is satisfied by
// clusterVolumeImporter and stubbed in tests.
type VolumeImporter interface {
	// DataImportName returns the deterministic identity-qualified DataImport name for the leaf.
	// The DataImport is created bottom-up immediately before its upload — its TTL is an idle
	// timer that starts at importer-pod start, so a freshly created importer must not sit
	// idle waiting for earlier siblings to finish.
	DataImportName(leaf PlannedNode) string
	// EnsureDataImport creates (idempotently) the DataImport for the leaf and returns its name.
	EnsureDataImport(ctx context.Context, leaf PlannedNode, namespace string) (string, error)
	// UploadVolumeData waits for the DataImport to become ready, streams the leaf's block
	// or filesystem data, finalises, and waits for completion. onProgress, when non-nil, is
	// called as raw bytes become known durable, including a validated server-side resume
	// prefix and each chunk or file upload; nil disables progress reporting and leaves upload
	// behaviour unchanged. setTotal, when non-nil, is
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
	dyn               dynamic.Interface
	sc                *safeClient.SafeClient
	newUploadClient   func([]byte, string) (uploadHTTPClient, error)
	ttl               string
	poll              time.Duration
	wait              time.Duration
	requestTimeout    time.Duration
	newRequestContext requestContextFactory
	log               *slog.Logger
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

	return &clusterVolumeImporter{
		dyn:               dyn,
		sc:                sc,
		ttl:               ttl,
		poll:              poll,
		wait:              wait,
		requestTimeout:    DefaultControlRequestTimeout,
		newRequestContext: context.WithTimeout,
		log:               log,
	}
}

// DataImportName returns the deterministic identity-qualified DataImport name for the leaf.
func (c *clusterVolumeImporter) DataImportName(leaf PlannedNode) string {
	if len(leaf.DataImportIdentity) < dataImportIdentityIDLength {
		return leaf.Name
	}

	suffix := leaf.DataImportIdentity[:dataImportIdentityIDLength]
	maxBaseLength := dataImportNameMaxLength - len(suffix) - 1

	base := strings.TrimRight(leaf.Name[:min(len(leaf.Name), maxBaseLength)], "-.")
	if base == "" {
		base = "dataimport"
	}

	return base + "-" + suffix
}

// EnsureDataImport upserts the PopulateData DataImport for the leaf snapshot node. The import
// stages the uploaded bytes into a transient scratch volume (spec.storageParams, sourced from
// the leaf's captured VolumeInfo in the archive) and captures them into a durable
// VolumeSnapshotContent bound to the leaf via spec.snapshotRef. The state-snapshotter
// reverse-lookup matches the leaf against spec.snapshotRef (apiVersion/kind/name); the
// DataImport controller itself does not read that ref.
func (c *clusterVolumeImporter) EnsureDataImport(ctx context.Context, leaf PlannedNode, namespace string) (string, error) {
	if err := validateDataImportLeaf(leaf); err != nil {
		return "", err
	}

	name := c.DataImportName(leaf)

	// storageParams.storageClassName and size are required by the DataImport CRD (CEL rule +
	// minLength). They originate from the leaf's captured VolumeInfo carried through the
	// archive; a blank value means a malformed or stale archive, so fail early with a clear
	// message rather than letting the API server reject an incomplete DataImport.
	if leaf.StorageClassName == "" || leaf.SizeBytes <= 0 {
		return "", fmt.Errorf("data leaf %s/%s is missing captured storage parameters in the archive "+
			"(storageClassName=%q sizeBytes=%d); re-download the snapshot with a current d8 version",
			leaf.Kind, leaf.Name, leaf.StorageClassName, leaf.SizeBytes)
	}

	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(dataImportGVR.GroupVersion().String())
	obj.SetKind(dataImportKind)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetLabels(map[string]string{
		dataImportIdentityLabel: dataImportShortID(leaf),
	})
	obj.SetAnnotations(dataImportAnnotations(leaf))

	storageParams := map[string]interface{}{
		"storageClassName": leaf.StorageClassName,
		"size":             strconv.FormatInt(leaf.SizeBytes, 10),
		"volumeMode":       leaf.VolumeMode,
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
		existing, err := runControlRequest(ctx, c.requestTimeout, c.newRequestContext,
			func(requestCtx context.Context) (*unstructured.Unstructured, error) {
				return ri.Get(requestCtx, name, metav1.GetOptions{})
			})
		if err == nil {
			if matchErr := validateDataImport(existing, leaf); matchErr != nil {
				return "", fmt.Errorf("reuse DataImport %s/%s: %w", namespace, name, matchErr)
			}

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

		_, err = runControlRequest(ctx, c.requestTimeout, c.newRequestContext,
			func(requestCtx context.Context) (*unstructured.Unstructured, error) {
				return ri.Create(requestCtx, obj, metav1.CreateOptions{})
			})
		if err != nil {
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

func validateDataImportLeaf(leaf PlannedNode) error {
	if len(leaf.DataImportIdentity) != sha256HexLength || len(leaf.NodeChecksum) != sha256HexLength {
		return fmt.Errorf("data leaf %s/%s has no verified content identity", leaf.Kind, leaf.Name)
	}

	if leaf.VolumeMode == "" || leaf.StorageClassName == "" || leaf.SizeBytes <= 0 ||
		leaf.PayloadKind == "" || leaf.Codec == "" {
		return fmt.Errorf("data leaf %s/%s has incomplete canonical upload metadata", leaf.Kind, leaf.Name)
	}

	return nil
}

func dataImportShortID(leaf PlannedNode) string {
	return leaf.DataImportIdentity[:dataImportIdentityIDLength]
}

func dataImportAnnotations(leaf PlannedNode) map[string]string {
	return map[string]string{
		dataImportIdentityVersionAnnotation: dataImportIdentityVersion,
		dataImportIdentityAnnotation:        leaf.DataImportIdentity,
		dataImportNodeChecksumAnnotation:    leaf.NodeChecksum,
		dataImportVolumeModeAnnotation:      leaf.VolumeMode,
		dataImportStorageClassAnnotation:    leaf.StorageClassName,
		dataImportSizeBytesAnnotation:       strconv.FormatInt(leaf.SizeBytes, 10),
		dataImportPayloadKindAnnotation:     leaf.PayloadKind,
		dataImportCodecAnnotation:           leaf.Codec,
	}
}

func validateDataImport(obj *unstructured.Unstructured, leaf PlannedNode) error {
	if err := validateDataImportMetadata(obj, leaf); err != nil {
		return err
	}

	return validateDataImportSpec(obj, leaf)
}

func validateDataImportMetadata(obj *unstructured.Unstructured, leaf PlannedNode) error {
	labels := obj.GetLabels()
	if labels[dataImportIdentityLabel] != dataImportShortID(leaf) {
		return fmt.Errorf("label %q does not match content identity: %w", dataImportIdentityLabel, ErrForeignDataImport)
	}

	annotations := obj.GetAnnotations()
	for key, want := range dataImportAnnotations(leaf) {
		if annotations[key] != want {
			return fmt.Errorf("annotation %q is %q, want %q: %w", key, annotations[key], want, ErrForeignDataImport)
		}
	}

	return nil
}

func validateDataImportSpec(obj *unstructured.Unstructured, leaf PlannedNode) error {
	mode, found, err := unstructured.NestedString(obj.Object, "spec", "mode")
	if err != nil || !found || mode != dataImportModePopulateData {
		return fmt.Errorf("spec.mode is %q, want %q: %w", mode, dataImportModePopulateData, ErrForeignDataImport)
	}

	for field, want := range map[string]string{
		"apiVersion": leaf.APIVersion,
		"kind":       leaf.Kind,
		"name":       leaf.Name,
	} {
		got, refFound, refErr := unstructured.NestedString(obj.Object, "spec", "snapshotRef", field)
		if refErr != nil || !refFound || got != want {
			return fmt.Errorf("spec.snapshotRef.%s is %q, want %q: %w", field, got, want, ErrForeignDataImport)
		}
	}

	storageClass, found, err := unstructured.NestedString(obj.Object, "spec", "storageParams", "storageClassName")
	if err != nil || !found || storageClass != leaf.StorageClassName {
		return fmt.Errorf("spec.storageParams.storageClassName is %q, want %q: %w",
			storageClass, leaf.StorageClassName, ErrForeignDataImport)
	}

	volumeMode, found, err := unstructured.NestedString(obj.Object, "spec", "storageParams", "volumeMode")
	if err != nil || !found || volumeMode != leaf.VolumeMode {
		return fmt.Errorf("spec.storageParams.volumeMode is %q, want %q: %w",
			volumeMode, leaf.VolumeMode, ErrForeignDataImport)
	}

	size, found, err := unstructured.NestedString(obj.Object, "spec", "storageParams", "size")
	if err != nil || !found {
		return fmt.Errorf("spec.storageParams.size is missing: %w", ErrForeignDataImport)
	}

	quantity, parseErr := resource.ParseQuantity(size)
	if parseErr != nil || quantity.Value() != leaf.SizeBytes {
		return fmt.Errorf("spec.storageParams.size is %q, want %d bytes: %w",
			size, leaf.SizeBytes, ErrForeignDataImport)
	}

	return nil
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

	_, err := runControlRequest(ctx, c.requestTimeout, c.newRequestContext,
		func(requestCtx context.Context) (*unstructured.Unstructured, error) {
			return ri.Update(requestCtx, updated, metav1.UpdateOptions{})
		})
	if err != nil {
		return fmt.Errorf("patch DataImport %s/%s ttl: %w", existing.GetNamespace(), existing.GetName(), err)
	}

	c.log.Info("aligned DataImport ttl",
		slog.String("namespace", existing.GetNamespace()), slog.String("name", existing.GetName()), slog.String("ttl", c.ttl))

	return nil
}

// deleteDataImportAndWait deletes the named DataImport and blocks until it is gone (bounded
// by the importer wait budget), so a fresh one can be created under the same name.
func (c *clusterVolumeImporter) deleteDataImportAndWait(ctx context.Context, ri dynamic.ResourceInterface, name string) error {
	err := runControlRequestNoResult(ctx, c.requestTimeout, c.newRequestContext,
		func(requestCtx context.Context) error {
			return ri.Delete(requestCtx, name, metav1.DeleteOptions{})
		})
	if err != nil && !kubeerrors.IsNotFound(err) {
		return fmt.Errorf("delete expired DataImport %s: %w", name, err)
	}

	deadline := time.Now().Add(c.wait)

	for {
		_, err := runControlRequest(ctx, c.requestTimeout, c.newRequestContext,
			func(requestCtx context.Context) (*unstructured.Unstructured, error) {
				return ri.Get(requestCtx, name, metav1.GetOptions{})
			})
		if kubeerrors.IsNotFound(err) {
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

	if err := validateDataImportLeaf(leaf); err != nil {
		return err
	}

	// Idempotent retry: if a prior run already produced this leaf's durable artifact, skip
	// the upload. A Completed importer typically has no live endpoint, so waiting for Ready
	// would just time out.
	done, err := c.dataImportCompleted(ctx, leaf, diName, namespace)
	if err != nil {
		return err
	}

	if done {
		if err := verifyLeafPayloadCurrent(ctx, leaf); err != nil {
			return fmt.Errorf("validate archive payload before reusing completed DataImport %s/%s: %w",
				namespace, diName, err)
		}

		c.log.Info("volume data already imported; skipping upload",
			slog.String("namespace", namespace), slog.String("dataimport", diName))

		return nil
	}

	di, err := c.waitDataImportReady(ctx, leaf, diName, namespace)
	if err != nil {
		return err
	}

	url, _, _ := unstructured.NestedString(di.Object, "status", "url")
	volumeMode, _, _ := unstructured.NestedString(di.Object, "status", "volumeMode")
	caB64, _, _ := unstructured.NestedString(di.Object, "status", "ca")

	httpClient, err := c.uploadClient(caB64, url)
	if err != nil {
		return err
	}
	defer httpClient.CloseIdleConnections()

	if err := c.sendVolumeData(ctx, httpClient, url, volumeMode, leaf, namespace, diName, setTotal, onProgress, activate); err != nil {
		return err
	}

	return c.waitDataImportCompleted(ctx, leaf, diName, namespace)
}

func verifyLeafPayloadCurrent(ctx context.Context, leaf PlannedNode) error {
	if leaf.archiveView == nil || leaf.payloadFile == nil {
		return nil
	}

	handle, err := leaf.archiveView.OpenVerifiedFile(ctx, leaf.payloadFile)
	if err != nil {
		return err
	}

	verifyErr := handle.Verify(ctx)

	closeErr := handle.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close verified payload: %w", closeErr)
	}

	return errors.Join(verifyErr, closeErr)
}

// sendVolumeData streams the volume payload (FS tar or raw block data) to the importer
// and signals end-of-upload via postFinished. It does NOT poll for DataImport completion;
// the caller must invoke waitDataImportCompleted afterwards.
// Using leaf.TarFile (not leaf.DataFile) for the FS path is essential: DataFile holds the
// block-data glob result (data.bin*), which is always empty for FS-only leaves.
func (c *clusterVolumeImporter) sendVolumeData(ctx context.Context, httpClient httpDoer, url, volumeMode string, leaf PlannedNode, namespace, diName string, setTotal func(int64), onProgress func(int), activate func()) error {
	if leaf.archiveView == nil || leaf.payloadFile == nil {
		return c.sendVolumeDataFromSource(ctx, httpClient, url, volumeMode, leaf, namespace, diName,
			nil, setTotal, onProgress, activate)
	}

	handle, err := leaf.archiveView.OpenVerifiedFile(ctx, leaf.payloadFile)
	if err != nil {
		return fmt.Errorf("open verified payload for %s/%s: %w", leaf.Kind, leaf.Name, err)
	}

	sendErr := c.sendVolumeDataFromSource(ctx, httpClient, url, volumeMode, leaf, namespace, diName,
		handle, setTotal, onProgress, activate)

	closeErr := handle.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close verified payload for %s/%s: %w", leaf.Kind, leaf.Name, closeErr)
	}

	return errors.Join(sendErr, closeErr)
}

func (c *clusterVolumeImporter) sendVolumeDataFromSource(
	ctx context.Context,
	httpClient httpDoer,
	url, volumeMode string,
	leaf PlannedNode,
	namespace, diName string,
	handle *archive.VerifiedHandle,
	setTotal func(int64),
	onProgress func(int),
	activate func(),
) error {
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
		var err error
		if handle == nil {
			err = importFSFromTar(ctx, httpClient, url, leaf.TarFile, c.log, setTotal, onProgress, activate)
		} else {
			err = importFSFromTarSource(ctx, httpClient, url, leaf.TarFile, handle, c.log, setTotal, onProgress, activate)
		}

		if err != nil {
			return fmt.Errorf("upload filesystem data for %s/%s: %w", namespace, diName, err)
		}

		if err := verifyPayloadHandle(ctx, handle); err != nil {
			return fmt.Errorf("verify filesystem payload for %s/%s before finalisation: %w",
				namespace, diName, err)
		}

		if err := postFinished(ctx, httpClient, url); err != nil {
			return fmt.Errorf("finalise upload for %s/%s: %w", namespace, diName, err)
		}

	case volumeModeBlock:
		ext := leaf.Ext

		var (
			totalSize int64
			err       error
		)

		if handle == nil {
			totalSize, err = blockTotalSize(leaf.DataFile, leaf.Size, ext)
		} else {
			totalSize, err = blockTotalSizeFromSource(leaf.DataFile, leaf.Size, ext, handle)
		}

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
		// and matches the onProgress increments (validated durable offsets advance the bar),
		// so report it as the total before any bytes are sent.
		if setTotal != nil {
			setTotal(totalSize)
		}

		if handle == nil {
			err = putBlock(ctx, httpClient, blockURL, leaf.DataFile, ext, totalSize,
				c.log, onProgress, activate)
		} else {
			err = putBlockFromSource(ctx, httpClient, blockURL, leaf.DataFile, ext, totalSize,
				handle, c.log, onProgress, activate)
		}

		if err != nil {
			return fmt.Errorf("upload block data for %s/%s: %w", namespace, diName, err)
		}

		if err := verifyPayloadHandle(ctx, handle); err != nil {
			return fmt.Errorf("verify block payload for %s/%s before finalisation: %w",
				namespace, diName, err)
		}

		if err := postFinished(ctx, httpClient, url); err != nil {
			return fmt.Errorf("finalise upload for %s/%s: %w", namespace, diName, err)
		}

	default:
		return fmt.Errorf("data import %s/%s reports unsupported volumeMode %q", namespace, diName, volumeMode)
	}

	return nil
}

func verifyPayloadHandle(ctx context.Context, handle *archive.VerifiedHandle) error {
	if handle == nil {
		return nil
	}

	return handle.Verify(ctx)
}

// uploadClient materializes one isolated authenticated transport stack that trusts
// the importer's status.ca. Its caller owns the result for exactly one DataImport
// upload lifecycle and closes its private idle HTTP/1.1 and HTTP/2 pools.
func (c *clusterVolumeImporter) uploadClient(caB64, rawURL string) (uploadHTTPClient, error) {
	ca, err := base64.StdEncoding.DecodeString(caB64)
	if err != nil {
		return nil, fmt.Errorf("decode DataImport status.ca: %w", err)
	}

	if err := safeClient.ValidateHTTPSIdentity(rawURL, ca); err != nil {
		return nil, fmt.Errorf("validate DataImport upload identity: %w", err)
	}

	if c.newUploadClient != nil {
		return c.newUploadClient(ca, rawURL)
	}

	if c.sc == nil {
		return nil, errors.New("build DataImport upload HTTP client: no safe client")
	}

	sub := c.sc.Copy()
	sub.SetRequestTimeout(0)

	if err := sub.SetTLSIdentityCAData(ca); err != nil {
		return nil, fmt.Errorf("configure DataImport upload TLS identity: %w", err)
	}

	if err := sub.SetNetworkTimeouts(safeClient.NetworkTimeouts{
		Connect:        uploadConnectTimeout,
		TLSHandshake:   uploadTLSHandshakeTimeout,
		ResponseHeader: uploadResponseHeaderTimeout,
		WriteIdle:      uploadWriteIdleTimeout,
		ReadIdle:       uploadReadIdleTimeout,
		ResponseTotal:  uploadResponseTotalTimeout,
		ResponseBytes:  uploadResponseByteLimit,
	}); err != nil {
		return nil, fmt.Errorf("configure DataImport upload network timeouts: %w", err)
	}

	httpClient, err := sub.NewPersistentHTTPSClientForOrigin(rawURL)
	if err != nil {
		return nil, fmt.Errorf("build DataImport upload HTTP client: %w", err)
	}

	return httpClient, nil
}

// waitDataImportReady blocks until the DataImport reports Ready=True with a populated
// status.url and volumeMode.
func (c *clusterVolumeImporter) waitDataImportReady(
	ctx context.Context,
	leaf PlannedNode,
	name, namespace string,
) (*unstructured.Unstructured, error) {
	deadline := time.Now().Add(c.wait)

	for {
		di, err := runControlRequest(ctx, c.requestTimeout, c.newRequestContext,
			func(requestCtx context.Context) (*unstructured.Unstructured, error) {
				return c.dyn.Resource(dataImportGVR).Namespace(namespace).
					Get(requestCtx, name, metav1.GetOptions{})
			})
		if err != nil {
			return nil, fmt.Errorf("get DataImport %s/%s: %w", namespace, name, err)
		}

		if err := validateDataImport(di, leaf); err != nil {
			return nil, fmt.Errorf("wait for DataImport %s/%s readiness: %w", namespace, name, err)
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
func (c *clusterVolumeImporter) dataImportCompleted(
	ctx context.Context,
	leaf PlannedNode,
	name, namespace string,
) (bool, error) {
	di, err := runControlRequest(ctx, c.requestTimeout, c.newRequestContext,
		func(requestCtx context.Context) (*unstructured.Unstructured, error) {
			return c.dyn.Resource(dataImportGVR).Namespace(namespace).
				Get(requestCtx, name, metav1.GetOptions{})
		})
	if err != nil {
		if kubeerrors.IsNotFound(err) {
			return false, nil
		}

		return false, fmt.Errorf("get DataImport %s/%s: %w", namespace, name, err)
	}

	if err := validateDataImport(di, leaf); err != nil {
		return false, fmt.Errorf("validate DataImport %s/%s completion identity: %w", namespace, name, err)
	}

	_, hasArtifact, _ := unstructured.NestedMap(di.Object, "status", "data", "artifactRef")

	return conditionTrue(di, conditionCompleted) && hasArtifact, nil
}

// waitDataImportCompleted blocks until the DataImport produces its durable artifact
// (Completed=True with a populated status.data.artifactRef).
func (c *clusterVolumeImporter) waitDataImportCompleted(
	ctx context.Context,
	leaf PlannedNode,
	name, namespace string,
) error {
	deadline := time.Now().Add(c.wait)

	for {
		done, err := c.dataImportCompleted(ctx, leaf, name, namespace)
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

// httpDoer is the minimal HTTP surface putBlock/postFinished need.
type httpDoer interface {
	HTTPDo(req *http.Request) (*http.Response, error)
}

type uploadHTTPClient interface {
	httpDoer
	CloseIdleConnections()
}

type requestBodyRange struct {
	start int64
	end   int64
}

type requestBodyReport struct {
	bodyRange requestBodyRange
	expected  int64
	consumed  int64
	readErr   error
	closeErr  error
	closed    bool
}

func (r requestBodyReport) lifecycleError() error {
	return errors.Join(r.readErr, r.closeErr)
}

func (r requestBodyReport) validateExact() error {
	var validationErr error

	rangeSize := r.bodyRange.end - r.bodyRange.start
	if r.bodyRange.start < 0 || rangeSize < 0 || rangeSize != r.expected {
		validationErr = fmt.Errorf(
			"request body range [%d,%d) has size %d, want declared size %d",
			r.bodyRange.start,
			r.bodyRange.end,
			rangeSize,
			r.expected,
		)
	}

	if r.consumed != r.expected {
		validationErr = errors.Join(
			validationErr,
			fmt.Errorf(
				"request body range [%d,%d) consumed %d bytes, want exactly %d",
				r.bodyRange.start,
				r.bodyRange.end,
				r.consumed,
				r.expected,
			),
		)
	}

	if !r.closed {
		validationErr = errors.Join(validationErr, errors.New("request body did not close"))
	}

	return errors.Join(validationErr, r.lifecycleError())
}

type attestedRequestBody struct {
	mu        sync.Mutex
	body      io.ReadCloser
	bodyRange requestBodyRange
	expected  int64
	consumed  int64
	readErr   error
	closeErr  error
	closed    bool
	closeOnce sync.Once
	done      chan struct{}
}

func newAttestedRequestBody(body io.ReadCloser, bodyRange requestBodyRange, expected int64) *attestedRequestBody {
	return &attestedRequestBody{
		body:      body,
		bodyRange: bodyRange,
		expected:  expected,
		done:      make(chan struct{}),
	}
}

func (b *attestedRequestBody) Read(p []byte) (int, error) {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()

		return 0, http.ErrBodyReadAfterClose
	}

	count, err := b.body.Read(p)
	if count < 0 || count > len(p) {
		readerErr := fmt.Errorf("request body reader returned invalid byte count %d for buffer length %d", count, len(p))
		b.readErr = errors.Join(b.readErr, readerErr)
		b.mu.Unlock()

		return 0, readerErr
	}

	if count > 0 {
		b.consumed += int64(count)

		if b.consumed > b.expected {
			b.readErr = errors.Join(
				b.readErr,
				fmt.Errorf("request body consumed %d bytes, exceeding declared size %d", b.consumed, b.expected),
			)
		}
	}

	if err != nil && !errors.Is(err, io.EOF) {
		b.readErr = errors.Join(b.readErr, err)
	}

	b.mu.Unlock()

	return count, err
}

func (b *attestedRequestBody) Close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		b.closeErr = b.body.Close()
		b.closed = true
		close(b.done)
	})

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.closeErr
}

// NetworkStall records the persistent transport's progress-idle cause and
// closes the body so asynchronous body-completion attestation cannot outlive
// the watchdog that canceled the request.
func (b *attestedRequestBody) NetworkStall(err error) {
	b.mu.Lock()
	b.readErr = errors.Join(b.readErr, err)
	b.mu.Unlock()

	_ = b.Close()
}

func (b *attestedRequestBody) wait(ctx context.Context) (requestBodyReport, error) {
	select {
	case <-b.done:
		return b.report(), nil
	default:
	}

	select {
	case <-b.done:
		return b.report(), nil
	case <-ctx.Done():
		closeErr := b.Close()

		return b.report(), errors.Join(ctx.Err(), closeErr)
	}
}

func (b *attestedRequestBody) report() requestBodyReport {
	b.mu.Lock()
	defer b.mu.Unlock()

	report := requestBodyReport{
		bodyRange: b.bodyRange,
		expected:  b.expected,
		consumed:  b.consumed,
		readErr:   b.readErr,
		closeErr:  b.closeErr,
		closed:    b.closed,
	}

	return report
}

func doAttestedRequest(
	client httpDoer,
	req *http.Request,
	bodyRange requestBodyRange,
) (*http.Response, requestBodyReport, error) {
	if req.ContentLength <= 0 {
		resp, err := client.HTTPDo(req)
		responseErr := drainAndCloseResponseBody(resp)
		report := requestBodyReport{
			bodyRange: bodyRange,
			expected:  req.ContentLength,
			closed:    true,
		}

		return resp, report, errors.Join(err, responseErr)
	}

	if req.Body == nil || req.Body == http.NoBody {
		return nil, requestBodyReport{}, fmt.Errorf(
			"request range [%d,%d) declares %d body bytes but has no body",
			bodyRange.start,
			bodyRange.end,
			req.ContentLength,
		)
	}

	body := newAttestedRequestBody(req.Body, bodyRange, req.ContentLength)
	req.Body = body
	req.GetBody = nil

	resp, requestErr := client.HTTPDo(req)
	responseErr := drainAndCloseResponseBody(resp)
	report, waitErr := body.wait(req.Context())

	return resp, report, errors.Join(requestErr, responseErr, waitErr, report.lifecycleError())
}

func drainAndCloseResponseBody(resp *http.Response) error {
	if resp == nil || resp.Body == nil {
		return nil
	}

	_, drainErr := io.Copy(io.Discard, resp.Body)
	closeErr := resp.Body.Close()
	resp.Body = http.NoBody

	return errors.Join(drainErr, closeErr)
}

// ErrRawBlockSizeMismatch is returned by blockTotalSize when a raw (codec
// none) data.bin file's on-disk size does not match the size captured in the
// archive's VolumeInfo. Unlike a compressed payload, a raw payload has no
// separate decompressed size to fall back on — stat size and captured size
// are the SAME quantity — so any disagreement means a truncated, corrupted, or
// mismatched archive. Checking this before any HEAD/PUT keeps the failure
// deterministic and sends zero HTTP requests, instead of streaming a wrong
// byte count to the importer and only discovering the mismatch mid-transfer.
var ErrRawBlockSizeMismatch = errors.New("raw block size mismatch")

var errFailedBlockDecoderClose = errors.New("failed to close block decoder")

// blockTotalSize returns the exact decompressed byte count of a node's block-volume data
// file without decompressing it. size (a resource.Quantity string like "10Gi", sourced from
// VolumeSnapshotContent.status.restoreSize — see archive.VolumeInfo.Size) is parsed for
// EVERY codec, including raw: a block-volume capture always reads exactly the device's
// provisioned byte size, so the captured size is the total regardless of codec. Parsing
// size instead of decompressing avoids a full decompression pass purely to learn a byte
// count, which is the whole point of the streaming upload path.
//
// For a raw file (ext==""), the parsed size is additionally cross-checked against the
// file's actual on-disk size (os.Stat) — see ErrRawBlockSizeMismatch — because a raw
// payload's stat size and its captured size are definitionally the same number, so any
// difference is a corrupt/mismatched archive rather than a codec-driven size difference.
// A compressed file's on-disk (compressed) size is not comparable to the captured
// (decompressed) size at all, so no such check is possible or meaningful for ext != "".
func blockTotalSize(dataFile, size, ext string) (int64, error) {
	return blockTotalSizeFromSource(dataFile, size, ext, nil)
}

func blockTotalSizeFromSource(dataFile, size, ext string, source interface {
	Stat() (os.FileInfo, error)
}) (int64, error) {
	q, err := resource.ParseQuantity(size)
	if err != nil {
		return 0, fmt.Errorf("parsing captured volume size %q for %s: %w", size, dataFile, err)
	}

	captured := q.Value()

	if ext != "" {
		return captured, nil
	}

	var info os.FileInfo
	if source == nil {
		info, err = os.Stat(dataFile)
	} else {
		info, err = source.Stat()
	}

	if err != nil {
		return 0, fmt.Errorf("stat volume data %s: %w", dataFile, err)
	}

	if info.Size() != captured {
		return 0, fmt.Errorf("%s: on-disk size %d does not match captured volume size %d (%q): %w",
			dataFile, info.Size(), captured, size, ErrRawBlockSizeMismatch)
	}

	return captured, nil
}

// putBlock streams the block-volume payload at dataFile to the importer's block
// endpoint, honouring the server-reported X-Next-Offset for resumable progress. ext
// selects the decode codec via compress.NewReader ("" for raw/no codec, matching
// Codec.Ext); totalSize is the volume's exact decompressed byte count (see
// blockTotalSize). onProgress, when non-nil, is called as validated server offsets make
// raw bytes known durable, including the initial HEAD prefix. activate, when non-nil, is called at the start of every
// real transfer iteration (never when offset==totalSize short-circuits before any PUT is
// attempted), so the caller's progress stream is activated only on a genuine transfer.
type blockArchiveSource interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

type authenticatedReadResetter interface {
	ResetAuthenticatedRead()
}

func resetAuthenticatedRead(source any) {
	if resetter, ok := source.(authenticatedReadResetter); ok {
		resetter.ResetAuthenticatedRead()
	}
}

func putBlock(ctx context.Context, httpClient httpDoer, url, dataFile, ext string, totalSize int64, log *slog.Logger, onProgress func(int), activate func()) error {
	if err := validateBlockOffset(0, totalSize); err != nil {
		return fmt.Errorf("invalid block upload size %d: %w", totalSize, err)
	}

	offset, err := headBlockOffset(ctx, httpClient, url, totalSize)
	if err != nil {
		return err
	}

	progress := blockUploadProgress{onProgress: onProgress}
	progress.creditTo(offset)

	if offset == totalSize && ext == "" {
		return nil
	}

	file, err := os.Open(dataFile)
	if err != nil {
		return fmt.Errorf("open volume data %s: %w", dataFile, err)
	}

	uploadErr := putBlockFromOffset(ctx, httpClient, url, dataFile, ext, totalSize, offset,
		file, log, &progress, activate)

	closeErr := file.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close volume data %s: %w", dataFile, closeErr)
	}

	return errors.Join(uploadErr, closeErr)
}

func putBlockFromSource(ctx context.Context, httpClient httpDoer, url, dataFile, ext string, totalSize int64, source blockArchiveSource, log *slog.Logger, onProgress func(int), activate func()) error {
	if err := validateBlockOffset(0, totalSize); err != nil {
		return fmt.Errorf("invalid block upload size %d: %w", totalSize, err)
	}

	// Resume from the importer's recorded write offset: a reused (non-expired) DataImport
	// may already hold a partial upload, and the block handler rejects a PUT whose X-Offset
	// disagrees with its current offset (HTTP 409), so restarting at 0 would never converge.
	offset, err := headBlockOffset(ctx, httpClient, url, totalSize)
	if err != nil {
		return err
	}

	progress := blockUploadProgress{onProgress: onProgress}
	progress.creditTo(offset)

	return putBlockFromOffset(ctx, httpClient, url, dataFile, ext, totalSize, offset,
		source, log, &progress, activate)
}

func putBlockFromOffset(
	ctx context.Context,
	httpClient httpDoer,
	url, dataFile, ext string,
	totalSize, offset int64,
	source blockArchiveSource,
	log *slog.Logger,
	progress *blockUploadProgress,
	activate func(),
) error {
	// A compressed full skip still has to prove the archive decodes to exactly totalSize.
	// A prior run may have durably written the under-declared prefix and then rejected an
	// extra decoded byte, leaving HEAD at totalSize for this run.
	if offset == totalSize {
		if ext != "" {
			return verifyCompressedBlockSizeFromSource(ctx, source, dataFile, ext, totalSize)
		}

		return nil
	}

	if ext == "" {
		return putBlockRaw(ctx, httpClient, url, source, offset, totalSize, progress, activate)
	}

	return putBlockCompressed(ctx, httpClient, url, source, dataFile, ext, offset, totalSize, log, progress, activate)
}

type blockUploadProgress struct {
	onProgress func(int)
	credited   int64
}

func (p *blockUploadProgress) creditTo(offset int64) {
	if offset <= p.credited {
		return
	}

	if p.onProgress != nil {
		p.onProgress(int(offset - p.credited))
	}

	p.credited = offset
}

type blockConflictTracker struct {
	offsets       [maxConsecutiveBlockConflicts + 1]int64
	count         int
	total         int
	highWater     int64
	replayedBytes int64
}

func newBlockConflictTracker(offset int64) blockConflictTracker {
	return blockConflictTracker{highWater: offset}
}

func (t *blockConflictTracker) observeConflict(from, to int64) error {
	if t.total == maxBlockConflictReplays {
		return fmt.Errorf(
			"too many block upload conflict replays (%d); latest transition from %d to %d",
			maxBlockConflictReplays,
			from,
			to,
		)
	}

	replayBytes := max(t.highWater-to, 0)
	if replayBytes > maxBlockReplayBytes-t.replayedBytes {
		return fmt.Errorf(
			"block upload replay budget exceeded (%d bytes); latest transition from %d to %d would replay %d bytes",
			maxBlockReplayBytes,
			from,
			to,
			replayBytes,
		)
	}

	if to > t.highWater {
		t.highWater = to
		t.reset()
	}

	if t.count == 0 {
		t.offsets[0] = from
		t.count = 1
	}

	for _, offset := range t.offsets[:t.count] {
		if offset == to {
			return fmt.Errorf("server-directed block upload offset loop from %d to %d", from, to)
		}
	}

	if t.count == len(t.offsets) {
		return fmt.Errorf("too many consecutive block upload conflicts (%d)", maxConsecutiveBlockConflicts)
	}

	t.offsets[t.count] = to
	t.count++
	t.total++
	t.replayedBytes += replayBytes

	return nil
}

func (t *blockConflictTracker) observeSuccess(to int64) {
	if to <= t.highWater {
		return
	}

	t.highWater = to
	t.reset()
}

func (t *blockConflictTracker) reset() {
	t.count = 0
}

// putBlockRaw streams an uncompressed data.bin file starting at offset. Every request
// gets a fresh SectionReader limited to the client cap, so neither nginx's 64m ingress
// limit nor a server-directed reposition can make one PUT body unbounded.
func putBlockRaw(ctx context.Context, httpClient httpDoer, url string, source io.ReaderAt, offset, totalSize int64, progress *blockUploadProgress, activate func()) error {
	return putBlockRawWithPayloadLimit(
		ctx,
		httpClient,
		url,
		source,
		offset,
		totalSize,
		blockPutPayloadLimit,
		progress,
		activate,
	)
}

func putBlockRawWithPayloadLimit(
	ctx context.Context,
	httpClient httpDoer,
	url string,
	source io.ReaderAt,
	offset, totalSize, payloadLimit int64,
	progress *blockUploadProgress,
	activate func(),
) error {
	conflicts := newBlockConflictTracker(offset)

	for offset < totalSize {
		if activate != nil {
			activate()
		}

		requestEnd := offset + min(payloadLimit, totalSize-offset)

		resetAuthenticatedRead(source)

		section := io.NewSectionReader(source, offset, requestEnd-offset)

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, io.NopCloser(section))
		if err != nil {
			return err
		}

		req.ContentLength = requestEnd - offset
		req.Header.Set("X-Content-Length", strconv.FormatInt(totalSize, 10))
		req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))
		req.Header.Set("X-Attribute-Permissions", blockAttrPermissions)
		req.Header.Set("X-Attribute-Uid", blockAttrUID)
		req.Header.Set("X-Attribute-Gid", blockAttrGID)

		next, reposition, err := doBlockChunk(httpClient, req, offset, requestEnd, totalSize)
		if err != nil {
			return err
		}

		if reposition {
			if err := conflicts.observeConflict(offset, next); err != nil {
				return err
			}
		} else {
			conflicts.observeSuccess(next)
		}

		progress.creditTo(next)
		offset = next
	}

	return nil
}

// putBlockCompressed streams a compressed data.bin.<ext> file starting at offset,
// decoding it on the fly via compress.NewReader instead of decompressing it into a
// temporary file first — the whole point of this path is to keep peak disk usage at one
// copy (the compressed archive) instead of two.
//
// RESUME STRATEGY has three cases (see resolveBlockDecodeReader):
//
//  1. offset == 0: no positioning needed — compress.NewReader(ext, f) from the start.
//  2. zstd and offset > 0: derive the fixed raw-frame index from
//     volume.DefaultChunkSize, walk only zstd frame headers to its compressed boundary,
//     then decode and discard only the intra-frame raw prefix.
//  3. gzip/lz4, or a failed zstd frame-walk attempt: reset f to byte zero, open a fresh
//     decoder, and discard offset decoded bytes. Gzip and lz4 deliberately retain this
//     O(offset) compatibility path because neither codec is user-selectable and neither
//     has the bounded header-walk integration implemented for zstd. The "none" codec
//     never reaches this function; putBlock routes it to putBlockRaw.
func putBlockCompressed(ctx context.Context, httpClient httpDoer, url string, source io.ReadSeeker, dataFile, ext string, offset, totalSize int64, log *slog.Logger, progress *blockUploadProgress, activate func()) error {
	return putBlockCompressedWithPayloadLimit(
		ctx,
		httpClient,
		url,
		source,
		dataFile,
		ext,
		offset,
		totalSize,
		blockPutPayloadLimit,
		log,
		progress,
		activate,
	)
}

func putBlockCompressedWithPayloadLimit(
	ctx context.Context,
	httpClient httpDoer,
	url string,
	source io.ReadSeeker,
	dataFile, ext string,
	offset, totalSize, payloadLimit int64,
	log *slog.Logger,
	progress *blockUploadProgress,
	activate func(),
) error {
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind compressed block %s: %w", dataFile, err)
	}

	decodeReader, _, err := resolveBlockDecodeReader(ctx, source, dataFile, ext, offset, log)
	if err != nil {
		return err
	}

	defer func() {
		if decodeReader != nil {
			_ = decodeReader.Close()
		}
	}()

	conflicts := newBlockConflictTracker(offset)

	for offset < totalSize {
		if activate != nil {
			activate()
		}

		requestEnd := offset + min(payloadLimit, totalSize-offset)
		requestSize := requestEnd - offset
		limited := io.LimitReader(decodeReader, requestSize)

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, io.NopCloser(limited))
		if err != nil {
			return err
		}

		// net/http only auto-detects Content-Length for *bytes.Buffer/*bytes.Reader/
		// *strings.Reader bodies; an io.LimitReader-wrapped decode stream needs it set
		// explicitly, or the request silently falls back to chunked transfer encoding.
		// Setting it also gives us the "too-large declared size" safety net for free: if
		// the decode stream runs dry before delivering requestSize bytes, net/http's transport
		// refuses to send a body shorter than its declared Content-Length and doBlockChunk
		// returns that error instead of silently truncating the request.
		req.ContentLength = requestSize

		req.Header.Set("X-Content-Length", strconv.FormatInt(totalSize, 10))
		req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))
		req.Header.Set("X-Attribute-Permissions", blockAttrPermissions)
		req.Header.Set("X-Attribute-Uid", blockAttrUID)
		req.Header.Set("X-Attribute-Gid", blockAttrGID)

		next, reposition, err := doBlockChunk(httpClient, req, offset, requestEnd, totalSize)
		if err != nil {
			return fmt.Errorf("%s: declared size %d bytes may not match the archive's actual decompressed content: %w", dataFile, totalSize, err)
		}

		if reposition {
			if err := conflicts.observeConflict(offset, next); err != nil {
				return err
			}
		} else {
			conflicts.observeSuccess(next)
		}

		progress.creditTo(next)

		if reposition {
			if closeErr := decodeReader.Close(); closeErr != nil {
				return fmt.Errorf("%w for %s before repositioning to offset %d: %w",
					errFailedBlockDecoderClose, dataFile, next, closeErr)
			}

			decodeReader = nil
			offset = next

			if offset == totalSize {
				break
			}

			if _, err := source.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("reset compressed block %s before repositioning to offset %d: %w", dataFile, offset, err)
			}

			decodeReader, _, err = resolveBlockDecodeReader(ctx, source, dataFile, ext, offset, log)
			if err != nil {
				return fmt.Errorf("reposition block decoder for %s to offset %d: %w", dataFile, offset, err)
			}

			continue
		}

		offset = next
	}

	// Zstd resume paths may have skipped whole compressed frames whose payload checksums
	// were never decoded in this invocation. Re-decode the complete stream before
	// finalisation so a frame that yielded its declared bytes before a terminal checksum
	// error cannot be accepted on a later retry.
	if ext == ".zst" {
		if decodeReader != nil {
			if closeErr := decodeReader.Close(); closeErr != nil {
				return fmt.Errorf("%w for %s before payload verification: %w",
					errFailedBlockDecoderClose, dataFile, closeErr)
			}

			decodeReader = nil
		}

		return verifyCompressedBlockSizeFromSource(ctx, source, dataFile, ext, totalSize)
	}

	// Safety net: totalSize came from the archive's captured metadata (leaf.Size), never
	// from decoding, so verify it wasn't an UNDER-count. If the decode stream still has
	// bytes left after every declared byte was sent, the archive is corrupt or was built
	// by a mismatched version — fail loudly instead of silently truncating the transfer
	// and reporting success (an under-count would otherwise write a truncated device and
	// still exit clean).
	var probe [1]byte

	if decodeReader == nil {
		return verifyCompressedBlockSizeFromSource(ctx, source, dataFile, ext, totalSize)
	}

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

func verifyCompressedBlockSizeFromSource(ctx context.Context, source io.ReadSeeker, dataFile, ext string, totalSize int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	resetAuthenticatedRead(source)

	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind compressed block %s for decoded-size verification: %w", dataFile, err)
	}

	if ext == ".zst" {
		decodedSize, proofErr := compress.ZstdDecodedSize(source)
		if proofErr != nil {
			return wrapCompressedBlockProofError(dataFile, proofErr)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if err := validateCompressedBlockSize(dataFile, decodedSize, totalSize); err != nil {
			return err
		}

		// Frame_Content_Size is only a bounded size preflight. A zstd decoder can
		// emit exactly that many bytes and report payload/checksum corruption only
		// on its terminal read, so finalisation requires a fresh authenticated
		// decoding pass through EOF.
		resetAuthenticatedRead(source)

		if _, err := source.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("rewind compressed block %s for payload verification: %w", dataFile, err)
		}
	}

	decodeReader, err := compress.NewReader(ext, source)
	if err != nil {
		return fmt.Errorf("open decompressor for %s size verification: %w", dataFile, err)
	}

	decodedSize, proofErr := countDecodedBlock(ctx, decodeReader, totalSize)
	decodeCloseErr := decodeReader.Close()

	if proofErr != nil || decodeCloseErr != nil {
		return errors.Join(
			wrapCompressedBlockProofError(dataFile, proofErr),
			wrapCompressedBlockDecoderCloseError(dataFile, decodeCloseErr),
		)
	}

	return validateCompressedBlockSize(dataFile, decodedSize, totalSize)
}

func countDecodedBlock(ctx context.Context, reader io.Reader, totalSize int64) (int64, error) {
	if totalSize < 0 {
		return 0, fmt.Errorf("negative expected decoded size %d", totalSize)
	}

	buf := make([]byte, blockDiscardBufferSize)

	var decodedSize int64

	for {
		if err := ctx.Err(); err != nil {
			return decodedSize, err
		}

		readBuf := buf
		if decodedSize == totalSize {
			readBuf = readBuf[:1]
		} else if remaining := totalSize - decodedSize; remaining < int64(len(readBuf)) {
			readBuf = readBuf[:remaining+1]
		}

		n, err := reader.Read(readBuf)
		decodedSize += int64(n)

		if decodedSize > totalSize {
			return decodedSize, nil
		}

		if errors.Is(err, io.EOF) {
			return decodedSize, nil
		}

		if err != nil {
			return decodedSize, fmt.Errorf("decode compressed block: %w", err)
		}

		if n == 0 {
			return decodedSize, io.ErrNoProgress
		}
	}
}

func validateCompressedBlockSize(dataFile string, decodedSize, totalSize int64) error {
	if decodedSize != totalSize {
		return fmt.Errorf("%s: declared size %d bytes does not match verified decoded size %d", dataFile, totalSize, decodedSize)
	}

	return nil
}

func wrapCompressedBlockProofError(dataFile string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("verify decoded size of %s: %w", dataFile, err)
}

func wrapCompressedBlockDecoderCloseError(dataFile string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("close decompressor after verifying %s: %w", dataFile, err)
}

type blockDecodeDependencies struct {
	skipZstdFrames func(io.ReadSeeker, int) (int64, error)
	newReader      func(string, io.Reader) (io.ReadCloser, error)
}

// resolveBlockDecodeReader returns a decode reader positioned at the requested
// decompressed offset. Production zstd frames use volume.DefaultChunkSize; tests
// can pass their own geometry to resolveBlockDecodeReaderWith. The discarded count
// is zero for a fresh upload, at most one frame for a successful zstd frame-skip,
// and offset for the byte-zero gzip/lz4/zstd fallback.
func resolveBlockDecodeReader(ctx context.Context, f io.ReadSeeker, dataFile, ext string, offset int64, log *slog.Logger) (io.ReadCloser, int64, error) {
	deps := blockDecodeDependencies{
		skipZstdFrames: compress.SkipZstdFrames,
		newReader:      compress.NewReader,
	}

	return resolveBlockDecodeReaderWith(ctx, f, dataFile, ext, offset, volume.DefaultChunkSize, log, deps)
}

func resolveBlockDecodeReaderWith(
	ctx context.Context,
	f io.ReadSeeker,
	dataFile, ext string,
	offset, chunkSize int64,
	log *slog.Logger,
	deps blockDecodeDependencies,
) (io.ReadCloser, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	if offset == 0 {
		decodeReader, err := deps.newReader(ext, f)
		if err != nil {
			return nil, 0, fmt.Errorf("open decompressor for %s: %w", dataFile, err)
		}

		return decodeReader, 0, nil
	}

	if ext == ".zst" {
		decodeReader, skipped, fastErr := resolveZstdFrameDecodeReader(ctx, f, dataFile, offset, chunkSize, deps)
		if fastErr == nil {
			return decodeReader, skipped, nil
		}

		if errors.Is(fastErr, context.Canceled) || errors.Is(fastErr, context.DeadlineExceeded) {
			return nil, 0, fastErr
		}

		if errors.Is(fastErr, errFailedBlockDecoderClose) {
			return nil, 0, fastErr
		}

		log.Warn("zstd frame-skip resume failed; falling back to byte-zero discard",
			slog.String("file", dataFile),
			slog.Any("error", fastErr))

		return discardFromStart(ctx, f, dataFile, ext, offset, log, deps.newReader, fastErr)
	}

	return discardFromStart(ctx, f, dataFile, ext, offset, log, deps.newReader, nil)
}

func resolveZstdFrameDecodeReader(
	ctx context.Context,
	f io.ReadSeeker,
	dataFile string,
	offset, chunkSize int64,
	deps blockDecodeDependencies,
) (io.ReadCloser, int64, error) {
	if chunkSize <= 0 {
		return nil, 0, fmt.Errorf("zstd frame size must be positive, got %d", chunkSize)
	}

	chunkIndex := offset / chunkSize
	intra := offset % chunkSize

	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	frameOffset, err := deps.skipZstdFrames(f, int(chunkIndex))
	if err != nil {
		return nil, 0, fmt.Errorf("locating zstd frame %d for %s: %w", chunkIndex, dataFile, err)
	}

	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	if _, err := f.Seek(frameOffset, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("seeking %s to zstd frame %d at compressed offset %d: %w", dataFile, chunkIndex, frameOffset, err)
	}

	decodeReader, err := deps.newReader(".zst", f)
	if err != nil {
		return nil, 0, fmt.Errorf("open zstd decompressor for %s at frame %d: %w", dataFile, chunkIndex, err)
	}

	skipped, err := discardDecoded(ctx, decodeReader, intra)
	if err != nil {
		discardErr := fmt.Errorf("discarding intra-frame prefix for %s at raw offset %d (got %d of %d bytes): %w",
			dataFile, offset, skipped, intra, err)

		closeErr := decodeReader.Close()
		if closeErr != nil {
			return nil, 0, errors.Join(discardErr, fmt.Errorf("%w for %s: %w", errFailedBlockDecoderClose, dataFile, closeErr))
		}

		return nil, 0, discardErr
	}

	return decodeReader, skipped, nil
}

// discardFromStart resets f unconditionally before opening the fallback decoder.
// The reset is required even for codecs whose normal reader construction has not
// consumed bytes because a preceding failed zstd attempt may leave f anywhere.
func discardFromStart(
	ctx context.Context,
	f io.ReadSeeker,
	dataFile, ext string,
	offset int64,
	log *slog.Logger,
	newReader func(string, io.Reader) (io.ReadCloser, error),
	fastErr error,
) (io.ReadCloser, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		resetErr := fmt.Errorf("resetting %s before byte-zero resume fallback: %w", dataFile, err)

		return nil, 0, errors.Join(fastErr, resetErr)
	}

	decodeReader, err := newReader(ext, f)
	if err != nil {
		openErr := fmt.Errorf("open decompressor for %s: %w", dataFile, err)

		return nil, 0, errors.Join(fastErr, openErr)
	}

	start := time.Now()

	skipped, err := discardDecoded(ctx, decodeReader, offset)
	if err != nil {
		discardErr := fmt.Errorf("fast-forwarding %s to resume offset %d (got %d bytes): %w", dataFile, offset, skipped, err)

		closeErr := decodeReader.Close()
		if closeErr != nil {
			discardErr = errors.Join(discardErr, fmt.Errorf("%w for %s: %w", errFailedBlockDecoderClose, dataFile, closeErr))
		}

		return nil, 0, errors.Join(fastErr, discardErr)
	}

	log.Info("discarded already-uploaded bytes from the start",
		slog.String("file", dataFile),
		slog.Int64("bytes", skipped),
		slog.Duration("took", time.Since(start)))

	return decodeReader, skipped, nil
}

func discardDecoded(ctx context.Context, r io.Reader, count int64) (int64, error) {
	buf := make([]byte, blockDiscardBufferSize)

	var discarded int64

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	for discarded < count {
		if err := ctx.Err(); err != nil {
			return discarded, err
		}

		remaining := count - discarded

		readBuf := buf
		if remaining < int64(len(readBuf)) {
			readBuf = readBuf[:remaining]
		}

		n, err := r.Read(readBuf)
		discarded += int64(n)

		if discarded == count {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return discarded, ctxErr
			}

			return discarded, nil
		}

		if err != nil {
			return discarded, err
		}

		if n == 0 {
			return discarded, io.ErrNoProgress
		}
	}

	return discarded, nil
}

// headBlockOffset asks the importer (HEAD) how many bytes it has already durably written so
// an interrupted upload can resume. A missing object means "start at 0"; a successful
// producer response must carry its X-Next-Offset contract header.
func headBlockOffset(ctx context.Context, httpClient httpDoer, url string, totalSize int64) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, err
	}

	resp, err := httpClient.HTTPDo(req)
	if err != nil {
		return 0, err
	}

	if err := drainAndCloseResponseBody(resp); err != nil {
		return 0, fmt.Errorf("drain HEAD %s response: %w", url, err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		next := resp.Header.Get("X-Next-Offset")
		if next == "" {
			return 0, fmt.Errorf("HEAD %s returned no X-Next-Offset header", url)
		}

		off, perr := strconv.ParseInt(next, 10, 64)
		if perr != nil {
			return 0, fmt.Errorf("invalid X-Next-Offset %q from %s: %w", next, url, perr)
		}

		if err := validateBlockOffset(off, totalSize); err != nil {
			return 0, fmt.Errorf("invalid X-Next-Offset %q from %s: %w", next, url, err)
		}

		return off, nil
	case http.StatusNotFound:
		if err := validateBlockOffset(0, totalSize); err != nil {
			return 0, fmt.Errorf("invalid implicit X-Next-Offset 0 from %s: %w", url, err)
		}

		return 0, nil
	default:
		return 0, fmt.Errorf("HEAD %s returned status %d (%s)", url, resp.StatusCode, resp.Status)
	}
}

// doBlockChunk performs one bounded PUT. Successful producer responses must acknowledge
// exactly requestEnd; a conflict returns the producer's validated reposition offset.
func doBlockChunk(httpClient httpDoer, req *http.Request, offset, requestEnd, totalSize int64) (int64, bool, error) {
	if err := validateBlockOffset(offset, totalSize); err != nil {
		return 0, false, fmt.Errorf("invalid PUT start offset: %w", err)
	}

	if err := validateBlockOffset(requestEnd, totalSize); err != nil {
		return 0, false, fmt.Errorf("invalid PUT end offset: %w", err)
	}

	if requestEnd <= offset {
		return 0, false, fmt.Errorf("invalid PUT range [%d,%d)", offset, requestEnd)
	}

	resp, bodyReport, err := doAttestedRequest(httpClient, req, requestBodyRange{start: offset, end: requestEnd})
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}

	if err != nil {
		return 0, false, err
	}

	if resp == nil {
		return 0, false, errors.New("PUT returned neither a response nor an error")
	}

	if resp.StatusCode == http.StatusConflict {
		expectedStr := resp.Header.Get("X-Expected-Offset")
		if expectedStr == "" {
			return 0, false, fmt.Errorf("server conflict at offset %d returned no X-Expected-Offset header", offset)
		}

		expected, parseErr := strconv.ParseInt(expectedStr, 10, 64)
		if parseErr != nil {
			return 0, false, fmt.Errorf("invalid X-Expected-Offset %q: %w", expectedStr, parseErr)
		}

		if err := validateBlockOffset(expected, totalSize); err != nil {
			return 0, false, fmt.Errorf("invalid X-Expected-Offset %q: %w", expectedStr, err)
		}

		if expected == offset {
			return 0, false, fmt.Errorf("server returned non-progressing X-Expected-Offset %d for PUT at offset %d", expected, offset)
		}

		return expected, true, nil
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return 0, false, fmt.Errorf("server error at offset %d: status %d (%s)", offset, resp.StatusCode, resp.Status)
	}

	if err := bodyReport.validateExact(); err != nil {
		return 0, false, fmt.Errorf("attest successful PUT body: %w", err)
	}

	wantStatus := http.StatusNoContent
	if requestEnd == totalSize {
		wantStatus = http.StatusCreated
	}

	if resp.StatusCode != wantStatus {
		return 0, false, fmt.Errorf(
			"server returned status %d (%s) for PUT range [%d,%d), want %d (%s)",
			resp.StatusCode,
			resp.Status,
			offset,
			requestEnd,
			wantStatus,
			http.StatusText(wantStatus),
		)
	}

	nextStr := resp.Header.Get("X-Next-Offset")
	if nextStr == "" {
		return 0, false, fmt.Errorf("successful PUT at offset %d returned no X-Next-Offset header", offset)
	}

	next, err := strconv.ParseInt(nextStr, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("invalid X-Next-Offset %q: %w", nextStr, err)
	}

	if err := validateBlockOffset(next, totalSize); err != nil {
		return 0, false, fmt.Errorf("invalid X-Next-Offset %q: %w", nextStr, err)
	}

	if next != requestEnd {
		return 0, false, fmt.Errorf("server returned X-Next-Offset %d, want exact request end %d", next, requestEnd)
	}

	return next, false, nil
}

func validateBlockOffset(offset, totalSize int64) error {
	if offset < 0 || offset > totalSize {
		return fmt.Errorf("offset %d is outside [0,%d]", offset, totalSize)
	}

	return nil
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

	if err := drainAndCloseResponseBody(resp); err != nil {
		return fmt.Errorf("drain finished response: %w", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("finished returned status %d (%s)", resp.StatusCode, resp.Status)
	}

	return nil
}
