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
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

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

	blockDiscardBufferSize = 32 * 1024
	blockPutPayloadLimit   = 32 * 1024 * 1024

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
		existing, err := ri.Get(ctx, name, metav1.GetOptions{})
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

	httpClient, err := c.uploadClient(caB64)
	if err != nil {
		return err
	}

	if err := c.sendVolumeData(ctx, httpClient, url, volumeMode, leaf, namespace, diName, setTotal, onProgress, activate); err != nil {
		return err
	}

	return c.waitDataImportCompleted(ctx, leaf, diName, namespace)
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
		ext := leaf.Ext

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
		// and matches the onProgress increments (validated durable offsets advance the bar),
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
func (c *clusterVolumeImporter) waitDataImportReady(
	ctx context.Context,
	leaf PlannedNode,
	name, namespace string,
) (*unstructured.Unstructured, error) {
	deadline := time.Now().Add(c.wait)

	for {
		di, err := c.dyn.Resource(dataImportGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
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
	di, err := c.dyn.Resource(dataImportGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
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

// httpDoer is the minimal HTTP surface putBlock/postFinished need; *safeClient.SafeClient
// satisfies it, and tests stub it.
type httpDoer interface {
	HTTPDo(req *http.Request) (*http.Response, error)
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
	q, err := resource.ParseQuantity(size)
	if err != nil {
		return 0, fmt.Errorf("parsing captured volume size %q for %s: %w", size, dataFile, err)
	}

	captured := q.Value()

	if ext != "" {
		return captured, nil
	}

	info, err := os.Stat(dataFile)
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
func putBlock(ctx context.Context, httpClient httpDoer, url, dataFile, ext string, totalSize int64, log *slog.Logger, onProgress func(int), activate func()) error {
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

	// offset == totalSize is a legitimate resume of an upload that fully transferred before
	// the previous run finalized it: nothing left to send, so skip building a decode reader
	// (or even opening the file) entirely and let the caller finalize.
	if offset == totalSize {
		return nil
	}

	if ext == "" {
		return putBlockRaw(ctx, httpClient, url, dataFile, offset, totalSize, &progress, activate)
	}

	return putBlockCompressed(ctx, httpClient, url, dataFile, ext, offset, totalSize, log, &progress, activate)
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

// putBlockRaw streams an uncompressed data.bin file starting at offset. Every request
// gets a fresh SectionReader limited to the client cap, so neither nginx's 64m ingress
// limit nor a server-directed reposition can make one PUT body unbounded.
func putBlockRaw(ctx context.Context, httpClient httpDoer, url, filePath string, offset, totalSize int64, progress *blockUploadProgress, activate func()) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	requestedOffsets := make(map[int64]struct{})

	for offset < totalSize {
		if _, repeated := requestedOffsets[offset]; repeated {
			return fmt.Errorf("block upload offset loop at %d", offset)
		}

		requestedOffsets[offset] = struct{}{}

		if activate != nil {
			activate()
		}

		requestEnd := offset + min(blockPutPayloadLimit, totalSize-offset)
		section := io.NewSectionReader(file, offset, requestEnd-offset)

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
			if _, repeated := requestedOffsets[next]; repeated {
				return fmt.Errorf("server-directed block upload offset loop from %d to %d", offset, next)
			}
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
func putBlockCompressed(ctx context.Context, httpClient httpDoer, url, dataFile, ext string, offset, totalSize int64, log *slog.Logger, progress *blockUploadProgress, activate func()) error {
	f, err := os.Open(dataFile)
	if err != nil {
		return fmt.Errorf("open volume data %s: %w", dataFile, err)
	}

	defer func() { _ = f.Close() }()

	decodeReader, _, err := resolveBlockDecodeReader(ctx, f, dataFile, ext, offset, log)
	if err != nil {
		return err
	}

	defer func() {
		if decodeReader != nil {
			_ = decodeReader.Close()
		}
	}()

	requestedOffsets := make(map[int64]struct{})

	for offset < totalSize {
		if _, repeated := requestedOffsets[offset]; repeated {
			return fmt.Errorf("block upload offset loop at %d", offset)
		}

		requestedOffsets[offset] = struct{}{}

		if activate != nil {
			activate()
		}

		requestEnd := offset + min(blockPutPayloadLimit, totalSize-offset)
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
			if _, repeated := requestedOffsets[next]; repeated {
				return fmt.Errorf("server-directed block upload offset loop from %d to %d", offset, next)
			}
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

			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("reset compressed block %s before repositioning to offset %d: %w", dataFile, offset, err)
			}

			decodeReader, _, err = resolveBlockDecodeReader(ctx, f, dataFile, ext, offset, log)
			if err != nil {
				return fmt.Errorf("reposition block decoder for %s to offset %d: %w", dataFile, offset, err)
			}

			continue
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

	if decodeReader == nil {
		return nil
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

type blockDecodeDependencies struct {
	skipZstdFrames func(io.ReadSeeker, int) (int64, error)
	newReader      func(string, io.Reader) (io.ReadCloser, error)
}

// resolveBlockDecodeReader returns a decode reader positioned at the requested
// decompressed offset. The discarded count is zero for a fresh upload, at most
// volume.DefaultChunkSize-1 for a successful zstd frame-skip, and offset for the
// byte-zero gzip/lz4/zstd fallback.
func resolveBlockDecodeReader(ctx context.Context, f io.ReadSeeker, dataFile, ext string, offset int64, log *slog.Logger) (io.ReadCloser, int64, error) {
	deps := blockDecodeDependencies{
		skipZstdFrames: compress.SkipZstdFrames,
		newReader:      compress.NewReader,
	}

	return resolveBlockDecodeReaderWith(ctx, f, dataFile, ext, offset, log, deps)
}

func resolveBlockDecodeReaderWith(
	ctx context.Context,
	f io.ReadSeeker,
	dataFile, ext string,
	offset int64,
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
		decodeReader, skipped, fastErr := resolveZstdFrameDecodeReader(ctx, f, dataFile, offset, deps)
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
	offset int64,
	deps blockDecodeDependencies,
) (io.ReadCloser, int64, error) {
	chunkIndex := offset / volume.DefaultChunkSize
	intra := offset % volume.DefaultChunkSize

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

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

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

	resp, err := httpClient.HTTPDo(req)
	if err != nil {
		return 0, false, err
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

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

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("finished returned status %d (%s)", resp.StatusCode, resp.Status)
	}

	return nil
}
