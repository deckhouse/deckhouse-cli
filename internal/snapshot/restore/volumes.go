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
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	diV1alpha1 "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/api/v1alpha1"
	diUtil "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/util"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	uploadChunkSize = 64 * 1024 * 1024 // 64 MiB per PUT chunk for block volumes
	defaultTTL      = "60m"
)

// VolumeRestorer creates DataImport objects and uploads volume data from the archive.
type VolumeRestorer struct {
	SafeClient *safeClient.SafeClient
	TTL        string
	Log        *slog.Logger
}

// Restore performs the full DataImport lifecycle for one VolumeOp:
// create DataImport → wait Ready → upload data → POST finished → wait Completed.
func (r *VolumeRestorer) Restore(ctx context.Context, op VolumeOp, targetNS string) error {
	ttl := r.TTL
	if ttl == "" {
		ttl = defaultTTL
	}

	diName := dataImportName(op.NodeID, op.VSCName)

	r.Log.Info("restoring volume via DataImport",
		slog.String("di", diName),
		slog.String("namespace", targetNS),
		slog.String("pvc", op.PVCName),
		slog.String("mode", op.VolumeMode),
	)

	rtClient, err := r.SafeClient.NewRTClient(diV1alpha1.AddToScheme)
	if err != nil {
		return fmt.Errorf("build runtime client: %w", err)
	}

	pvcTemplate := buildPVCTemplate(op)

	if err := diUtil.CreateDataImport(ctx, diName, targetNS, ttl, false, false, pvcTemplate, rtClient); err != nil {
		return fmt.Errorf("create DataImport %s: %w", diName, err)
	}

	// PrepareUpload waits until DataImport becomes Ready and returns:
	//   typedURL  — either "<base>/api/v1/block" or "<base>/api/v1/files"
	//   volumeMode — "Block" or "Filesystem"
	//   subClient  — copy of SafeClient with DataImport's CA in trust pool
	typedURL, volumeMode, subClient, err := diUtil.PrepareUpload(ctx, diName, targetNS, false, r.SafeClient, r.Log)
	if err != nil {
		return fmt.Errorf("prepare DataImport upload %s: %w", diName, err)
	}

	// Derive the pod base URL by stripping the well-known typed endpoint suffix.
	podURL := typedURLToBase(typedURL, volumeMode)

	if volumeMode != op.VolumeMode {
		r.Log.Warn("volume mode mismatch — uploading as DataImport reports",
			slog.String("archive", op.VolumeMode),
			slog.String("dataimport", volumeMode),
		)
	}

	r.Log.Info("uploading volume data",
		slog.String("di", diName),
		slog.String("mode", volumeMode),
		slog.String("dataPath", op.DataPath),
	)

	switch volumeMode {
	case "Block":
		if err := r.uploadBlock(ctx, subClient, podURL, op); err != nil {
			return fmt.Errorf("upload block volume for %s: %w", diName, err)
		}
	case "Filesystem":
		if err := r.uploadFilesystem(ctx, subClient, podURL, op.DataPath); err != nil {
			return fmt.Errorf("upload filesystem volume for %s: %w", diName, err)
		}
	default:
		return fmt.Errorf("unsupported volume mode %q", volumeMode)
	}

	if err := diUtil.FinishUpload(ctx, subClient, podURL); err != nil {
		return fmt.Errorf("finish upload for DataImport %s: %w", diName, err)
	}

	r.Log.Info("waiting for DataImport to complete", slog.String("di", diName))

	if err := diUtil.WaitUploadCompleted(ctx, diName, targetNS, rtClient, r.Log); err != nil {
		return fmt.Errorf("wait for DataImport %s: %w", diName, err)
	}

	r.Log.Info("volume restored", slog.String("di", diName), slog.String("pvc", op.PVCName))

	return nil
}

// typedURLToBase strips the typed path suffix from the URL returned by
// PrepareUpload, yielding the pod base URL needed for finish and other calls.
func typedURLToBase(typedURL, volumeMode string) string {
	switch volumeMode {
	case "Filesystem":
		return strings.TrimSuffix(typedURL, "/api/v1/files")
	case "Block":
		return strings.TrimSuffix(typedURL, "/api/v1/block")
	}

	// Fallback: strip everything after the last meaningful path component.
	if idx := strings.LastIndex(typedURL, "/api/"); idx > 0 {
		return typedURL[:idx]
	}

	return typedURL
}

// uploadBlock streams a block volume image to PUT /api/v1/block.
// It handles both raw (.img) and multi-member gzip (.img.gz) archives.
func (r *VolumeRestorer) uploadBlock(ctx context.Context, httpClient *safeClient.SafeClient, podURL string, op VolumeOp) error {
	blockURL, err := neturl.JoinPath(podURL, "api/v1/block")
	if err != nil {
		return err
	}

	f, err := os.Open(op.DataPath)
	if err != nil {
		return fmt.Errorf("open block data %s: %w", op.DataPath, err)
	}
	defer f.Close()

	var reader io.Reader
	totalBytes := op.BytesTotal

	if op.Compression == "gzip" {
		gz, gzErr := gzip.NewReader(f)
		if gzErr != nil {
			return fmt.Errorf("open gzip reader for %s: %w", op.DataPath, gzErr)
		}
		defer gz.Close()

		reader = gz
	} else {
		if totalBytes == 0 {
			fi, statErr := f.Stat()
			if statErr == nil {
				totalBytes = fi.Size()
			}
		}

		reader = f
	}

	buf := make([]byte, uploadChunkSize)
	var offset int64

	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			if putErr := putBlockChunk(ctx, httpClient, blockURL, buf[:n], offset, totalBytes); putErr != nil {
				return putErr
			}

			offset += int64(n)
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}

		if readErr != nil {
			return fmt.Errorf("read block data: %w", readErr)
		}
	}

	r.Log.Debug("block upload complete", slog.Int64("bytes", offset))

	return nil
}

// uploadFilesystem walks the archive filesystem directory and uploads each
// file to PUT /api/v1/files/{relpath}.
func (r *VolumeRestorer) uploadFilesystem(ctx context.Context, httpClient *safeClient.SafeClient, podURL, dataDir string) error {
	filesBase, err := neturl.JoinPath(podURL, "api/v1/files")
	if err != nil {
		return err
	}

	return filepath.WalkDir(dataDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			return nil
		}

		// Relative path inside the volume.
		rel, relErr := filepath.Rel(dataDir, path)
		if relErr != nil {
			return relErr
		}

		// Strip .gz suffix — the server path uses the original filename.
		relTarget := rel
		isGzip := strings.HasSuffix(path, ".gz")

		if isGzip {
			relTarget = strings.TrimSuffix(rel, ".gz")
		}

		fileURL, joinErr := neturl.JoinPath(filesBase, filepath.ToSlash(relTarget))
		if joinErr != nil {
			return joinErr
		}

		return r.putFile(ctx, httpClient, fileURL, path, isGzip)
	})
}

// putFile uploads a single file. Gzip files are decompressed in memory so
// that X-Content-Length reflects the uncompressed size.
func (r *VolumeRestorer) putFile(ctx context.Context, httpClient *safeClient.SafeClient, fileURL, localPath string, isGzip bool) error {
	var body []byte
	var err error

	if isGzip {
		body, err = decompressGzip(localPath)
	} else {
		body, err = os.ReadFile(localPath)
	}

	if err != nil {
		return fmt.Errorf("read %s: %w", localPath, err)
	}

	totalSize := int64(len(body))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, fileURL,
		strings.NewReader(string(body)))
	if err != nil {
		return err
	}

	req.ContentLength = totalSize
	req.Header.Set("X-Content-Length", strconv.FormatInt(totalSize, 10))
	req.Header.Set("X-Offset", "0")
	req.Header.Set("X-Attribute-Permissions", "0644")
	req.Header.Set("X-Attribute-Uid", "0")
	req.Header.Set("X-Attribute-Gid", "0")

	resp, err := httpClient.HTTPDo(req)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", fileURL, err)
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s returned status %d", fileURL, resp.StatusCode)
	}

	return nil
}

// putBlockChunk sends one chunk of block data to PUT /api/v1/block.
func putBlockChunk(ctx context.Context, httpClient *safeClient.SafeClient, blockURL string, data []byte, offset, totalBytes int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, blockURL,
		strings.NewReader(string(data)))
	if err != nil {
		return err
	}

	req.ContentLength = int64(len(data))
	req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))

	if totalBytes > 0 {
		req.Header.Set("X-Content-Length", strconv.FormatInt(totalBytes, 10))
	}

	req.Header.Set("X-Attribute-Permissions", "0644")
	req.Header.Set("X-Attribute-Uid", "0")
	req.Header.Set("X-Attribute-Gid", "0")

	resp, err := httpClient.HTTPDo(req)
	if err != nil {
		return fmt.Errorf("PUT block at offset %d: %w", offset, err)
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("PUT block at offset %d returned status %d", offset, resp.StatusCode)
	}

	return nil
}

// decompressGzip reads and decompresses a gzip file into memory.
func decompressGzip(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gzip reader %s: %w", path, err)
	}
	defer gz.Close()

	data, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("decompress %s: %w", path, err)
	}

	return data, nil
}

// buildPVCTemplate converts a VolumeOp into the DataImport PVC template spec.
func buildPVCTemplate(op VolumeOp) *diV1alpha1.PersistentVolumeClaimTemplateSpec {
	spec := op.PVCSpec

	pvcName := op.PVCName
	var accessModes []string
	var storageClassName, volumeMode, storageRequest string

	if spec != nil {
		if spec.Name != "" {
			pvcName = spec.Name
		}

		accessModes = spec.AccessModes
		storageClassName = spec.StorageClassName
		volumeMode = spec.VolumeMode
		storageRequest = spec.StorageRequest
	}

	if pvcName == "" {
		pvcName = "restore-" + op.VSCName
	}

	if len(accessModes) == 0 {
		accessModes = []string{"ReadWriteOnce"}
	}

	if volumeMode == "" {
		volumeMode = op.VolumeMode
	}

	if storageRequest == "" {
		storageRequest = BytesToStorage(op.BytesTotal)
	}

	diAccessModes := make([]diV1alpha1.PersistentVolumeAccessMode, 0, len(accessModes))
	for _, m := range accessModes {
		diAccessModes = append(diAccessModes, diV1alpha1.PersistentVolumeAccessMode(m))
	}

	vm := diV1alpha1.PersistentVolumeMode(volumeMode)
	qty := resource.MustParse(storageRequest)

	tpl := &diV1alpha1.PersistentVolumeClaimTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName},
		PersistentVolumeClaimSpec: diV1alpha1.PersistentVolumeClaimSpec{
			AccessModes: diAccessModes,
			VolumeMode:  &vm,
			Resources: diV1alpha1.VolumeResourceRequirements{
				Requests: diV1alpha1.ResourceList{
					diV1alpha1.ResourceStorage: qty,
				},
			},
		},
	}

	if storageClassName != "" {
		sc := storageClassName
		tpl.PersistentVolumeClaimSpec.StorageClassName = &sc
	}

	return tpl
}

// dataImportName generates a stable, short DataImport name from nodeID and VSCName.
func dataImportName(nodeID, vscName string) string {
	sum := sha256.Sum256([]byte(nodeID + "/" + vscName))
	return "snap-di-" + hex.EncodeToString(sum[:8])
}
