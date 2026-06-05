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

package pipeline

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"sync"

	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	deV1alpha1 "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/util"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	concurrentFileDownloads = 4

	// blockGzipChunkSize is the uncompressed size of each gzip member written
	// for block volumes. Each chunk is flushed and synced so it forms a complete
	// independent gzip member — this allows safe truncation on resume.
	blockGzipChunkSize = 64 * 1024 * 1024 // 64 MiB

	// CompressionGzip is the gzip compression mode value.
	CompressionGzip = "gzip"
	// CompressionNone is the no-compression mode value.
	CompressionNone = "none"
)

// downloadNodeVolumes downloads all volume data for node n into the archive data directory.
//
// For each DataRef the function:
//  1. Creates a temporary shadow VolumeSnapshotContent + VolumeSnapshot pointing at the
//     original VSC's snapshotHandle (CLI-side bridging; no server-side changes needed).
//  2. Creates a DataExport (kind=VolumeSnapshot) targeting the shadow VS.
//  3. Downloads the data into the archive data directory.
//  4. Cleans up the DataExport and shadow objects on exit.
//
// Already-complete volumes (per existingVolProgress) are skipped.
func downloadNodeVolumes(
	ctx context.Context,
	_ ctrlrtclient.Client,
	sClient *safeClient.SafeClient,
	w *archive.DirWriter,
	n *source.Node,
	existingVolProgress map[string]archive.VolumeProgressRecord,
	opts Options,
	log *slog.Logger,
) error {
	// Create an rtClient that knows about the DataExport CRD.
	deRTClient, err := sClient.NewRTClient(deV1alpha1.AddToScheme)
	if err != nil {
		return fmt.Errorf("build DataExport client: %w", err)
	}

	for _, dr := range n.DataRefs {
		key := archive.VolumeProgressKey(n.ID, dr.VSCName)
		if rec, ok := existingVolProgress[key]; ok && rec.Complete {
			log.Debug("volume already complete, skipping", "node", n.ID, "vsc", dr.VSCName)
			continue
		}

		if err := downloadOneVolume(ctx, deRTClient, sClient, w, n, dr, existingVolProgress, opts, log); err != nil {
			return fmt.Errorf("download volume %s (node %s): %w", dr.VSCName, n.ID, err)
		}
	}

	return nil
}

// downloadOneVolume downloads a single volume identified by dr using the shadow
// VolumeSnapshotContent+VolumeSnapshot approach:
//
//  1. Creates a pre-provisioned shadow VSC pointing at origVSC.status.snapshotHandle.
//  2. Creates a shadow VS with StorageClass/volume-mode annotations.
//  3. Waits for external-snapshotter to bind and mark the shadow VS readyToUse.
//  4. Creates a DataExport(kind=VolumeSnapshot) targeting the shadow VS.
//  5. Downloads via util.PrepareDownloadFunc.
//  6. Deferred cleanup removes the DataExport and shadow pair.
func downloadOneVolume(
	ctx context.Context,
	deRTClient ctrlrtclient.Client,
	sClient *safeClient.SafeClient,
	w *archive.DirWriter,
	n *source.Node,
	dr source.DataRef,
	existingVolProgress map[string]archive.VolumeProgressRecord,
	opts Options,
	log *slog.Logger,
) error {
	log.Info("preparing shadow snapshot pair for volume", "node", n.ID, "vsc", dr.VSCName)

	shadowVSName, cleanupShadow, err := createAndWaitShadowPair(ctx, deRTClient, n.ID, n.Namespace, dr, log)
	defer cleanupShadow()

	if err != nil {
		return fmt.Errorf("create shadow snapshot pair for %s: %w", dr.VSCName, err)
	}

	deName := volumeDataExportName(n.ID, dr.VSCName)

	log.Info("creating DataExport for volume", "node", n.ID, "vsc", dr.VSCName, "de", deName, "shadowVS", shadowVSName)

	if err := util.CreateDataExport(ctx, deName, n.Namespace, opts.DataExportTTL,
		dataio.VolumeSnapshotKind, shadowVSName, false, deRTClient); err != nil {
		return fmt.Errorf("create DataExport %s: %w", deName, err)
	}

	defer func() {
		if delErr := util.DeleteDataExport(context.Background(), deName, n.Namespace, deRTClient); delErr != nil {
			log.Warn("failed to delete DataExport", "de", deName, "err", delErr)
		}
	}()

	downloadURL, volumeMode, subClient, err := util.PrepareDownloadFunc(ctx, log, deName, n.Namespace, false, sClient)
	if err != nil {
		return fmt.Errorf("prepare download for DataExport %s: %w", deName, err)
	}

	compression := opts.VolumeCompression
	if compression == "" {
		compression = CompressionGzip
	}

	log.Info("downloading volume data", "node", n.ID, "vsc", dr.VSCName, "mode", volumeMode, "compression", compression)

	key := archive.VolumeProgressKey(n.ID, dr.VSCName)
	existing := existingVolProgress[key]

	switch volumeMode {
	case "Block":
		if err := downloadBlockVolume(ctx, subClient, w, n.ID, dr, downloadURL, existing, compression, log); err != nil {
			return err
		}
	case "Filesystem":
		if err := downloadFilesystemVolume(ctx, subClient, w, n.ID, dr, downloadURL, compression, log); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported volumeMode %q for VSC %s", volumeMode, dr.VSCName)
	}

	return nil
}

// downloadBlockVolume downloads a block-mode volume.
//
// When compression == "gzip", the output is a multi-member gzip file (<vsc>.img.gz).
// Each chunk (blockGzipChunkSize) is written as one independent gzip member and
// synced, so on resume the file can be safely truncated to the last checkpoint's
// CompressedBytes before appending the next member.
//
// When compression == "none", the output is a raw .img file with HTTP Range resume.
func downloadBlockVolume(
	ctx context.Context,
	sClient *safeClient.SafeClient,
	w *archive.DirWriter,
	nodeID string,
	dr source.DataRef,
	baseURL string,
	existing archive.VolumeProgressRecord,
	compression string,
	log *slog.Logger,
) error {
	outDir := filepath.Join(w.DataDir(), nodeID)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}

	if compression == CompressionGzip {
		return downloadBlockVolumeGzip(ctx, sClient, w, nodeID, dr, baseURL, outDir, existing, log)
	}

	return downloadBlockVolumeRaw(ctx, sClient, w, nodeID, dr, baseURL, outDir, existing, log)
}

// downloadBlockVolumeRaw downloads a block volume without compression (raw .img).
func downloadBlockVolumeRaw(
	ctx context.Context,
	sClient *safeClient.SafeClient,
	w *archive.DirWriter,
	nodeID string,
	dr source.DataRef,
	baseURL string,
	outDir string,
	existing archive.VolumeProgressRecord,
	log *slog.Logger,
) error {
	outPath := filepath.Join(outDir, dr.VSCName+".img")

	var offset int64
	if info, err := os.Stat(outPath); err == nil {
		offset = info.Size()
	}

	if offset > 0 && existing.BytesTotal > 0 && offset >= existing.BytesTotal {
		return w.AppendVolumeProgress(archive.VolumeProgressRecord{
			NodeID:      nodeID,
			VSCName:     dr.VSCName,
			PVCName:     dr.PVCName,
			VolumeMode:  "Block",
			Compression: CompressionNone,
			BytesDone:   offset,
			BytesTotal:  existing.BytesTotal,
			Complete:    true,
		})
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}

	resp, err := sClient.HTTPDo(req)
	if err != nil {
		return fmt.Errorf("HTTP GET block volume: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("unexpected status %d downloading block volume: %s", resp.StatusCode, body)
	}

	flags := os.O_WRONLY | os.O_CREATE
	if offset > 0 && resp.StatusCode == http.StatusPartialContent {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
		offset = 0
	}

	f, err := os.OpenFile(outPath, flags, 0o644)
	if err != nil {
		return fmt.Errorf("open output file %s: %w", outPath, err)
	}

	defer f.Close()

	written, err := io.Copy(f, resp.Body)
	if err != nil {
		return fmt.Errorf("copy block volume data: %w", err)
	}

	totalBytes := offset + written
	bytesTotal := resp.ContentLength

	if bytesTotal < 0 {
		bytesTotal = totalBytes
	} else if offset > 0 {
		bytesTotal = offset + resp.ContentLength
	}

	log.Info("block volume downloaded", "node", nodeID, "vsc", dr.VSCName, "bytes", totalBytes)

	return w.AppendVolumeProgress(archive.VolumeProgressRecord{
		NodeID:      nodeID,
		VSCName:     dr.VSCName,
		PVCName:     dr.PVCName,
		VolumeMode:  "Block",
		Compression: CompressionNone,
		BytesDone:   totalBytes,
		BytesTotal:  bytesTotal,
		Complete:    true,
	})
}

// downloadBlockVolumeGzip downloads a block volume as a multi-member gzip file.
//
// Resume: if existing.CompressedBytes > 0, the output file is truncated to that
// size (dropping any half-written trailing member), then the server is asked for
// Range: bytes=existing.BytesDone- so we continue from the right source offset.
// Each blockGzipChunkSize is one independent gzip member; after each member
// a partial progress record is written so BytesDone + CompressedBytes are durable.
func downloadBlockVolumeGzip(
	ctx context.Context,
	sClient *safeClient.SafeClient,
	w *archive.DirWriter,
	nodeID string,
	dr source.DataRef,
	baseURL string,
	outDir string,
	existing archive.VolumeProgressRecord,
	log *slog.Logger,
) error {
	outPath := filepath.Join(outDir, dr.VSCName+".img.gz")

	srcOffset := existing.BytesDone
	compressedLen := existing.CompressedBytes

	// Already complete from a previous run?
	if srcOffset > 0 && existing.BytesTotal > 0 && srcOffset >= existing.BytesTotal {
		return w.AppendVolumeProgress(archive.VolumeProgressRecord{
			NodeID:          nodeID,
			VSCName:         dr.VSCName,
			PVCName:         dr.PVCName,
			VolumeMode:      "Block",
			Compression:     CompressionGzip,
			BytesDone:       srcOffset,
			BytesTotal:      existing.BytesTotal,
			CompressedBytes: compressedLen,
			Complete:        true,
		})
	}

	// Open (or create) the output file for append.
	f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("open gzip output %s: %w", outPath, err)
	}

	defer f.Close()

	// Truncate to the last durable checkpoint to discard any half-written member.
	if compressedLen > 0 {
		if err := f.Truncate(compressedLen); err != nil {
			return fmt.Errorf("truncate gzip output to checkpoint %d: %w", compressedLen, err)
		}
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek to end of gzip output: %w", err)
	}

	// Fetch data starting at source offset.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	if srcOffset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", srcOffset))
	}

	resp, err := sClient.HTTPDo(req)
	if err != nil {
		return fmt.Errorf("HTTP GET block volume: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}

	// Compute total uncompressed size.
	var bytesTotal int64
	if resp.ContentLength >= 0 {
		bytesTotal = srcOffset + resp.ContentLength
	}

	// Stream body in chunks; each chunk = one independent gzip member.
	buf := make([]byte, blockGzipChunkSize)

	for {
		n, readErr := io.ReadFull(resp.Body, buf)
		if n > 0 {
			if err := appendGzipMember(f, buf[:n]); err != nil {
				return fmt.Errorf("write gzip member at offset %d: %w", srcOffset, err)
			}

			srcOffset += int64(n)

			info, statErr := f.Stat()
			if statErr != nil {
				return fmt.Errorf("stat gzip output: %w", statErr)
			}

			compressedLen = info.Size()

			// Checkpoint after every member (complete=false until final).
			if appendErr := w.AppendVolumeProgress(archive.VolumeProgressRecord{
				NodeID:          nodeID,
				VSCName:         dr.VSCName,
				PVCName:         dr.PVCName,
				VolumeMode:      "Block",
				Compression:     CompressionGzip,
				BytesDone:       srcOffset,
				BytesTotal:      bytesTotal,
				CompressedBytes: compressedLen,
				Complete:        false,
			}); appendErr != nil {
				return fmt.Errorf("checkpoint progress: %w", appendErr)
			}
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}

		if readErr != nil {
			return fmt.Errorf("read block volume body: %w", readErr)
		}
	}

	if bytesTotal == 0 {
		bytesTotal = srcOffset
	}

	log.Info("block volume downloaded (gzip)", "node", nodeID, "vsc", dr.VSCName, "srcBytes", srcOffset, "compressedBytes", compressedLen)

	return w.AppendVolumeProgress(archive.VolumeProgressRecord{
		NodeID:          nodeID,
		VSCName:         dr.VSCName,
		PVCName:         dr.PVCName,
		VolumeMode:      "Block",
		Compression:     CompressionGzip,
		BytesDone:       srcOffset,
		BytesTotal:      bytesTotal,
		CompressedBytes: compressedLen,
		Complete:        true,
	})
}

// appendGzipMember compresses data as one complete gzip member and appends it to f.
// The member is flushed and the file is synced before returning so that
// the data is durable and CompressedBytes can be used as a safe truncation point.
func appendGzipMember(f *os.File, data []byte) error {
	gz := gzip.NewWriter(f)

	if _, err := gz.Write(data); err != nil {
		return err
	}

	if err := gz.Close(); err != nil {
		return err
	}

	return f.Sync()
}

// downloadFilesystemVolume recursively downloads a filesystem-mode volume.
//
// When compression == "gzip", each file is stored as <name>.gz (atomic tmp→rename).
// A file is skipped on resume if its .gz already exists (atomicity guarantees it is complete).
// When compression == "none", files are stored as plain files (same behaviour as before).
func downloadFilesystemVolume(
	ctx context.Context,
	sClient *safeClient.SafeClient,
	w *archive.DirWriter,
	nodeID string,
	dr source.DataRef,
	baseURL string,
	compression string,
	log *slog.Logger,
) error {
	dirName := dr.PVCName
	if dirName == "" {
		dirName = dr.VSCName
	}

	outDir := filepath.Join(w.DataDir(), nodeID, dirName)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}

	sem := make(chan struct{}, concurrentFileDownloads)

	useGzip := compression == CompressionGzip

	if err := recursiveVolumeDownload(ctx, sClient, log, sem, baseURL, "/", outDir, useGzip); err != nil {
		return fmt.Errorf("recursive download: %w", err)
	}

	log.Info("filesystem volume downloaded", "node", nodeID, "vsc", dr.VSCName, "dir", outDir, "compression", compression)

	return w.AppendVolumeProgress(archive.VolumeProgressRecord{
		NodeID:      nodeID,
		VSCName:     dr.VSCName,
		PVCName:     dr.PVCName,
		VolumeMode:  "Filesystem",
		Compression: compression,
		Complete:    true,
	})
}

type dirItem struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func recursiveVolumeDownload(ctx context.Context, sClient *safeClient.SafeClient, log *slog.Logger, sem chan struct{}, baseURL, srcPath, dstDir string, useGzip bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	isDir := srcPath == "" || srcPath[len(srcPath)-1] == '/'

	// Per-file resume: skip HTTP request entirely when the .gz already exists.
	// This is only safe for leaf files (not directories whose listing may change).
	if !isDir && useGzip {
		if _, err := os.Stat(dstDir + ".gz"); err == nil {
			return nil
		}
	}

	dataURL, err := neturl.JoinPath(baseURL, srcPath)
	if err != nil {
		return err
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, dataURL, nil)

	resp, err := sClient.HTTPDo(req)
	if err != nil {
		return fmt.Errorf("HTTPDo: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("HTTP %d %s: %s", resp.StatusCode, srcPath, body)
	}

	if isDir {
		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			firstErr error
		)

		setFirstErr := func(subErr error) {
			if subErr == nil {
				return
			}

			mu.Lock()

			if firstErr == nil {
				firstErr = subErr
			}

			mu.Unlock()
		}

		dec := json.NewDecoder(resp.Body)

		for {
			t, err := dec.Token()
			if err != nil {
				break
			}

			if t == "items" {
				if _, err := dec.Token(); err != nil {
					break
				}

				break
			}
		}

		for dec.More() {
			var item dirItem

			if err := dec.Decode(&item); err != nil {
				break
			}

			subPath := item.Name

			switch item.Type {
			case "dir":
				if mkErr := os.MkdirAll(filepath.Join(dstDir, subPath), 0o755); mkErr != nil {
					return mkErr
				}

				subPath += "/"
			case "file", "link":
				// downloadable
			default:
				log.Warn("skipping unsupported entry", "path", item.Name, "type", item.Type)
				continue
			}

			sp := subPath
			downloadOne := func() {
				setFirstErr(recursiveVolumeDownload(ctx, sClient, log, sem, baseURL, srcPath+sp, filepath.Join(dstDir, sp), useGzip))
			}

			select {
			case sem <- struct{}{}:
				wg.Add(1)

				go func() {
					defer func() { <-sem; wg.Done() }()

					downloadOne()
				}()
			default:
				downloadOne()
			}
		}

		wg.Wait()

		return firstErr
	}

	// Leaf file.
	if srcPath == "" || srcPath == "/" {
		return nil
	}

	if useGzip {
		return writeGzipFileAtomic(resp.Body, dstDir)
	}

	return writeRawFile(resp.Body, dstDir)
}

// writeGzipFileAtomic compresses body and writes it to path+".gz" atomically
// (write to .tmp -> fsync -> rename). A pre-existing .gz is skipped (resume).
func writeGzipFileAtomic(body io.Reader, path string) error {
	outPath := path + ".gz"

	// Skip if already downloaded (atomic write guarantees completeness).
	if _, err := os.Stat(outPath); err == nil {
		return nil
	}

	tmpPath := outPath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create tmp %s: %w", tmpPath, err)
	}

	cleanup := func() { _ = os.Remove(tmpPath) }

	gz := gzip.NewWriter(f)

	if _, err := io.Copy(gz, body); err != nil {
		_ = f.Close()

		cleanup()

		return fmt.Errorf("compress file: %w", err)
	}

	if err := gz.Close(); err != nil {
		_ = f.Close()

		cleanup()

		return fmt.Errorf("close gzip writer: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()

		cleanup()

		return fmt.Errorf("sync tmp file: %w", err)
	}

	if err := f.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close tmp file: %w", err)
	}

	if err := os.Rename(tmpPath, outPath); err != nil {
		cleanup()
		return fmt.Errorf("rename to %s: %w", outPath, err)
	}

	return nil
}

// writeRawFile writes body to path without compression.
func writeRawFile(body io.Reader, path string) error {
	out, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create file %s: %w", path, err)
	}

	defer out.Close()

	if _, err := io.Copy(out, body); err != nil {
		return fmt.Errorf("copy file %s: %w", path, err)
	}

	return nil
}

// volumeDataExportName generates a deterministic short name for a DataExport
// created during snapshot volume download.
func volumeDataExportName(nodeID, vscName string) string {
	sum := sha256.Sum256([]byte(nodeID + "/" + vscName))
	return "snap-de-" + hex.EncodeToString(sum[:8])
}
