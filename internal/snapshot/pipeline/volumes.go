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

type NodeVolumesRequest struct {
	SafeClient          *safeClient.SafeClient
	Writer              *archive.DirWriter
	Node                *source.Node
	ExistingVolProgress map[string]archive.VolumeProgressRecord
	Options             Options
	Log                 *slog.Logger
}

type volumeDownloader struct {
	sClient             *safeClient.SafeClient
	writer              *archive.DirWriter
	node                *source.Node
	existingVolProgress map[string]archive.VolumeProgressRecord
	opts                Options
	log                 *slog.Logger
}

type volumeData struct {
	nodeID      string
	ref         source.DataRef
	baseURL     string
	existing    archive.VolumeProgressRecord
	compression string
	outDir      string
}

func downloadNodeVolumes(ctx context.Context, req NodeVolumesRequest) error {
	d := newVolumeDownloader(req)
	deRTClient, err := d.sClient.NewRTClient(deV1alpha1.AddToScheme)
	if err != nil {
		return fmt.Errorf("build DataExport client: %w", err)
	}

	for _, dr := range d.node.DataRefs {
		if d.volumeComplete(dr) {
			continue
		}

		if err := d.downloadOne(ctx, deRTClient, dr); err != nil {
			return fmt.Errorf("download volume %s (node %s): %w", dr.VSCName, d.node.ID, err)
		}
	}

	return nil
}

func newVolumeDownloader(req NodeVolumesRequest) *volumeDownloader {
	return &volumeDownloader{
		sClient:             req.SafeClient,
		writer:              req.Writer,
		node:                req.Node,
		existingVolProgress: req.ExistingVolProgress,
		opts:                req.Options,
		log:                 req.Log,
	}
}

func (d *volumeDownloader) volumeComplete(dr source.DataRef) bool {
	key := archive.VolumeProgressKey(d.node.ID, dr.VSCName)
	rec, ok := d.existingVolProgress[key]
	if !ok || !rec.Complete {
		return false
	}

	d.log.Debug("volume already complete, skipping", "node", d.node.ID, "vsc", dr.VSCName)

	return true
}

func (d *volumeDownloader) downloadOne(ctx context.Context, deRTClient ctrlrtclient.Client, dr source.DataRef) error {
	d.log.Info("preparing shadow snapshot pair for volume", "node", d.node.ID, "vsc", dr.VSCName)

	shadowVSName, cleanupShadow, err := createAndWaitShadowPair(ctx, deRTClient, d.node.ID, d.node.Namespace, dr, d.log)
	defer cleanupShadow()

	if err != nil {
		return fmt.Errorf("create shadow snapshot pair for %s: %w", dr.VSCName, err)
	}

	deName := volumeDataExportName(d.node.ID, dr.VSCName)

	d.log.Info("creating DataExport for volume", "node", d.node.ID, "vsc", dr.VSCName, "de", deName, "shadowVS", shadowVSName)

	if err := util.CreateDataExport(ctx, deName, d.node.Namespace, d.opts.DataExportTTL,
		dataio.VolumeSnapshotKind, shadowVSName, false, deRTClient); err != nil {
		return fmt.Errorf("create DataExport %s: %w", deName, err)
	}

	defer func() {
		if delErr := util.DeleteDataExport(context.Background(), deName, d.node.Namespace, deRTClient); delErr != nil {
			d.log.Warn("failed to delete DataExport", "de", deName, "err", delErr)
		}
	}()

	downloadURL, volumeMode, subClient, err := util.PrepareDownloadFunc(ctx, d.log, deName, d.node.Namespace, false, d.sClient)
	if err != nil {
		return fmt.Errorf("prepare download for DataExport %s: %w", deName, err)
	}

	compression := d.opts.VolumeCompression
	if compression == "" {
		compression = CompressionGzip
	}

	d.log.Info("downloading volume data", "node", d.node.ID, "vsc", dr.VSCName, "mode", volumeMode, "compression", compression)

	key := archive.VolumeProgressKey(d.node.ID, dr.VSCName)
	volume := volumeData{
		nodeID:      d.node.ID,
		ref:         dr,
		baseURL:     downloadURL,
		existing:    d.existingVolProgress[key],
		compression: compression,
	}

	switch volumeMode {
	case "Block":
		return d.downloadBlock(ctx, subClient, volume)
	case "Filesystem":
		return d.downloadFilesystem(ctx, subClient, volume)
	default:
		return fmt.Errorf("unsupported volumeMode %q for VSC %s", volumeMode, dr.VSCName)
	}
}

func (d *volumeDownloader) downloadBlock(ctx context.Context, sClient *safeClient.SafeClient, volume volumeData) error {
	outDir := filepath.Join(d.writer.DataDir(), volume.nodeID)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}

	volume.outDir = outDir

	if volume.compression == CompressionGzip {
		return d.downloadBlockGzip(ctx, sClient, volume)
	}

	return d.downloadBlockRaw(ctx, sClient, volume)
}

func (d *volumeDownloader) downloadBlockRaw(ctx context.Context, sClient *safeClient.SafeClient, volume volumeData) error {
	outPath := filepath.Join(volume.outDir, volume.ref.VSCName+".img")

	var offset int64
	if info, err := os.Stat(outPath); err == nil {
		offset = info.Size()
	}

	if offset > 0 && volume.existing.BytesTotal > 0 && offset >= volume.existing.BytesTotal {
		return d.writer.AppendVolumeProgress(archive.VolumeProgressRecord{
			NodeID:      volume.nodeID,
			VSCName:     volume.ref.VSCName,
			PVCName:     volume.ref.PVCName,
			VolumeMode:  "Block",
			Compression: CompressionNone,
			BytesDone:   offset,
			BytesTotal:  volume.existing.BytesTotal,
			Complete:    true,
		})
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, volume.baseURL, nil)
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

	flags, offset := blockOutputFlags(offset, resp.StatusCode)

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
	bytesTotal := blockBytesTotal(resp.ContentLength, offset, totalBytes)

	d.log.Info("block volume downloaded", "node", volume.nodeID, "vsc", volume.ref.VSCName, "bytes", totalBytes)

	return d.writer.AppendVolumeProgress(archive.VolumeProgressRecord{
		NodeID:      volume.nodeID,
		VSCName:     volume.ref.VSCName,
		PVCName:     volume.ref.PVCName,
		VolumeMode:  "Block",
		Compression: CompressionNone,
		BytesDone:   totalBytes,
		BytesTotal:  bytesTotal,
		Complete:    true,
	})
}

func blockOutputFlags(offset int64, statusCode int) (int, int64) {
	flags := os.O_WRONLY | os.O_CREATE
	if offset > 0 && statusCode == http.StatusPartialContent {
		return flags | os.O_APPEND, offset
	}

	return flags | os.O_TRUNC, 0
}

func blockBytesTotal(contentLength, offset, totalBytes int64) int64 {
	if contentLength < 0 {
		return totalBytes
	}

	if offset > 0 {
		return offset + contentLength
	}

	return contentLength
}

func (d *volumeDownloader) downloadBlockGzip(ctx context.Context, sClient *safeClient.SafeClient, volume volumeData) error {
	outPath := filepath.Join(volume.outDir, volume.ref.VSCName+".img.gz")

	srcOffset := volume.existing.BytesDone
	compressedLen := volume.existing.CompressedBytes

	if srcOffset > 0 && volume.existing.BytesTotal > 0 && srcOffset >= volume.existing.BytesTotal {
		return d.writer.AppendVolumeProgress(archive.VolumeProgressRecord{
			NodeID:          volume.nodeID,
			VSCName:         volume.ref.VSCName,
			PVCName:         volume.ref.PVCName,
			VolumeMode:      "Block",
			Compression:     CompressionGzip,
			BytesDone:       srcOffset,
			BytesTotal:      volume.existing.BytesTotal,
			CompressedBytes: compressedLen,
			Complete:        true,
		})
	}

	f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("open gzip output %s: %w", outPath, err)
	}

	defer f.Close()

	if compressedLen > 0 {
		if err := f.Truncate(compressedLen); err != nil {
			return fmt.Errorf("truncate gzip output to checkpoint %d: %w", compressedLen, err)
		}
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek to end of gzip output: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, volume.baseURL, nil)
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

	var bytesTotal int64
	if resp.ContentLength >= 0 {
		bytesTotal = srcOffset + resp.ContentLength
	}

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

			if appendErr := d.writer.AppendVolumeProgress(archive.VolumeProgressRecord{
				NodeID:          volume.nodeID,
				VSCName:         volume.ref.VSCName,
				PVCName:         volume.ref.PVCName,
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

	d.log.Info("block volume downloaded (gzip)", "node", volume.nodeID, "vsc", volume.ref.VSCName, "srcBytes", srcOffset, "compressedBytes", compressedLen)

	return d.writer.AppendVolumeProgress(archive.VolumeProgressRecord{
		NodeID:          volume.nodeID,
		VSCName:         volume.ref.VSCName,
		PVCName:         volume.ref.PVCName,
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

type filesystemDownloader struct {
	sClient *safeClient.SafeClient
	log     *slog.Logger
	sem     chan struct{}
	baseURL string
	useGzip bool
}

func (d *volumeDownloader) downloadFilesystem(ctx context.Context, sClient *safeClient.SafeClient, volume volumeData) error {
	dirName := volume.ref.PVCName
	if dirName == "" {
		dirName = volume.ref.VSCName
	}

	outDir := filepath.Join(d.writer.DataDir(), volume.nodeID, dirName)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}

	fs := filesystemDownloader{
		sClient: sClient,
		log:     d.log,
		sem:     make(chan struct{}, concurrentFileDownloads),
		baseURL: volume.baseURL,
		useGzip: volume.compression == CompressionGzip,
	}

	if err := fs.download(ctx, "/", outDir); err != nil {
		return fmt.Errorf("recursive download: %w", err)
	}

	d.log.Info("filesystem volume downloaded", "node", volume.nodeID, "vsc", volume.ref.VSCName, "dir", outDir, "compression", volume.compression)

	return d.writer.AppendVolumeProgress(archive.VolumeProgressRecord{
		NodeID:      volume.nodeID,
		VSCName:     volume.ref.VSCName,
		PVCName:     volume.ref.PVCName,
		VolumeMode:  "Filesystem",
		Compression: volume.compression,
		Complete:    true,
	})
}

type dirItem struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (d *filesystemDownloader) download(ctx context.Context, srcPath, dstDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	isDir := srcPath == "" || srcPath[len(srcPath)-1] == '/'

	if !isDir && d.useGzip {
		if _, err := os.Stat(dstDir + ".gz"); err == nil {
			return nil
		}
	}

	dataURL, err := neturl.JoinPath(d.baseURL, srcPath)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dataURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	resp, err := d.sClient.HTTPDo(req)
	if err != nil {
		return fmt.Errorf("HTTPDo: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("HTTP %d %s: %s", resp.StatusCode, srcPath, body)
	}

	if isDir {
		return d.downloadDir(ctx, resp.Body, srcPath, dstDir)
	}

	if srcPath == "" || srcPath == "/" {
		return nil
	}

	if d.useGzip {
		return writeGzipFileAtomic(resp.Body, dstDir)
	}

	return writeRawFile(resp.Body, dstDir)
}

func (d *filesystemDownloader) downloadDir(ctx context.Context, body io.Reader, srcPath, dstDir string) error {
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

	dec := json.NewDecoder(body)
	if err := seekItemsArray(dec); err != nil {
		return err
	}

	for dec.More() {
		var item dirItem

		if err := dec.Decode(&item); err != nil {
			break
		}

		subPath, ok, err := d.prepareDirItem(dstDir, item)
		if err != nil {
			return err
		}

		if !ok {
			continue
		}

		sp := subPath
		downloadOne := func() {
			setFirstErr(d.download(ctx, srcPath+sp, filepath.Join(dstDir, sp)))
		}

		select {
		case d.sem <- struct{}{}:
			wg.Add(1)

			go func() {
				defer func() { <-d.sem; wg.Done() }()

				downloadOne()
			}()
		default:
			downloadOne()
		}
	}

	wg.Wait()

	return firstErr
}

func seekItemsArray(dec *json.Decoder) error {
	for {
		t, err := dec.Token()
		if err != nil {
			return nil
		}

		if t != "items" {
			continue
		}

		_, _ = dec.Token()

		return nil
	}
}

func (d *filesystemDownloader) prepareDirItem(dstDir string, item dirItem) (string, bool, error) {
	switch item.Type {
	case "dir":
		if err := os.MkdirAll(filepath.Join(dstDir, item.Name), 0o755); err != nil {
			return "", false, err
		}

		return item.Name + "/", true, nil
	case "file", "link":
		return item.Name, true, nil
	default:
		d.log.Warn("skipping unsupported entry", "path", item.Name, "type", item.Type)

		return "", false, nil
	}
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
