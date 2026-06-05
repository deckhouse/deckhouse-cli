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

	deV1alpha1 "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/util"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	volumeSnapshotContentKind = "VolumeSnapshotContent"
	concurrentFileDownloads   = 4
)

// downloadNodeVolumes downloads all volume data for node n into the archive data directory.
// For each DataRef it creates a temporary DataExport (kind=VolumeSnapshotContent), waits for
// Ready, downloads the data, appends a VolumeProgressRecord, and deletes the DataExport.
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

// downloadOneVolume downloads a single volume identified by dr.
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
	deName := volumeDataExportName(n.ID, dr.VSCName)

	log.Info("creating DataExport for volume", "node", n.ID, "vsc", dr.VSCName, "de", deName)

	if err := util.CreateDataExport(ctx, deName, n.Namespace, opts.DataExportTTL,
		volumeSnapshotContentKind, dr.VSCName, false, deRTClient); err != nil {
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

	log.Info("downloading volume data", "node", n.ID, "vsc", dr.VSCName, "mode", volumeMode)

	key := archive.VolumeProgressKey(n.ID, dr.VSCName)
	existing := existingVolProgress[key]

	switch volumeMode {
	case "Block":
		if err := downloadBlockVolume(ctx, subClient, w, n.ID, dr, downloadURL, existing, log); err != nil {
			return err
		}
	case "Filesystem":
		if err := downloadFilesystemVolume(ctx, subClient, w, n.ID, dr, downloadURL, existing, log); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported volumeMode %q for VSC %s", volumeMode, dr.VSCName)
	}

	return nil
}

// downloadBlockVolume downloads a block-mode volume using HTTP Range for resume.
func downloadBlockVolume(
	ctx context.Context,
	sClient *safeClient.SafeClient,
	w *archive.DirWriter,
	nodeID string,
	dr source.DataRef,
	baseURL string,
	existing archive.VolumeProgressRecord,
	log *slog.Logger,
) error {
	outDir := filepath.Join(w.DataDir(), nodeID)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}

	outPath := filepath.Join(outDir, dr.VSCName+".img")

	var offset int64
	if info, err := os.Stat(outPath); err == nil {
		offset = info.Size()
	}

	if offset > 0 && existing.BytesTotal > 0 && offset >= existing.BytesTotal {
		// Already fully written; mark complete.
		return w.AppendVolumeProgress(archive.VolumeProgressRecord{
			NodeID:     nodeID,
			VSCName:    dr.VSCName,
			PVCName:    dr.PVCName,
			VolumeMode: "Block",
			BytesDone:  offset,
			BytesTotal: existing.BytesTotal,
			Complete:   true,
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
		NodeID:     nodeID,
		VSCName:    dr.VSCName,
		PVCName:    dr.PVCName,
		VolumeMode: "Block",
		BytesDone:  totalBytes,
		BytesTotal: bytesTotal,
		Complete:   true,
	})
}

// downloadFilesystemVolume recursively downloads a filesystem-mode volume.
func downloadFilesystemVolume(
	ctx context.Context,
	sClient *safeClient.SafeClient,
	w *archive.DirWriter,
	nodeID string,
	dr source.DataRef,
	baseURL string,
	_ archive.VolumeProgressRecord,
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
	if err := recursiveVolumeDownload(ctx, sClient, log, sem, baseURL, "/", outDir); err != nil {
		return fmt.Errorf("recursive download: %w", err)
	}

	log.Info("filesystem volume downloaded", "node", nodeID, "vsc", dr.VSCName, "dir", outDir)

	return w.AppendVolumeProgress(archive.VolumeProgressRecord{
		NodeID:     nodeID,
		VSCName:    dr.VSCName,
		PVCName:    dr.PVCName,
		VolumeMode: "Filesystem",
		Complete:   true,
	})
}

type dirItem struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func recursiveVolumeDownload(ctx context.Context, sClient *safeClient.SafeClient, log *slog.Logger, sem chan struct{}, baseURL, srcPath, dstDir string) error {
	if err := ctx.Err(); err != nil {
		return err
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

	if srcPath != "" && srcPath[len(srcPath)-1] == '/' {
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
				setFirstErr(recursiveVolumeDownload(ctx, sClient, log, sem, baseURL, srcPath+sp, filepath.Join(dstDir, sp)))
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

	out, err := os.Create(dstDir)
	if err != nil {
		return fmt.Errorf("create file %s: %w", dstDir, err)
	}

	defer out.Close()

	if _, err := io.Copy(out, resp.Body); err != nil {
		return fmt.Errorf("copy file %s: %w", dstDir, err)
	}

	log.Debug("downloaded file", "path", dstDir)

	return nil
}

// volumeDataExportName generates a deterministic short name for a DataExport
// created during snapshot volume download.
func volumeDataExportName(nodeID, vscName string) string {
	sum := sha256.Sum256([]byte(nodeID + "/" + vscName))
	return "snap-de-" + hex.EncodeToString(sum[:8])
}
