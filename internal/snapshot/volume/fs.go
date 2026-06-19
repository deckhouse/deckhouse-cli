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

package volume

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
)

// fsItem is one file-system entry collected from the data-exporter listing.
// It holds both the download URL (for file type) and the metadata needed to
// build a TarEntry for the final data.tar assembly.
type fsItem struct {
	relPath  string
	itemType string // "file", "dir", or "link"
	uri      string // download URL; non-empty only for itemType == "file"
	mode     fs.FileMode
	uid      int
	gid      int
	mtime    time.Time
	linkname string // symlink target; non-empty only for itemType == "link"
}

// DownloadFilesystemVolume downloads all files from the data-exporter filesystem
// volume at filesRootURL, stages them as raw bytes under stagingDir, then
// assembles a single uncompressed PAX tar at tarPath.
//
// If tarPath already exists the whole operation is skipped (resume: tar complete).
// Already-staged files under stagingDir are not re-downloaded (partial resume).
// The stagingDir is removed on successful tar assembly.
//
// workers bounds the parallelism for file downloads; the first error cancels all
// in-flight downloads.
func DownloadFilesystemVolume(
	ctx context.Context,
	log *slog.Logger,
	tarPath string,
	stagingDir string,
	filesRootURL string,
	workers int,
	fetcher *exporter.Fetcher,
) error {
	// Resume: completed tar → skip entirely.
	if _, err := os.Stat(tarPath); err == nil {
		log.Info("fs tar already present, skipping", slog.String("path", tarPath))

		return nil
	}

	if workers <= 0 {
		workers = 1
	}

	if err := archive.EnsureDir(stagingDir); err != nil {
		return fmt.Errorf("create staging dir %s: %w", stagingDir, err)
	}

	base, err := url.Parse(filesRootURL)
	if err != nil {
		return fmt.Errorf("parse files root URL %q: %w", filesRootURL, err)
	}

	items, err := collectAllFSItems(ctx, fetcher, filesRootURL, base, "")
	if err != nil {
		return fmt.Errorf("list filesystem volume: %w", err)
	}

	log.Info("staging filesystem volume",
		slog.String("tar", tarPath),
		slog.Int("items", len(items)),
		slog.Int("workers", workers))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for _, it := range items {
		if it.itemType != "file" {
			continue
		}

		item := it

		g.Go(func() error {
			return stageRawFile(gctx, log, stagingDir, item, fetcher)
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("stage filesystem files: %w", err)
	}

	entries := make([]TarEntry, 0, len(items))

	for _, it := range items {
		entries = append(entries, TarEntry{
			RelPath:  it.relPath,
			Type:     it.itemType,
			Mode:     it.mode,
			UID:      it.uid,
			GID:      it.gid,
			Mtime:    it.mtime,
			Linkname: it.linkname,
		})
	}

	if err := WriteTar(tarPath, stagingDir, entries); err != nil {
		return fmt.Errorf("assemble tar %s: %w", tarPath, err)
	}

	log.Info("fs tar assembled", slog.String("path", tarPath))

	if err := os.RemoveAll(stagingDir); err != nil {
		log.Warn("failed to remove FS staging dir",
			slog.String("dir", stagingDir),
			slog.String("error", err.Error()))
	}

	return nil
}

// collectAllFSItems recursively walks the listing at dirURL and accumulates
// all items (file, dir, link) with their relative paths and metadata.
// filesRootURL is the absolute URL of the volume root used to resolve relative URIs.
// relPrefix is prepended to item names when building the relative path.
func collectAllFSItems(ctx context.Context, fetcher *exporter.Fetcher, dirURL string, base *url.URL, relPrefix string) ([]fsItem, error) {
	items, err := fetcher.ListDir(ctx, dirURL)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", dirURL, err)
	}

	var result []fsItem

	for _, item := range items {
		ref, err := url.Parse(item.URI)
		if err != nil {
			return nil, fmt.Errorf("parse item URI %q: %w", item.URI, err)
		}

		absURI := base.ResolveReference(ref).String()
		relPath := relPrefix + item.Name
		mode, uid, gid, mtime := parseItemAttrs(item.Attributes)

		switch item.Type {
		case "file":
			result = append(result, fsItem{
				relPath:  relPath,
				itemType: "file",
				uri:      absURI,
				mode:     mode,
				uid:      uid,
				gid:      gid,
				mtime:    mtime,
			})

		case "dir":
			result = append(result, fsItem{
				relPath:  relPath,
				itemType: "dir",
				mode:     mode,
				uid:      uid,
				gid:      gid,
				mtime:    mtime,
			})

			subPrefix := relPath + "/"

			subItems, err := collectAllFSItems(ctx, fetcher, absURI, base, subPrefix)
			if err != nil {
				return nil, err
			}

			result = append(result, subItems...)

		case "link":
			result = append(result, fsItem{
				relPath:  relPath,
				itemType: "link",
				mode:     mode,
				uid:      uid,
				gid:      gid,
				mtime:    mtime,
				linkname: item.TargetPath,
			})

		default:
			// Unknown or error items: skip silently (forward-compatible).
		}
	}

	return result, nil
}

// stageRawFile downloads one file as raw bytes and writes it atomically to
// stagingDir/<relPath>. Already-complete destination files are skipped.
// Stale *.tmp files are removed before the download attempt.
func stageRawFile(
	ctx context.Context,
	log *slog.Logger,
	stagingDir string,
	item fsItem,
	fetcher *exporter.Fetcher,
) error {
	destPath := filepath.Join(stagingDir, filepath.FromSlash(item.relPath))

	if _, err := os.Stat(destPath); err == nil {
		log.Info("staging file already present, skipping", slog.String("path", item.relPath))

		return nil
	}

	tmpPath := destPath + ".tmp"

	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale tmp %s: %w", tmpPath, err)
	}

	parentDir := filepath.Dir(destPath)

	if err := archive.EnsureDir(parentDir); err != nil {
		return fmt.Errorf("create parent dir %s: %w", parentDir, err)
	}

	log.Info("staging fs file", slog.String("path", item.relPath))

	body, err := fetcher.GetFile(ctx, item.uri)
	if err != nil {
		return fmt.Errorf("GET %s: %w", item.uri, err)
	}

	defer func() { _ = body.Close() }()

	aw, err := archive.NewAtomicWriter(destPath)
	if err != nil {
		return fmt.Errorf("open atomic writer for %s: %w", destPath, err)
	}

	if _, err := io.Copy(aw, body); err != nil {
		aw.Abort()

		return fmt.Errorf("stage %s: %w", item.relPath, err)
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit staging %s: %w", destPath, err)
	}

	log.Info("staging file written", slog.String("path", item.relPath))

	return nil
}

// parseItemAttrs extracts file metadata from the data-exporter listing attributes map.
// Missing or unrecognised attribute values produce zero values (sensible defaults
// are applied by WriteTar: 0644 for files, 0755 for dirs, 0777 for links).
func parseItemAttrs(attrs map[string]any) (fs.FileMode, int, int, time.Time) {
	var mode fs.FileMode

	var uid, gid int

	var mtime time.Time

	if v, ok := attrs["mode"]; ok {
		if n, ok := v.(float64); ok {
			mode = fs.FileMode(uint32(n))
		}
	}

	if v, ok := attrs["uid"]; ok {
		if n, ok := v.(float64); ok {
			uid = int(n)
		}
	}

	if v, ok := attrs["gid"]; ok {
		if n, ok := v.(float64); ok {
			gid = int(n)
		}
	}

	if v, ok := attrs["mtime"]; ok {
		switch n := v.(type) {
		case float64:
			sec := int64(n)
			nsec := int64((n - float64(sec)) * 1e9)
			mtime = time.Unix(sec, nsec)
		case string:
			if parsed, parseErr := time.Parse(time.RFC3339Nano, n); parseErr == nil {
				mtime = parsed
			}
		}
	}

	return mode, uid, gid, mtime
}
