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
	"log/slog"
	"net/url"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
)

// fsFileJob is a single file to be downloaded in DownloadFilesystemVolume.
type fsFileJob struct {
	// relPath is the path relative to the filesystem root (forward-slash separated).
	relPath string
	// uri is the full URL to GET the file content from.
	uri string
}

// DownloadFilesystemVolume recursively lists the filesystem volume starting at
// filesRootURL and downloads each file into dataDir/<relpath>.zst, preserving
// directory structure. Symlink items ("link") are recorded but not followed.
//
// dataDir is the absolute path to the target data directory (the caller
// constructs it using archive.DataDirName or archive.MultiVolumeDir for
// multi-volume layouts).
//
// Already-complete destination files are skipped. Stale *.tmp files are removed
// before a download attempt. workers bounds parallelism; the first error cancels
// all in-flight downloads.
func DownloadFilesystemVolume(
	ctx context.Context,
	log *slog.Logger,
	dataDir string,
	filesRootURL string,
	workers int,
	fetcher *exporter.Fetcher,
	enc *compress.Encoder,
) error {
	if workers <= 0 {
		workers = 1
	}

	if err := archive.EnsureDir(dataDir); err != nil {
		return fmt.Errorf("create data dir %s: %w", dataDir, err)
	}

	// Collect all file jobs via a recursive listing walk (serial).
	jobs, err := collectFSFiles(ctx, fetcher, filesRootURL, filesRootURL, "")
	if err != nil {
		return fmt.Errorf("list filesystem volume: %w", err)
	}

	log.Info("downloading filesystem volume",
		slog.String("dir", dataDir),
		slog.Int("files", len(jobs)),
		slog.Int("workers", workers))

	// Download jobs in parallel with bounded concurrency.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for _, job := range jobs {
		j := job

		g.Go(func() error {
			return downloadFSFile(gctx, log, dataDir, j, fetcher, enc)
		})
	}

	return g.Wait()
}

// collectFSFiles recursively walks the listing at dirURL, accumulating file jobs.
// filesRootURL is the absolute URL of the volume root; item URIs are resolved
// against it because the data-exporter returns root-relative paths, not absolute URLs.
// relPrefix is the path prefix for items inside this directory (forward-slash).
func collectFSFiles(ctx context.Context, fetcher *exporter.Fetcher, dirURL, filesRootURL, relPrefix string) ([]fsFileJob, error) {
	base, err := url.Parse(filesRootURL)
	if err != nil {
		return nil, fmt.Errorf("parse files root URL %q: %w", filesRootURL, err)
	}

	return collectFSFilesRec(ctx, fetcher, dirURL, base, relPrefix)
}

// collectFSFilesRec is the internal recursive worker for collectFSFiles.
func collectFSFilesRec(ctx context.Context, fetcher *exporter.Fetcher, dirURL string, base *url.URL, relPrefix string) ([]fsFileJob, error) {
	items, err := fetcher.ListDir(ctx, dirURL)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", dirURL, err)
	}

	var jobs []fsFileJob

	for _, item := range items {
		ref, err := url.Parse(item.URI)
		if err != nil {
			return nil, fmt.Errorf("parse item URI %q: %w", item.URI, err)
		}

		absURI := base.ResolveReference(ref).String()

		switch item.Type {
		case "file":
			relPath := relPrefix + item.Name
			jobs = append(jobs, fsFileJob{relPath: relPath, uri: absURI})

		case "dir":
			subPrefix := relPrefix + item.Name + "/"

			subJobs, err := collectFSFilesRec(ctx, fetcher, absURI, base, subPrefix)
			if err != nil {
				return nil, err
			}

			jobs = append(jobs, subJobs...)

		case "link":
			// Symlinks are recorded (visible in listing) but not followed to
			// avoid escaping the volume boundary. They produce no output file.

		default:
			// Unknown or error items: skip silently (forward-compatible).
		}
	}

	return jobs, nil
}

// downloadFSFile downloads one file, zstd-encodes it as a streaming frame, and
// writes it atomically to dataDir/<relPath>.zst.
func downloadFSFile(
	ctx context.Context,
	log *slog.Logger,
	dataDir string,
	job fsFileJob,
	fetcher *exporter.Fetcher,
	enc *compress.Encoder,
) error {
	// Normalise to OS path separators.
	destRel := filepath.FromSlash(archive.FsFileName(job.relPath))
	destPath := filepath.Join(dataDir, destRel)

	// Skip files already fully written.
	if _, err := os.Stat(destPath); err == nil {
		log.Info("fs file already present, skipping", slog.String("path", job.relPath))

		return nil
	}

	// Remove any stale temporary file from a previous aborted attempt.
	tmpPath := destPath + ".tmp"

	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale tmp %s: %w", tmpPath, err)
	}

	// Ensure parent directory exists.
	parentDir := filepath.Dir(destPath)

	if err := archive.EnsureDir(parentDir); err != nil {
		return fmt.Errorf("create parent dir %s: %w", parentDir, err)
	}

	log.Info("downloading fs file", slog.String("path", job.relPath))

	body, err := fetcher.GetFile(ctx, job.uri)
	if err != nil {
		return fmt.Errorf("GET %s: %w", job.uri, err)
	}

	defer func() { _ = body.Close() }()

	// Stream-encode directly into the atomic writer to avoid buffering the
	// full file content in memory.
	aw, err := archive.NewAtomicWriter(destPath)
	if err != nil {
		return fmt.Errorf("open atomic writer for %s: %w", destPath, err)
	}

	if err := enc.EncodeStream(aw, body); err != nil {
		aw.Abort()
		return fmt.Errorf("encode %s: %w", job.relPath, err)
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", destPath, err)
	}

	log.Info("fs file written", slog.String("path", job.relPath))

	return nil
}
