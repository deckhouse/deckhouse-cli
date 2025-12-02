/*
Copyright 2024 Flant JSC

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

package export

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	"github.com/deckhouse/deckhouse-cli/internal/data/domain"
	"github.com/deckhouse/deckhouse-cli/internal/data/usecase"
)

// DownloadUseCase handles data download from DataExport
type DownloadUseCase struct {
	repo       usecase.DataExportRepository
	httpClient usecase.HTTPClient
	fs         usecase.FileSystem
	logger     usecase.Logger
}

// NewDownloadUseCase creates a new DownloadUseCase
func NewDownloadUseCase(
	repo usecase.DataExportRepository,
	httpClient usecase.HTTPClient,
	fs usecase.FileSystem,
	logger usecase.Logger,
) *DownloadUseCase {
	return &DownloadUseCase{
		repo:       repo,
		httpClient: httpClient,
		fs:         fs,
		logger:     logger,
	}
}

// DownloadParams contains parameters for downloading data
type DownloadParams struct {
	DataName  string // DataExport name or volume reference (e.g., "pvc/my-volume")
	Namespace string
	SrcPath   string
	DstPath   string
	Publish   bool
	TTL       string
}

// DownloadResult contains the result of a download operation
type DownloadResult struct {
	ExportName    string
	WasCreated    bool // true if DataExport was created during download
	FilesDownloaded int
}

// Execute downloads data from a DataExport
func (uc *DownloadUseCase) Execute(ctx context.Context, params *DownloadParams) (*DownloadResult, error) {
	result := &DownloadResult{}

	// Check if we need to create a DataExport
	exportName, volumeRef, needsCreate := domain.GenerateExportName(params.DataName)
	result.ExportName = exportName
	result.WasCreated = needsCreate

	if needsCreate {
		ttl := params.TTL
		if ttl == "" {
			ttl = domain.DefaultTTL
		}
		createParams := &domain.CreateExportParams{
			Name:       exportName,
			Namespace:  params.Namespace,
			TTL:        ttl,
			VolumeKind: volumeRef.Kind,
			VolumeName: volumeRef.Name,
			Publish:    params.Publish,
		}
		if err := uc.repo.Create(ctx, createParams); err != nil {
			return nil, fmt.Errorf("create DataExport: %w", err)
		}
		uc.logger.Info("DataExport created", "name", exportName, "namespace", params.Namespace)
	}

	// Wait for DataExport to be ready and get URL
	export, err := uc.repo.GetWithRetry(ctx, exportName, params.Namespace)
	if err != nil {
		return nil, fmt.Errorf("get DataExport: %w", err)
	}

	// Prepare HTTP client with CA if needed
	httpClient := uc.httpClient
	if !params.Publish && export.Status.CA != "" {
		httpClient = uc.httpClient.Copy()
		caData, err := base64.StdEncoding.DecodeString(export.Status.CA)
		if err != nil {
			return nil, fmt.Errorf("decode CA: %w", err)
		}
		httpClient.SetCA(caData)
	}

	// Build download URL
	baseURL := export.Status.URL
	if params.Publish && export.Status.PublicURL != "" {
		baseURL = export.Status.PublicURL
	}

	var downloadURL string
	switch export.Status.VolumeMode {
	case domain.VolumeModeFilesystem:
		downloadURL, err = url.JoinPath(baseURL, "api/v1/files")
	case domain.VolumeModeBlock:
		downloadURL, err = url.JoinPath(baseURL, "api/v1/block")
	default:
		return nil, fmt.Errorf("%w: %s", domain.ErrUnsupportedVolumeMode, export.Status.VolumeMode)
	}
	if err != nil {
		return nil, fmt.Errorf("build URL: %w", err)
	}

	// Determine source and destination paths
	srcPath := params.SrcPath
	dstPath := params.DstPath

	switch export.Status.VolumeMode {
	case domain.VolumeModeFilesystem:
		if srcPath == "" {
			return nil, fmt.Errorf("source path is required for Filesystem mode")
		}
		if dstPath == "" {
			pathList := strings.Split(srcPath, "/")
			dstPath = pathList[len(pathList)-1]
		}
	case domain.VolumeModeBlock:
		srcPath = ""
		if dstPath == "" {
			dstPath = exportName
		}
	}

	uc.logger.Info("Starting download", "url", downloadURL+srcPath, "dst", dstPath)

	// Perform download
	sem := make(chan struct{}, 10) // concurrency limit
	count, err := uc.recursiveDownload(ctx, httpClient, sem, downloadURL, srcPath, dstPath)
	result.FilesDownloaded = count

	if err != nil {
		uc.logger.Error("Download failed", "error", err.Error())
		return result, err
	}

	uc.logger.Info("Download completed", "files", count, "dst", dstPath)
	return result, nil
}

// DeleteCreatedExport deletes a DataExport that was created during download
func (uc *DownloadUseCase) DeleteCreatedExport(ctx context.Context, name, namespace string) error {
	return uc.repo.Delete(ctx, name, namespace)
}

type dirItem struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (uc *DownloadUseCase) recursiveDownload(
	ctx context.Context,
	client usecase.HTTPClient,
	sem chan struct{},
	baseURL, srcPath, dstPath string,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	dataURL, err := url.JoinPath(baseURL, srcPath)
	if err != nil {
		return 0, err
	}

	body, statusCode, err := client.Get(ctx, dataURL)
	if err != nil {
		return 0, fmt.Errorf("HTTP GET: %w", err)
	}
	defer body.Close()

	if statusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(body, 1000))
		return 0, fmt.Errorf("server returned %d: %s", statusCode, string(msg))
	}

	// Check if this is a directory listing
	if srcPath != "" && strings.HasSuffix(srcPath, "/") {
		return uc.downloadDirectory(ctx, client, sem, baseURL, srcPath, dstPath, body)
	}

	// Download single file
	return 1, uc.downloadFile(dstPath, body)
}

func (uc *DownloadUseCase) downloadDirectory(
	ctx context.Context,
	client usecase.HTTPClient,
	sem chan struct{},
	baseURL, srcPath, dstPath string,
	body io.ReadCloser,
) (int, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	totalCount := 0

	dec := json.NewDecoder(body)
	
	// Find "items" array
	for {
		t, err := dec.Token()
		if err != nil {
			return 0, err
		}
		if t == "items" {
			t, err = dec.Token()
			if err != nil {
				return 0, err
			}
			if t != json.Delim('[') {
				return 0, fmt.Errorf("JSON items is not a list")
			}
			break
		}
	}

	// Process items
	for dec.More() {
		var item dirItem
		if err := dec.Decode(&item); err != nil {
			break
		}

		subPath := item.Name
		if item.Type == "dir" {
			if err := uc.fs.MkdirAll(filepath.Join(dstPath, subPath)); err != nil {
				return 0, fmt.Errorf("create dir: %w", err)
			}
			subPath += "/"
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(sp string) {
			defer func() { <-sem; wg.Done() }()
			count, err := uc.recursiveDownload(ctx, client, sem, baseURL, srcPath+sp, filepath.Join(dstPath, sp))
			mu.Lock()
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("download %s: %w", filepath.Join(srcPath, sp), err)
			}
			totalCount += count
			mu.Unlock()
		}(subPath)
	}

	wg.Wait()
	return totalCount, firstErr
}

func (uc *DownloadUseCase) downloadFile(dstPath string, body io.ReadCloser) error {
	if dstPath == "" {
		// Write to stdout (handled by caller)
		return nil
	}

	out, err := uc.fs.Create(dstPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, body)
	if err != nil {
		return err
	}

	uc.logger.Info("Downloaded file", "path", dstPath)
	return nil
}

