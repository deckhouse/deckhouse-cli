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

package dataimport

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/deckhouse/deckhouse-cli/internal/data/usecase"
)

// UploadUseCase handles file upload to DataImport
type UploadUseCase struct {
	repo       usecase.DataImportRepository
	httpClient usecase.HTTPClient
	fs         usecase.FileSystem
	logger     usecase.Logger
}

// NewUploadUseCase creates a new UploadUseCase
func NewUploadUseCase(
	repo usecase.DataImportRepository,
	httpClient usecase.HTTPClient,
	fs usecase.FileSystem,
	logger usecase.Logger,
) *UploadUseCase {
	return &UploadUseCase{
		repo:       repo,
		httpClient: httpClient,
		fs:         fs,
		logger:     logger,
	}
}

// UploadParams contains parameters for uploading data
type UploadParams struct {
	Name      string
	Namespace string
	FilePath  string
	DstPath   string
	Publish   bool
	Chunks    int
	Resume    bool
}

// Execute uploads a file to a DataImport
func (uc *UploadUseCase) Execute(ctx context.Context, params *UploadParams) error {
	// Get DataImport and wait for it to be ready
	dataImport, err := uc.repo.GetWithRetry(ctx, params.Name, params.Namespace)
	if err != nil {
		return fmt.Errorf("get DataImport: %w", err)
	}

	// Prepare HTTP client
	httpClient := uc.httpClient
	if !params.Publish && dataImport.Status.CA != "" {
		httpClient = uc.httpClient.Copy()
		caData, err := base64.StdEncoding.DecodeString(dataImport.Status.CA)
		if err != nil {
			return fmt.Errorf("decode CA: %w", err)
		}
		httpClient.SetCA(caData)
	}

	// Build upload URL
	baseURL := dataImport.Status.URL
	if params.Publish && dataImport.Status.PublicURL != "" {
		baseURL = dataImport.Status.PublicURL
	}

	uploadURL, err := url.JoinPath(baseURL, "api/v1/files", params.DstPath)
	if err != nil {
		return fmt.Errorf("build URL: %w", err)
	}

	// Get file info
	fileInfo, err := uc.fs.Stat(params.FilePath)
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	totalSize := fileInfo.Size()
	if totalSize < 0 {
		return fmt.Errorf("invalid file size")
	}

	// Check resume progress
	var offset int64
	if params.Resume {
		offset, err = uc.checkUploadProgress(ctx, httpClient, uploadURL)
		if err != nil {
			return fmt.Errorf("check progress: %w", err)
		}
	}

	// Open file
	file, _, err := uc.fs.Open(params.FilePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	// Calculate chunk size
	chunks := params.Chunks
	if chunks < 1 {
		chunks = 1
	}
	chunkSize := totalSize / int64(chunks)
	if totalSize%int64(chunks) != 0 {
		chunkSize++
	}

	// Upload in chunks
	permOctal := fmt.Sprintf("%04o", fileInfo.Mode())
	uid := fileInfo.Uid()
	gid := fileInfo.Gid()

	for offset < totalSize {
		remaining := totalSize - offset
		sendLen := chunkSize
		if sendLen > remaining {
			sendLen = remaining
		}

		headers := map[string]string{
			"X-Content-Length":       strconv.FormatInt(totalSize, 10),
			"X-Attribute-Permissions": permOctal,
			"X-Attribute-Uid":         strconv.Itoa(uid),
			"X-Attribute-Gid":         strconv.Itoa(gid),
			"X-Offset":                strconv.FormatInt(offset, 10),
		}

		// Create section reader for the chunk
		section := io.NewSectionReader(file.(io.ReaderAt), offset, sendLen)

		respHeaders, statusCode, err := httpClient.Put(ctx, uploadURL, section, headers)
		if err != nil {
			return fmt.Errorf("upload chunk at offset %d: %w", offset, err)
		}

		if statusCode < 200 || statusCode >= 300 {
			return fmt.Errorf("server error at offset %d: status %d", offset, statusCode)
		}

		// Get next offset from response
		if nextOffsetStr := respHeaders["X-Next-Offset"]; nextOffsetStr != "" {
			nextOffset, err := strconv.ParseInt(nextOffsetStr, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid X-Next-Offset: %s: %w", nextOffsetStr, err)
			}
			if nextOffset < offset {
				return fmt.Errorf("server returned X-Next-Offset (%d) smaller than current offset (%d)", nextOffset, offset)
			}
			offset = nextOffset
		} else {
			offset += sendLen
		}
	}

	uc.logger.Info("Upload completed", "file", params.FilePath, "dst", params.DstPath)
	return nil
}

func (uc *UploadUseCase) checkUploadProgress(ctx context.Context, client usecase.HTTPClient, uploadURL string) (int64, error) {
	headers, statusCode, err := client.Head(ctx, uploadURL)
	if err != nil {
		return 0, err
	}

	if statusCode == http.StatusNotFound {
		return 0, nil
	}

	if statusCode != http.StatusOK {
		return 0, fmt.Errorf("server returned %d", statusCode)
	}

	if offsetStr := headers["X-Current-Offset"]; offsetStr != "" {
		return strconv.ParseInt(offsetStr, 10, 64)
	}

	return 0, nil
}

