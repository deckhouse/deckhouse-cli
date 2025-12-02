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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/deckhouse-cli/internal/data/domain"
	"github.com/deckhouse/deckhouse-cli/internal/data/usecase"
)

// ListUseCase handles listing DataExport contents
type ListUseCase struct {
	repo       usecase.DataExportRepository
	httpClient usecase.HTTPClient
	logger     usecase.Logger
}

// NewListUseCase creates a new ListUseCase
func NewListUseCase(
	repo usecase.DataExportRepository,
	httpClient usecase.HTTPClient,
	logger usecase.Logger,
) *ListUseCase {
	return &ListUseCase{
		repo:       repo,
		httpClient: httpClient,
		logger:     logger,
	}
}

// ListParams contains parameters for listing
type ListParams struct {
	DataName  string
	Namespace string
	Path      string
	Publish   bool
	TTL       string
}

// ListResult contains the result of a list operation
type ListResult struct {
	ExportName string
	WasCreated bool
	Content    io.Reader
}

// Execute lists contents of a DataExport
func (uc *ListUseCase) Execute(ctx context.Context, params *ListParams) (*ListResult, error) {
	result := &ListResult{}

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

	// Wait for DataExport to be ready
	export, err := uc.repo.GetWithRetry(ctx, exportName, params.Namespace)
	if err != nil {
		return nil, fmt.Errorf("get DataExport: %w", err)
	}

	// Prepare HTTP client
	httpClient := uc.httpClient
	if !params.Publish && export.Status.CA != "" {
		httpClient = uc.httpClient.Copy()
		caData, err := base64.StdEncoding.DecodeString(export.Status.CA)
		if err != nil {
			return nil, fmt.Errorf("decode CA: %w", err)
		}
		httpClient.SetCA(caData)
	}

	// Build URL
	baseURL := export.Status.URL
	if params.Publish && export.Status.PublicURL != "" {
		baseURL = export.Status.PublicURL
	}

	switch export.Status.VolumeMode {
	case domain.VolumeModeFilesystem:
		return uc.listFilesystem(ctx, httpClient, baseURL, params.Path)
	case domain.VolumeModeBlock:
		return uc.listBlock(ctx, httpClient, baseURL)
	default:
		return nil, fmt.Errorf("%w: %s", domain.ErrUnsupportedVolumeMode, export.Status.VolumeMode)
	}
}

// DeleteCreatedExport deletes a DataExport that was created during list
func (uc *ListUseCase) DeleteCreatedExport(ctx context.Context, name, namespace string) error {
	return uc.repo.Delete(ctx, name, namespace)
}

func (uc *ListUseCase) listFilesystem(ctx context.Context, client usecase.HTTPClient, baseURL, path string) (*ListResult, error) {
	if path == "" || path[len(path)-1] != '/' {
		return nil, fmt.Errorf("path must end with '/'")
	}

	listURL, err := url.JoinPath(baseURL, "api/v1/files", path)
	if err != nil {
		return nil, err
	}

	uc.logger.Info("Listing directory", "url", listURL)

	body, statusCode, err := client.Get(ctx, listURL)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET: %w", err)
	}

	if statusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(body, 4096))
		body.Close()
		return nil, fmt.Errorf("server returned %d: %s", statusCode, string(msg))
	}

	return &ListResult{Content: body}, nil
}

func (uc *ListUseCase) listBlock(ctx context.Context, client usecase.HTTPClient, baseURL string) (*ListResult, error) {
	blockURL, err := url.JoinPath(baseURL, "api/v1/block")
	if err != nil {
		return nil, err
	}

	uc.logger.Info("Getting block info", "url", blockURL)

	headers, statusCode, err := client.Head(ctx, blockURL)
	if err != nil {
		return nil, fmt.Errorf("HTTP HEAD: %w", err)
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned %d", statusCode)
	}

	// Format size info
	content := ""
	if contLen := headers["Content-Length"]; contLen != "" {
		if size, err := strconv.ParseInt(contLen, 10, 64); err == nil {
			q := resource.NewQuantity(size, resource.BinarySI)
			content = fmt.Sprintf("Disk size: %s\n", q.String())
		} else {
			content = fmt.Sprintf("Disk size: %s bytes\n", contLen)
		}
	}

	return &ListResult{
		Content: io.NopCloser(stringReader(content)),
	}, nil
}

type stringReader string

func (s stringReader) Read(p []byte) (n int, err error) {
	n = copy(p, s)
	return n, io.EOF
}

