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

package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/backup/domain"
)

const (
	lokiURL            = "https://loki.d8-monitoring:3100/loki/api/v1"
	namespaceDeckhouse = "d8-system"
	containerDeckhouse = "deckhouse"
	namespaceLoki      = "d8-monitoring"
	secretNameLoki     = "loki-api-token"
	templateDate       = time.DateTime
)

// LokiDumpUseCase handles Loki logs dump operations
type LokiDumpUseCase struct {
	k8s    K8sClient
	logger Logger
}

// NewLokiDumpUseCase creates a new LokiDumpUseCase
func NewLokiDumpUseCase(k8s K8sClient, logger Logger) *LokiDumpUseCase {
	return &LokiDumpUseCase{
		k8s:    k8s,
		logger: logger,
	}
}

// QueryRange represents Loki query_range API response
type QueryRange struct {
	Data struct {
		Result []struct {
			Values [][]string `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// SeriesAPI represents Loki series API response
type SeriesAPI struct {
	Data []map[string]string `json:"data"`
}

// CurlRequest helps build curl commands for Loki API
type CurlRequest struct {
	BaseURL   string
	Params    map[string]string
	AuthToken string
}

// Execute performs Loki logs dump
func (uc *LokiDumpUseCase) Execute(ctx context.Context, params *domain.LokiBackupParams, output io.Writer, deckhousePodName string) (*domain.BackupResult, error) {
	result := &domain.BackupResult{
		Type: domain.BackupTypeLoki,
	}

	// Get Loki token from secret
	tokenData, err := uc.k8s.GetSecret(ctx, namespaceLoki, secretNameLoki)
	if err != nil {
		result.Error = fmt.Errorf("get Loki token: %w", err)
		return result, result.Error
	}

	token := string(tokenData["token"])
	if token == "" {
		result.Error = fmt.Errorf("token not found in secret")
		return result, result.Error
	}

	fmt.Fprintln(output, "Getting logs from Loki API...")

	// Get end timestamp
	endTimestamp, err := uc.getEndTimestamp(ctx, params.EndTimestamp, token, deckhousePodName)
	if err != nil {
		result.Error = fmt.Errorf("get end timestamp: %w", err)
		return result, result.Error
	}

	// Calculate chunk size
	chunkSize := time.Duration(params.ChunkDays) * 24 * time.Hour

	// Process chunks
	for chunkEnd := endTimestamp; chunkEnd > 0; chunkEnd -= chunkSize.Nanoseconds() {
		chunkStart := chunkEnd - chunkSize.Nanoseconds()
		if params.StartTimestamp != "" {
			var err error
			chunkStart, err = uc.parseTimestamp(params.StartTimestamp)
			if err != nil {
				result.Error = err
				return result, result.Error
			}
		}

		// Get stream list
		series, err := uc.getSeriesList(ctx, token, chunkStart, chunkEnd, deckhousePodName)
		if err != nil {
			result.Error = fmt.Errorf("get series list: %w", err)
			return result, result.Error
		}

		if len(series.Data) == 0 {
			fmt.Fprintln(output, "No more streams.\nStop...")
			break
		}

		// Process each stream
		for _, stream := range series.Data {
			if err := uc.fetchLogs(ctx, stream, chunkStart, endTimestamp, token, params.Limit, deckhousePodName, output); err != nil {
				result.Error = fmt.Errorf("fetch logs: %w", err)
				return result, result.Error
			}
		}
	}

	result.Success = true
	return result, nil
}

func (uc *LokiDumpUseCase) getEndTimestamp(ctx context.Context, endTimestampStr, token, deckhousePodName string) (int64, error) {
	if endTimestampStr == "" {
		// Get latest timestamp from Loki
		curlParam := CurlRequest{
			BaseURL: "query_range",
			Params: map[string]string{
				"query":     `{pod=~".+"}`,
				"limit":     "1",
				"direction": "BACKWARD",
			},
			AuthToken: token,
		}

		queryRange, err := uc.execLokiQuery(ctx, curlParam.GenerateCurlCommand(), deckhousePodName)
		if err != nil {
			return 0, fmt.Errorf("get latest timestamp: %w", err)
		}

		if len(queryRange.Data.Result) == 0 || len(queryRange.Data.Result[0].Values) == 0 {
			return 0, fmt.Errorf("no logs found in Loki")
		}

		ts, err := strconv.ParseInt(queryRange.Data.Result[0].Values[0][0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse timestamp: %w", err)
		}
		return ts, nil
	}

	return uc.parseTimestamp(endTimestampStr)
}

func (uc *LokiDumpUseCase) parseTimestamp(timestampStr string) (int64, error) {
	t, err := time.Parse(templateDate, timestampStr)
	if err != nil {
		return 0, fmt.Errorf("parse date %q: %w", timestampStr, err)
	}
	return t.UnixNano(), nil
}

func (uc *LokiDumpUseCase) getSeriesList(ctx context.Context, token string, start, end int64, deckhousePodName string) (*SeriesAPI, error) {
	curlParam := CurlRequest{
		BaseURL: "series",
		Params: map[string]string{
			"end":   strconv.FormatInt(end, 10),
			"start": strconv.FormatInt(start, 10),
		},
		AuthToken: token,
	}

	return uc.execLokiSeriesQuery(ctx, curlParam.GenerateCurlCommand(), deckhousePodName)
}

func (uc *LokiDumpUseCase) fetchLogs(ctx context.Context, stream map[string]string, chunkStart, endTimestamp int64, token, limit, deckhousePodName string, output io.Writer) error {
	// Build query from stream labels
	filters := make([]string, 0, len(stream))
	for key, value := range stream {
		filters = append(filters, fmt.Sprintf(`%s=%q`, key, value))
	}
	query := fmt.Sprintf(`{%s}`, strings.Join(filters, ", "))

	chunkEnd := endTimestamp
	for chunkEnd > chunkStart {
		curlParam := CurlRequest{
			BaseURL: "query_range",
			Params: map[string]string{
				"end":       strconv.FormatInt(chunkEnd, 10),
				"start":     strconv.FormatInt(chunkStart, 10),
				"query":     query,
				"limit":     limit,
				"direction": "BACKWARD",
			},
			AuthToken: token,
		}

		queryRange, err := uc.execLokiQuery(ctx, curlParam.GenerateCurlCommand(), deckhousePodName)
		if err != nil {
			return fmt.Errorf("query logs: %w", err)
		}

		if len(queryRange.Data.Result) == 0 {
			break
		}

		// Print logs
		for _, result := range queryRange.Data.Result {
			for _, entry := range result.Values {
				ts, err := strconv.ParseInt(entry[0], 10, 64)
				if err != nil {
					return fmt.Errorf("parse timestamp: %w", err)
				}
				timestampUTC := time.Unix(0, ts).UTC()
				fmt.Fprintf(output, "Timestamp: [%v], Log: %s\n", timestampUTC, entry[1])
			}
		}

		// Get last timestamp for pagination
		lastValues := queryRange.Data.Result[0].Values
		if len(lastValues) == 0 {
			break
		}
		lastTimestamp, err := strconv.ParseInt(lastValues[len(lastValues)-1][0], 10, 64)
		if err != nil {
			return fmt.Errorf("parse last timestamp: %w", err)
		}
		chunkEnd = lastTimestamp
	}

	return nil
}

func (uc *LokiDumpUseCase) execLokiQuery(ctx context.Context, command []string, deckhousePodName string) (*QueryRange, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	err := uc.k8s.ExecInPod(ctx, namespaceDeckhouse, deckhousePodName, containerDeckhouse, command, stdout, stderr)
	if err != nil {
		return nil, fmt.Errorf("exec in pod: %w", err)
	}

	if !json.Valid(stdout.Bytes()) {
		return nil, fmt.Errorf("invalid JSON response: %s", stdout.String())
	}

	var result QueryRange
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &result, nil
}

func (uc *LokiDumpUseCase) execLokiSeriesQuery(ctx context.Context, command []string, deckhousePodName string) (*SeriesAPI, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	err := uc.k8s.ExecInPod(ctx, namespaceDeckhouse, deckhousePodName, containerDeckhouse, command, stdout, stderr)
	if err != nil {
		return nil, fmt.Errorf("exec in pod: %w", err)
	}

	if !json.Valid(stdout.Bytes()) {
		return nil, fmt.Errorf("invalid JSON response: %s", stdout.String())
	}

	var result SeriesAPI
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &result, nil
}

// GenerateCurlCommand builds curl command for Loki API
func (c *CurlRequest) GenerateCurlCommand() []string {
	curlParts := []string{"curl", "--insecure", "-v"}
	curlParts = append(curlParts, fmt.Sprintf("%s/%s", lokiURL, c.BaseURL))
	for key, value := range c.Params {
		if value != "" {
			curlParts = append(curlParts, "--data-urlencode", fmt.Sprintf("%s=%s", key, value))
		}
	}
	if c.AuthToken != "" {
		curlParts = append(curlParts, "-H", fmt.Sprintf("Authorization: Bearer %s", c.AuthToken))
	}
	return curlParts
}

