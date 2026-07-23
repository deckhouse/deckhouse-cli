//go:build integration

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

package snapimport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// TestPutBlockCompressed_ProductionFrameGeometryResume exercises the producer and
// importer contract at its real geometry: a 256 MiB zstd frame followed by a
// short frame. It is deliberately integration-tagged because the fixture writes
// 256 MiB to disk; run it explicitly with:
//
// go test -tags=integration -count=1 ./internal/snapshot/snapimport -run '^TestPutBlockCompressed_ProductionFrameGeometryResume$'
func TestPutBlockCompressed_ProductionFrameGeometryResume(t *testing.T) {
	const shortFrameSize = int64(1<<20 + 17)

	totalSize := int64(volume.DefaultChunkSize) + shortFrameSize
	resumeOffset := int64(volume.DefaultChunkSize) + shortFrameSize/2

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end, err := parseFixtureRange(r.Header.Get("Range"), totalSize)
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)

			return
		}

		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
		w.WriteHeader(http.StatusPartialContent)

		if err := writeFixtureBytes(w, start, end-start+1); err != nil {
			t.Errorf("write fixture range: %v", err)
		}
	}))
	t.Cleanup(source.Close)

	codec, err := compress.New(compress.DefaultCodecName, 0)
	require.NoError(t, err)

	dir := t.TempDir()
	chunkDir := filepath.Join(dir, archive.BlockChunksDirName)
	dataFile := filepath.Join(dir, "data.bin"+codec.Ext())

	require.NoError(t, volume.DownloadBlockChunks(
		context.Background(),
		slog.Default(),
		chunkDir,
		source.URL,
		totalSize,
		volume.DefaultChunkSize,
		1,
		exporter.NewFetcher(source.Client()),
		codec,
		nil,
	))
	require.NoError(t, volume.MergeBlockChunks(
		context.Background(), chunkDir, dataFile, totalSize, volume.DefaultChunkSize, codec.Ext(),
	))

	file, err := os.Open(dataFile)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	countingFile := &fixtureCountingSeeker{ReadSeeker: file}
	reader, discarded, err := resolveBlockDecodeReader(
		context.Background(), countingFile, dataFile, codec.Ext(), resumeOffset, discardLogger(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	if discarded != shortFrameSize/2 {
		t.Fatalf("fast-path discarded = %d, want only intra-frame prefix %d; byte-zero fallback would discard %d", discarded, shortFrameSize/2, resumeOffset)
	}

	if discarded >= int64(volume.DefaultChunkSize) {
		t.Fatalf("fast-path discarded = %d, want less than frame size %d", discarded, volume.DefaultChunkSize)
	}

	if countingFile.readBytes >= int64(volume.DefaultChunkSize) {
		t.Fatalf("resolver read %d compressed bytes, want bounded header/intra-frame work below first-frame size", countingFile.readBytes)
	}

	checker := &fixtureSuffixImporter{offset: resumeOffset}
	importer := httptest.NewServer(checker)
	t.Cleanup(importer.Close)

	require.NoError(t, putBlock(
		context.Background(), plainHTTPDoer{}, importer.URL, dataFile, codec.Ext(), totalSize, discardLogger(), nil, nil,
	))

	if checker.received != totalSize-resumeOffset {
		t.Fatalf("uploaded suffix size = %d, want %d", checker.received, totalSize-resumeOffset)
	}
}

type fixtureCountingSeeker struct {
	io.ReadSeeker
	readBytes int64
}

func (s *fixtureCountingSeeker) Read(p []byte) (int, error) {
	n, err := s.ReadSeeker.Read(p)
	s.readBytes += int64(n)

	return n, err
}

type fixtureSuffixImporter struct {
	offset   int64
	received int64
}

func (i *fixtureSuffixImporter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		w.Header().Set("X-Next-Offset", strconv.FormatInt(i.offset, 10))
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		offset, err := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
		if err != nil || offset != i.offset {
			http.Error(w, "unexpected upload offset", http.StatusConflict)

			return
		}

		read, err := checkFixtureBytes(r.Body, i.offset)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		i.offset += read
		i.received += read
		w.Header().Set("X-Next-Offset", strconv.FormatInt(i.offset, 10))
		w.WriteHeader(http.StatusCreated)
	default:
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
	}
}

func parseFixtureRange(header string, totalSize int64) (int64, int64, error) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, fmt.Errorf("missing byte range %q", header)
	}

	parts := strings.Split(strings.TrimPrefix(header, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid byte range %q", header)
	}

	start, startErr := strconv.ParseInt(parts[0], 10, 64)
	end, endErr := strconv.ParseInt(parts[1], 10, 64)
	if startErr != nil || endErr != nil || start < 0 || end < start || end >= totalSize {
		return 0, 0, fmt.Errorf("invalid byte range %q", header)
	}

	return start, end, nil
}

func writeFixtureBytes(dst io.Writer, offset, size int64) error {
	buf := make([]byte, 32*1024)

	for size > 0 {
		n := min(int64(len(buf)), size)
		fillFixtureBytes(buf[:n], offset)

		if _, err := dst.Write(buf[:n]); err != nil {
			return err
		}

		offset += n
		size -= n
	}

	return nil
}

func checkFixtureBytes(src io.Reader, offset int64) (int64, error) {
	buf := make([]byte, 32*1024)
	want := make([]byte, len(buf))

	var total int64

	for {
		n, err := src.Read(buf)
		if n > 0 {
			fillFixtureBytes(want[:n], offset+total)
			for idx := range n {
				if buf[idx] != want[idx] {
					return total, fmt.Errorf("wrong byte at raw offset %d", offset+total+int64(idx))
				}
			}

			total += int64(n)
		}

		if errors.Is(err, io.EOF) {
			return total, nil
		}

		if err != nil {
			return total, err
		}
	}
}

func fillFixtureBytes(dst []byte, offset int64) {
	for idx := range dst {
		value := uint64(offset + int64(idx))
		value ^= value >> 17
		value *= 0xed5ad4bb
		value ^= value >> 11
		dst[idx] = byte(value)
	}
}
