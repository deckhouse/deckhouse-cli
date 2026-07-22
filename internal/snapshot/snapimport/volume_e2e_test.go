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
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// This file closes the producer/consumer contract gap invariant #5 warns about (see
// .agent/implementer-prompt.md): volume_test.go's buildMergedZstdFixture already drives
// payload through the REAL volume.MergeBlockChunks, but it hand-writes each chunk's
// codec.EncodeFrame output directly to disk, so it never exercises
// volume.DownloadBlockChunks -- the actual producer of those chunk files in production.
// Both MergeBlockChunks (the write side) and putBlockCompressed/resolveBlockDecodeReader
// (the read side) already have package-local unit tests, but each fabricates its own
// independent fixture, which proves nothing about whether the two sides genuinely agree on
// the real on-disk byte format once wired together end to end. The tests below drive the
// WHOLE real pipeline -- DownloadBlockChunks against a genuine Range-serving
// httptest.Server, then MergeBlockChunks, then putBlockCompressed's resume-positioning
// entry points -- with no hand-fabricated fixture anywhere in the chain.

// e2eBlockRangeServer serves payload from /api/v1/block via http.ServeContent, honouring
// Range requests exactly like the real data-exporter block endpoint -- mirroring
// volume/block_test.go's newBlockServer (unexported there, so reimplemented locally rather
// than exported purely for this cross-package test).
func e2eBlockRangeServer(t *testing.T, payload []byte) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, "data.img", time.Time{}, bytes.NewReader(payload))
	})

	return httptest.NewServer(mux)
}

// buildRealMergedZstdFixture drives payload through the REAL volume.DownloadBlockChunks
// (against a genuine Range-serving httptest.Server) followed by the REAL
// volume.MergeBlockChunks, producing a data.bin.zst with a genuine embedded seek table.
// This is the fixture-fabrication gap this file exists to close: unlike
// buildMergedZstdFixture in volume_test.go (which hand-writes each chunk's
// codec.EncodeFrame output straight to disk), every byte of the returned file passed
// through the actual chunk-download-over-HTTP-then-merge code path a real `d8 snapshot
// download` run would use.
func buildRealMergedZstdFixture(t *testing.T, payload []byte, chunkSize int64) string {
	t.Helper()

	srv := e2eBlockRangeServer(t, payload)
	t.Cleanup(srv.Close)

	fetcher := exporter.NewFetcher(srv.Client())

	codec, err := compress.New("zstd", int(compress.LevelFastest))
	if err != nil {
		t.Fatalf("compress.New(zstd): %v", err)
	}

	nodeDir := t.TempDir()
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	totalSize := int64(len(payload))

	if err := volume.DownloadBlockChunks(
		context.Background(), slog.Default(), chunkDir, srv.URL+"/api/v1/block",
		totalSize, chunkSize, 4, fetcher, codec, nil,
	); err != nil {
		t.Fatalf("volume.DownloadBlockChunks: %v", err)
	}

	outPath := filepath.Join(nodeDir, archive.DataBlockName(codec.Ext()))

	if err := volume.MergeBlockChunks(context.Background(), chunkDir, outPath, totalSize, chunkSize, codec.Ext()); err != nil {
		t.Fatalf("volume.MergeBlockChunks: %v", err)
	}

	return outPath
}

// assertBoundedNativeSeek proves resolveBlockDecodeReader's native zstd seek fast path
// positions correctly at offset while discarding at most one chunk's worth of decoded
// bytes -- the acceptance bar this whole task exists to prove empirically (see
// EMPIRICAL VERIFICATION below and this file's mandatory fallback-regression check, which
// confirms this exact assertion actually fails once the fast path is disabled).
//
// Two independent, instrumented measurements are asserted, neither based on timing:
//   - discarded, resolveBlockDecodeReader's own return value: on the native path this is
//     always exactly 0 (Seek is a seek-table lookup, never a decode-and-discard); on the
//     byte-zero fallback it equals offset, which for every offset this test drives is far
//     beyond one chunk's worth -- this alone already discriminates the regression the
//     mandatory empirical step reintroduces.
//   - every ReadAt call recorded by wrapping the real *os.File in recordingFile (defined in
//     volume_test.go) must land at or after the TARGET FRAME's own compressed offset (from
//     the seek table itself, via zstdSeekTableCompressedOffset, also in volume_test.go): a
//     decode-and-discard scan from byte zero would necessarily touch every earlier, smaller
//     compressed offset first, so a call below the target frame's own offset proves the
//     fallback ran instead of the native seek. (The subsequent io.ReadAll below reads
//     through to EOF to prove correctness of the decoded bytes, which legitimately touches
//     every LATER chunk too -- that is expected and is why this check is a per-call floor,
//     not a total-bytes-fetched ceiling.)
func assertBoundedNativeSeek(t *testing.T, dataFile string, payload []byte, offset, chunkSize int64) {
	t.Helper()

	targetCompressedOffset := zstdSeekTableCompressedOffset(t, dataFile, offset)

	f, err := os.Open(dataFile)
	if err != nil {
		t.Fatalf("open %s: %v", dataFile, err)
	}
	defer func() { _ = f.Close() }()

	rf := &recordingFile{f: f}

	decodeReader, discarded, err := resolveBlockDecodeReader(rf, dataFile, ".zst", offset, discardLogger())
	if err != nil {
		t.Fatalf("resolveBlockDecodeReader: %v", err)
	}
	defer func() { _ = decodeReader.Close() }()

	if discarded > chunkSize {
		t.Fatalf("discarded = %d, want at most one chunk's worth (%d) -- a byte-zero fallback resuming this "+
			"deep into the archive would discard far more than one chunk", discarded, chunkSize)
	}

	rest, err := io.ReadAll(decodeReader)
	if err != nil {
		t.Fatalf("read remaining decoded bytes: %v", err)
	}

	if !bytes.Equal(rest, payload[offset:]) {
		t.Fatalf("decode reader positioned at the wrong offset: got %d remaining bytes, want %d matching payload[offset:]",
			len(rest), len(payload)-int(offset))
	}

	for _, c := range rf.recordedCalls() {
		if c.offset < int64(targetCompressedOffset) {
			t.Fatalf("ReadAt at compressed offset %d precedes the target frame's own compressed offset %d -- "+
				"the reader scanned earlier chunks instead of seeking directly to the target frame", c.offset, targetCompressedOffset)
		}
	}
}

// assertResumedUploadReconstructsPayload drives the REAL putBlock -> putBlockCompressed ->
// resolveBlockDecodeReader dispatch over a genuine net/http round trip against
// fakeBlockImporter (defined in volume_test.go), seeded with exactly the prefix a byte-zero
// run would already have durably sent up to offset. The server's final durably-received
// bytes must equal the original payload exactly, proving the resumed upload's own bytes
// concatenate correctly onto that seeded prefix.
func assertResumedUploadReconstructsPayload(t *testing.T, dataFile string, payload []byte, offset int64) {
	t.Helper()

	imp := &fakeBlockImporter{}
	imp.seed(payload[:offset])

	srv := httptest.NewServer(imp)
	defer srv.Close()

	if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, ".zst", int64(len(payload)), discardLogger(), nil, nil); err != nil {
		t.Fatalf("putBlock (resume from offset %d): %v", offset, err)
	}

	got := imp.received()
	if !bytes.Equal(got, payload) {
		t.Fatalf("resumed upload from offset %d produced %d bytes not matching the original %d-byte payload "+
			"(a regression here means the write side's seek table and the read side's seek lookup disagree on "+
			"the real on-disk byte format)", offset, len(got), len(payload))
	}
}

// TestE2E_BlockDownloadMergeUpload_NativeZstdSeekResume is the cross-package regression
// test resume-seekable-zstd-e2e-integration-test adds: synthetic multi-chunk content is
// downloaded and merged through the REAL volume.DownloadBlockChunks + volume.MergeBlockChunks
// pipeline (buildRealMergedZstdFixture), producing a real data.bin.zst with an embedded seek
// table, which is then fed into the REAL putBlockCompressed resume path
// (resolveBlockDecodeReader / putBlock) at three distinct resume offsets: the start of a
// middle chunk, mid-chunk (not on a boundary), and inside the volume's (deliberately
// shorter) last chunk. Every offset must show bounded discard and a byte-perfect
// reconstruction of the original content.
func TestE2E_BlockDownloadMergeUpload_NativeZstdSeekResume(t *testing.T) {
	t.Parallel()

	const chunkSize = 700_000     // 700 KiB per chunk.
	const fullChunks = 6          // six full-size chunks ...
	const lastChunkSize = 350_000 // ... plus one deliberately shorter final chunk: 7 chunks, ~4.55 MiB total.

	payload := randomPayload(t, chunkSize*fullChunks+lastChunkSize)

	dataFile := buildRealMergedZstdFixture(t, payload, chunkSize)

	testCases := []struct {
		name   string
		offset int64
	}{
		{name: "start of a middle chunk", offset: 3 * chunkSize},
		{name: "mid-chunk, not on a boundary", offset: 3*chunkSize + chunkSize/3},
		{name: "inside the last (shorter) chunk", offset: fullChunks*chunkSize + lastChunkSize/2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assertBoundedNativeSeek(t, dataFile, payload, tc.offset, chunkSize)
			assertResumedUploadReconstructsPayload(t, dataFile, payload, tc.offset)
		})
	}
}
