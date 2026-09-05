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

package archive

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestFSMetadata_PAXRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		codec        string
		originalPath string
		rawSize      int64
		storedPath   string
	}{
		{name: "none preserves codec-looking filename", codec: "none", originalPath: "report.zst", rawSize: 7, storedPath: "report.zst"},
		{name: "zstd appends codec independently", codec: "zstd", originalPath: "report.gz", rawSize: 11, storedPath: "report.gz.zst"},
		{name: "gzip nested path", codec: "gzip", originalPath: "dir/file.lz4", rawSize: 0, storedPath: "dir/file.lz4.gz"},
		{name: "lz4 path", codec: "lz4", originalPath: "file", rawSize: 19, storedPath: "file.lz4"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			metadata, err := NewFSMetadata(tc.codec, tc.originalPath, tc.rawSize)
			if err != nil {
				t.Fatalf("NewFSMetadata: %v", err)
			}

			var buf bytes.Buffer

			tw := tar.NewWriter(&buf)
			hdr := &tar.Header{
				Format:     tar.FormatPAX,
				Typeflag:   tar.TypeReg,
				Name:       tc.storedPath,
				Mode:       0o600,
				Size:       tc.rawSize,
				PAXRecords: metadata.PAXRecords(),
			}

			if err := tw.WriteHeader(hdr); err != nil {
				t.Fatalf("WriteHeader: %v", err)
			}

			if _, err := tw.Write(make([]byte, tc.rawSize)); err != nil {
				t.Fatalf("Write: %v", err)
			}

			if err := tw.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
			gotHeader, err := tr.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}

			got, err := ParseFSMetadata(gotHeader)
			if err != nil {
				t.Fatalf("ParseFSMetadata: %v", err)
			}

			if got != metadata {
				t.Fatalf("metadata = %#v, want %#v", got, metadata)
			}

			if gotHeader.Name != tc.storedPath {
				t.Fatalf("stored path = %q, want %q", gotHeader.Name, tc.storedPath)
			}
		})
	}
}

func TestParseFSMetadata_FailsClosed(t *testing.T) {
	t.Parallel()

	valid := map[string]string{
		PAXFSCodec:        "zstd",
		PAXFSOriginalPath: "file.txt",
		PAXFSRawSize:      "12",
	}

	tests := []struct {
		name    string
		header  *tar.Header
		mutate  func(map[string]string)
		wantErr error
	}{
		{name: "missing codec", header: &tar.Header{Typeflag: tar.TypeReg, Name: "file.txt.zst"}, mutate: func(records map[string]string) { delete(records, PAXFSCodec) }, wantErr: ErrInvalidFSMetadata},
		{name: "missing original path", header: &tar.Header{Typeflag: tar.TypeReg, Name: "file.txt.zst"}, mutate: func(records map[string]string) { delete(records, PAXFSOriginalPath) }, wantErr: ErrInvalidFSMetadata},
		{name: "missing raw size", header: &tar.Header{Typeflag: tar.TypeReg, Name: "file.txt.zst"}, mutate: func(records map[string]string) { delete(records, PAXFSRawSize) }, wantErr: ErrInvalidFSMetadata},
		{name: "unknown codec", header: &tar.Header{Typeflag: tar.TypeReg, Name: "file.txt.zst"}, mutate: func(records map[string]string) { records[PAXFSCodec] = "brotli" }, wantErr: ErrInvalidFSMetadata},
		{name: "negative raw size", header: &tar.Header{Typeflag: tar.TypeReg, Name: "file.txt.zst"}, mutate: func(records map[string]string) { records[PAXFSRawSize] = "-1" }, wantErr: ErrInvalidFSMetadata},
		{name: "noncanonical raw size", header: &tar.Header{Typeflag: tar.TypeReg, Name: "file.txt.zst"}, mutate: func(records map[string]string) { records[PAXFSRawSize] = "012" }, wantErr: ErrInvalidFSMetadata},
		{name: "unsafe original path", header: &tar.Header{Typeflag: tar.TypeReg, Name: "../file.txt.zst"}, mutate: func(records map[string]string) { records[PAXFSOriginalPath] = "../file.txt" }, wantErr: ErrInvalidFSMetadata},
		{name: "stored path mismatch", header: &tar.Header{Typeflag: tar.TypeReg, Name: "file.txt"}, mutate: func(map[string]string) {}, wantErr: ErrInvalidFSMetadata},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			records := make(map[string]string, len(valid))
			for key, value := range valid {
				records[key] = value
			}

			tc.mutate(records)
			tc.header.PAXRecords = records
			tc.header.Size = 5

			_, err := ParseFSMetadata(tc.header)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ParseFSMetadata error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestComputeNodeChecksum_CoversFSPAXMetadata(t *testing.T) {
	t.Parallel()

	firstNode := makeNodeDir(t)
	secondNode := makeNodeDir(t)

	writeTestMetadataTar(t, filepath.Join(firstNode, FsTarName), 10)
	writeTestMetadataTar(t, filepath.Join(secondNode, FsTarName), 11)

	first, err := ComputeNodeChecksum(firstNode)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum first: %v", err)
	}

	second, err := ComputeNodeChecksum(secondNode)
	if err != nil {
		t.Fatalf("ComputeNodeChecksum second: %v", err)
	}

	if first.Hex == second.Hex {
		t.Fatal("changing only rawSize PAX metadata must change the data.tar checksum")
	}
}

// TestSumTarRawSizes covers SumTarRawSizes: sums only regular-entry PAX raw sizes, ignores
// directory/symlink entries, and propagates ParseFSMetadata's fail-closed errors.
func TestSumTarRawSizes(t *testing.T) {
	t.Parallel()

	t.Run("success: empty tar sums to zero", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		tw := tar.NewWriter(&buf)
		if err := tw.Close(); err != nil {
			t.Fatalf("close tar writer: %v", err)
		}

		total, err := SumTarRawSizes(context.Background(), bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("SumTarRawSizes: %v", err)
		}

		if total != 0 {
			t.Errorf("total = %d, want 0", total)
		}
	})

	t.Run("success: sums regular entries, ignores directory and symlink entries", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		tw := tar.NewWriter(&buf)

		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o755}); err != nil {
			t.Fatalf("write directory header: %v", err)
		}

		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeSymlink, Name: "link", Linkname: "dir/first", Mode: 0o777}); err != nil {
			t.Fatalf("write symlink header: %v", err)
		}

		writeRegularPAXEntry(t, tw, "dir/first", "none", 10)
		writeRegularPAXEntry(t, tw, "second", "zstd", 25)

		if err := tw.Close(); err != nil {
			t.Fatalf("close tar writer: %v", err)
		}

		total, err := SumTarRawSizes(context.Background(), bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatalf("SumTarRawSizes: %v", err)
		}

		if want := int64(10 + 25); total != want {
			t.Errorf("total = %d, want %d", total, want)
		}
	})

	t.Run("error: regular entry missing required PAX metadata", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		tw := tar.NewWriter(&buf)

		// A regular entry with no PAX records at all: ParseFSMetadata must reject it,
		// and SumTarRawSizes must propagate that failure rather than skip the entry.
		if err := tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     "broken.txt",
			Mode:     0o600,
			Size:     3,
		}); err != nil {
			t.Fatalf("write header: %v", err)
		}

		if _, err := io.WriteString(tw, "abc"); err != nil {
			t.Fatalf("write body: %v", err)
		}

		if err := tw.Close(); err != nil {
			t.Fatalf("close tar writer: %v", err)
		}

		_, err := SumTarRawSizes(context.Background(), bytes.NewReader(buf.Bytes()))
		if !errors.Is(err, ErrInvalidFSMetadata) {
			t.Fatalf("SumTarRawSizes error = %v, want wrapping ErrInvalidFSMetadata", err)
		}
	})

	t.Run("error: context canceled before completion", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer

		tw := tar.NewWriter(&buf)
		writeRegularPAXEntry(t, tw, "first", "none", 5)

		if err := tw.Close(); err != nil {
			t.Fatalf("close tar writer: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := SumTarRawSizes(ctx, bytes.NewReader(buf.Bytes()))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("SumTarRawSizes error = %v, want context.Canceled", err)
		}
	})
}

// writeRegularPAXEntry writes one well-formed regular PAX entry of rawSize plaintext bytes.
// Body is always written raw (codec name is just metadata) so stored size always == rawSize.
func writeRegularPAXEntry(t *testing.T, tw *tar.Writer, originalPath, codec string, rawSize int64) {
	t.Helper()

	metadata, err := NewFSMetadata(codec, originalPath, rawSize)
	if err != nil {
		t.Fatalf("NewFSMetadata: %v", err)
	}

	storedPath, err := metadata.StoredPath()
	if err != nil {
		t.Fatalf("StoredPath: %v", err)
	}

	hdr := &tar.Header{
		Format:     tar.FormatPAX,
		Typeflag:   tar.TypeReg,
		Name:       storedPath,
		Mode:       0o600,
		Size:       rawSize,
		PAXRecords: metadata.PAXRecords(),
	}

	// SumTarRawSizes never decodes the body, so any bytes of the declared Size work here;
	// content correctness for a given codec is exercised elsewhere (fsmetadata round trip).
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}

	if _, err := tw.Write(make([]byte, rawSize)); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func writeTestMetadataTar(t *testing.T, path string, rawSize int64) {
	t.Helper()

	var buf bytes.Buffer

	tw := tar.NewWriter(&buf)
	metadata, err := NewFSMetadata("zstd", "file.txt", rawSize)
	if err != nil {
		t.Fatalf("NewFSMetadata: %v", err)
	}

	hdr := &tar.Header{
		Format:     tar.FormatPAX,
		Typeflag:   tar.TypeReg,
		Name:       "file.txt.zst",
		Mode:       0o600,
		Size:       1,
		PAXRecords: metadata.PAXRecords(),
	}

	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}

	if _, err := io.WriteString(tw, "x"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}
