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
