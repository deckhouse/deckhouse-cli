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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	kgzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// conditionTrue reports whether status.conditions[type==condType].status == "True".
func conditionTrue(obj *unstructured.Unstructured, condType string) bool {
	conds, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}

	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}

		t, _, _ := unstructured.NestedString(m, "type")
		if t != condType {
			continue
		}

		status, _, _ := unstructured.NestedString(m, "status")

		return status == string(metav1.ConditionTrue)
	}

	return false
}

// sleepCtx sleeps for d or returns false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// isCompressedBlockFile reports whether path carries a codec extension this importer
// understands (and must therefore decompress). A file without one (e.g. data.bin) is raw.
func isCompressedBlockFile(path string) bool {
	switch filepath.Ext(path) {
	case ".zst", ".gz", ".lz4":
		return true
	default:
		return false
	}
}

// resolveBlockSource yields a seekable, raw (decompressed) block file ready for upload plus
// its size and a cleanup func. tempDir is the directory for the decompressed temp file; pass
// filepath.Dir(dataFile) to keep it on the same filesystem as the archive (the default when
// no explicit --temp-dir is given). A raw archive file (data.bin) is used in place with a
// no-op cleanup — avoiding a full second on-disk copy; a compressed file is decompressed into
// a temp file that cleanup removes.
func resolveBlockSource(dataFile, tempDir string) (path string, size int64, cleanup func(), err error) {
	if !isCompressedBlockFile(dataFile) {
		info, sErr := os.Stat(dataFile)
		if sErr != nil {
			return "", 0, nil, fmt.Errorf("stat volume data %s: %w", dataFile, sErr)
		}

		return dataFile, info.Size(), func() {}, nil
	}

	tmpPath, sz, dErr := decompressToTemp(dataFile, tempDir)
	if dErr != nil {
		return "", 0, nil, dErr
	}

	return tmpPath, sz, func() { _ = os.Remove(tmpPath) }, nil
}

// decompressToTemp decompresses the archive volume file at srcPath (codec inferred from
// its extension) into a new temporary file in dir, returning the temp path and its size.
// The caller is responsible for removing the returned file. A ".bin" file with no codec
// extension is copied verbatim.
//
// Block-volume files are a concatenation of independent codec frames (one per chunk).
// zstd and gzip readers consume concatenated frames natively; lz4 frames must be decoded
// one at a time, so the lz4 path loops over the seekable source.
func decompressToTemp(srcPath, dir string) (string, int64, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return "", 0, fmt.Errorf("open volume data %s: %w", srcPath, err)
	}
	defer src.Close()

	tmp, err := os.CreateTemp(dir, "d8-import-*.raw")
	if err != nil {
		return "", 0, fmt.Errorf("create temp file: %w", err)
	}

	if err := decompressInto(tmp, src, filepath.Ext(srcPath)); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())

		return "", 0, fmt.Errorf("decompress %s: %w", srcPath, err)
	}

	size, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())

		return "", 0, fmt.Errorf("stat temp file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())

		return "", 0, fmt.Errorf("close temp file: %w", err)
	}

	return tmp.Name(), size, nil
}

// decompressInto streams the decompressed bytes of src (codec inferred from ext) into dst.
// src may be any io.Reader — an os.File for block-volume decompression or a tar.Reader
// for per-entry filesystem decompression.
func decompressInto(dst io.Writer, src io.Reader, ext string) error {
	switch ext {
	case ".zst":
		zr, err := zstd.NewReader(src)
		if err != nil {
			return fmt.Errorf("open zstd reader: %w", err)
		}
		defer zr.Close()

		_, err = io.Copy(dst, zr)

		return err
	case ".gz":
		gr, err := kgzip.NewReader(src)
		if err != nil {
			return fmt.Errorf("open gzip reader: %w", err)
		}
		defer gr.Close()

		_, err = io.Copy(dst, gr)

		return err
	case ".lz4":
		return decompressLZ4Frames(dst, src)
	default:
		// No codec extension (e.g. data.bin): the stored bytes are already raw.
		_, err := io.Copy(dst, src)

		return err
	}
}

// decompressLZ4Frames decodes a concatenation of independent lz4 frames from src.
// lz4.Reader decodes a single frame and reads exactly up to its end marker; a fresh
// reader is created per frame. A buffered reader is used so that peek-ahead can detect
// end-of-stream without consuming bytes that belong to the next frame.
func decompressLZ4Frames(dst io.Writer, src io.Reader) error {
	br := bufio.NewReader(src)

	for {
		if _, err := br.Peek(1); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("peek lz4 source: %w", err)
		}

		if _, err := io.Copy(dst, lz4.NewReader(br)); err != nil {
			return fmt.Errorf("decode lz4 frame: %w", err)
		}
	}
}
