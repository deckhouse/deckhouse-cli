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
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	jsonlInitialBufferSize      = 64 * 1024
	progressMaxBufferSize       = 10 * 1024 * 1024
	volumeProgressMaxBufferSize = 1024 * 1024
)

type DirReader struct {
	dir string
}

func OpenDir(dir string) (*DirReader, error) {
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("archive directory %q: %w", dir, err)
	}

	if _, err := os.Stat(filepath.Join(dir, fileArchive)); err != nil {
		return nil, fmt.Errorf("not a snapshot archive (missing %s): %w", fileArchive, err)
	}

	return &DirReader{dir: dir}, nil
}

func (r *DirReader) Meta() (Meta, error) {
	var m Meta

	if err := readJSON(filepath.Join(r.dir, fileArchive), &m); err != nil {
		return Meta{}, fmt.Errorf("read %s: %w", fileArchive, err)
	}

	return m, nil
}

func (r *DirReader) Index() (Index, error) {
	var idx Index

	if err := readJSON(filepath.Join(r.dir, fileIndex), &idx); err != nil {
		return Index{}, fmt.Errorf("read %s: %w", fileIndex, err)
	}

	return idx, nil
}

func (r *DirReader) Nodes() ([]NodeRecord, error) {
	path := filepath.Join(r.dir, dirIndexes, fileNodes)

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	defer f.Close()

	var records []NodeRecord

	sc := bufio.NewScanner(f)

	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}

		var rec NodeRecord

		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, fmt.Errorf("decode node record: %w", err)
		}

		records = append(records, rec)
	}

	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}

	return records, nil
}

func (r *DirReader) ForEachObject(fn func(ObjectRecord) error) error {
	path := filepath.Join(r.dir, dirIndexes, fileObjects)

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}

	defer f.Close()

	sc := bufio.NewScanner(f)

	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}

		var rec ObjectRecord

		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return fmt.Errorf("decode object record: %w", err)
		}

		if err := fn(rec); err != nil {
			return err
		}
	}

	return sc.Err()
}

func (r *DirReader) VolumeProgress() (map[string]VolumeProgressRecord, error) {
	return readVolumeProgressFile(filepath.Join(r.dir, dirIndexes, fileVolumes))
}

// Dir returns the archive root directory.
func (r *DirReader) Dir() string {
	return r.dir
}

// ReadObjectBlob opens the gzip-compressed manifest blob referenced by rec,
// decompresses it, and returns the raw JSON bytes.
func (r *DirReader) ReadObjectBlob(rec ObjectRecord) ([]byte, error) {
	path := filepath.Join(r.dir, rec.Blob)

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open blob %s: %w", rec.Blob, err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gzip reader for blob %s: %w", rec.Blob, err)
	}
	defer gz.Close()

	data, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("read blob %s: %w", rec.Blob, err)
	}

	return data, nil
}

// readProgressFile reads a progress JSONL file and returns a map of records
// keyed by node ID. A truncated trailing line is silently skipped.
// The function is used by both DirReader.Progress and OpenForResume.
func readProgressFile(path string) (map[string]ProgressRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]ProgressRecord), nil
		}

		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	defer f.Close()

	result := make(map[string]ProgressRecord)

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, jsonlInitialBufferSize), progressMaxBufferSize)

	for sc.Scan() {
		line := sc.Text()

		if line == "" {
			continue
		}

		var rec ProgressRecord

		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			// Tolerate a truncated trailing line from a crash by breaking out.
			break
		}

		result[rec.NodeID] = rec
	}

	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}

	return result, nil
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, v)
}
