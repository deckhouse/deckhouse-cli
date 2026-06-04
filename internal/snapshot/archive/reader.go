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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// DirReader reads a snapshot archive directory written by DirWriter.
type DirReader struct {
	dir string
}

// OpenDir opens an archive directory for reading.
// Returns an error if the directory or archive.json is absent.
func OpenDir(dir string) (*DirReader, error) {
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("archive directory %q: %w", dir, err)
	}

	if _, err := os.Stat(filepath.Join(dir, fileArchive)); err != nil {
		return nil, fmt.Errorf("not a snapshot archive (missing %s): %w", fileArchive, err)
	}

	return &DirReader{dir: dir}, nil
}

// Meta reads and parses archive.json.
func (r *DirReader) Meta() (Meta, error) {
	var m Meta

	if err := readJSON(filepath.Join(r.dir, fileArchive), &m); err != nil {
		return Meta{}, fmt.Errorf("read %s: %w", fileArchive, err)
	}

	return m, nil
}

// Index reads and parses index.json.
func (r *DirReader) Index() (Index, error) {
	var idx Index

	if err := readJSON(filepath.Join(r.dir, fileIndex), &idx); err != nil {
		return Index{}, fmt.Errorf("read %s: %w", fileIndex, err)
	}

	return idx, nil
}

// Nodes reads indexes/nodes.jsonl and returns all NodeRecords.
// Lines are decoded one at a time to avoid buffering the entire file.
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

// ForEachObject streams indexes/objects.jsonl and calls fn for each ObjectRecord.
// fn is called in order; iteration stops and the error is returned if fn returns one.
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

// readJSON reads a JSON file and unmarshals it into v.
func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, v)
}
