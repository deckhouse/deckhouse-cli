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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// DirWriter writes a snapshot archive to a directory on the local filesystem.
type DirWriter struct {
	dir           string
	nodesWriter   *bufio.Writer
	objectsWriter *bufio.Writer
	nodesFile     *os.File
	objectsFile   *os.File
	seenDigests   map[string]int64
	nodeCount     int
	objectCount   int
}

// NewDirWriter creates the archive directory structure and opens index files for writing.
func NewDirWriter(dir string, meta Meta) (*DirWriter, error) {
	for _, sub := range []string{dirIndexes, dirObjects, dirData} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("create archive dir %s: %w", sub, err)
		}
	}

	archivePath := filepath.Join(dir, fileArchive)

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal archive metadata: %w", err)
	}

	if err = os.WriteFile(archivePath, data, 0o644); err != nil {
		return nil, fmt.Errorf("write %s: %w", fileArchive, err)
	}

	nodesF, err := os.Create(filepath.Join(dir, dirIndexes, fileNodes))
	if err != nil {
		return nil, fmt.Errorf("create nodes index: %w", err)
	}

	objectsF, err := os.Create(filepath.Join(dir, dirIndexes, fileObjects))
	if err != nil {
		nodesF.Close()

		return nil, fmt.Errorf("create objects index: %w", err)
	}

	return &DirWriter{
		dir:           dir,
		nodesFile:     nodesF,
		nodesWriter:   bufio.NewWriter(nodesF),
		objectsFile:   objectsF,
		objectsWriter: bufio.NewWriter(objectsF),
		seenDigests:   make(map[string]int64),
	}, nil
}

// AppendNode writes one NodeRecord to the nodes index.
func (w *DirWriter) AppendNode(rec NodeRecord) error {
	if err := appendRecord(w.nodesWriter, &w.nodeCount, rec); err != nil {
		return fmt.Errorf("append node %s: %w", rec.ID, err)
	}

	return nil
}

// AppendObject writes one ObjectRecord to the objects index.
func (w *DirWriter) AppendObject(rec ObjectRecord) error {
	if err := appendRecord(w.objectsWriter, &w.objectCount, rec); err != nil {
		return fmt.Errorf("append object %s/%s/%s: %w", rec.APIVersion, rec.Kind, rec.Name, err)
	}

	return nil
}

// AddObject content-addresses rawJSON, writes a gzipped blob if not yet present,
// and returns a populated ObjectRecord ready for AppendObject.
// map[string]any is used because manifests are arbitrary Kubernetes objects with
// no fixed Go type; re-marshalling produces canonical key order for a stable digest.
func (w *DirWriter) AddObject(nodeID string, rawJSON []byte) (ObjectRecord, error) {
	var obj map[string]any

	if err := json.Unmarshal(rawJSON, &obj); err != nil {
		return ObjectRecord{}, fmt.Errorf("unmarshal manifest: %w", err)
	}

	canonical, err := json.Marshal(obj)
	if err != nil {
		return ObjectRecord{}, fmt.Errorf("marshal canonical JSON: %w", err)
	}

	sum := sha256.Sum256(canonical)
	digest := hex.EncodeToString(sum[:])

	blobRel, err := BlobPath(digest)
	if err != nil {
		return ObjectRecord{}, err
	}

	apiVersion, kind, name, ns := extractIdentity(obj)

	rec := ObjectRecord{
		NodeID:     nodeID,
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       name,
		Namespace:  ns,
		Digest:     digest,
		Blob:       blobRel,
	}

	if size, seen := w.seenDigests[digest]; seen {
		rec.Size = size

		return rec, nil
	}

	blobAbs := filepath.Join(w.dir, blobRel)

	if err := os.MkdirAll(filepath.Dir(blobAbs), 0o755); err != nil {
		return ObjectRecord{}, fmt.Errorf("create blob dir for %s: %w", digest, err)
	}

	written, err := writeGzipBlob(blobAbs, canonical)
	if err != nil {
		return ObjectRecord{}, fmt.Errorf("write blob %s: %w", digest, err)
	}

	rec.Size = written
	w.seenDigests[digest] = written

	return rec, nil
}

// Finalize flushes all index writers, writes index.json, and creates the COMPLETE sentinel.
// It returns a populated IndexSummary with the final counts.
func (w *DirWriter) Finalize(idx Index) (IndexSummary, error) {
	if err := w.nodesWriter.Flush(); err != nil {
		return IndexSummary{}, fmt.Errorf("flush nodes index: %w", err)
	}

	if err := w.objectsWriter.Flush(); err != nil {
		return IndexSummary{}, fmt.Errorf("flush objects index: %w", err)
	}

	if err := w.nodesFile.Close(); err != nil {
		return IndexSummary{}, fmt.Errorf("close nodes file: %w", err)
	}

	if err := w.objectsFile.Close(); err != nil {
		return IndexSummary{}, fmt.Errorf("close objects file: %w", err)
	}

	summary := IndexSummary{
		Nodes:    w.nodeCount,
		Objects:  w.objectCount,
		Complete: true,
	}

	idx.Summary = summary

	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return IndexSummary{}, fmt.Errorf("marshal index: %w", err)
	}

	if err = os.WriteFile(filepath.Join(w.dir, fileIndex), data, 0o644); err != nil {
		return IndexSummary{}, fmt.Errorf("write %s: %w", fileIndex, err)
	}

	completeFile := filepath.Join(w.dir, fileComplete)
	stamp := []byte(time.Now().UTC().Format(time.RFC3339) + "\n")

	if err = os.WriteFile(completeFile, stamp, 0o644); err != nil {
		return IndexSummary{}, fmt.Errorf("write %s: %w", fileComplete, err)
	}

	return summary, nil
}

// appendRecord serializes rec as a JSONL line into bw and increments *count.
func appendRecord(bw *bufio.Writer, count *int, rec any) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	if _, err = bw.Write(data); err != nil {
		return err
	}

	if err = bw.WriteByte('\n'); err != nil {
		return err
	}

	*count++

	return nil
}

// extractIdentity pulls apiVersion, kind, name, and namespace from a decoded manifest.
func extractIdentity(obj map[string]any) (string, string, string, string) {
	apiVersion, _ := obj["apiVersion"].(string)
	kind, _ := obj["kind"].(string)

	var name, ns string

	meta, _ := obj["metadata"].(map[string]any)
	if meta != nil {
		name, _ = meta["name"].(string)
		ns, _ = meta["namespace"].(string)
	}

	return apiVersion, kind, name, ns
}

// writeGzipBlob writes data to path as a gzip-compressed file and returns the compressed size.
func writeGzipBlob(path string, data []byte) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}

	defer f.Close()

	gz := gzip.NewWriter(f)

	if _, err = gz.Write(data); err != nil {
		return 0, err
	}

	if err = gz.Close(); err != nil {
		return 0, err
	}

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}
