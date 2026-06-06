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
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

type DirWriter struct {
	dir                   string
	progressFile          *os.File
	progressWriter        *bufio.Writer
	progressRecords       []ProgressRecord
	seenDigests           map[string]int64
	volumeProgressFile    *os.File
	volumeProgressWriter  *bufio.Writer
	volumeProgressRecords []VolumeProgressRecord
}

type objectIndexResult struct {
	count           int
	referencedBlobs map[string]struct{}
}

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

	progressF, err := os.Create(filepath.Join(dir, dirIndexes, fileProgress))
	if err != nil {
		return nil, fmt.Errorf("create progress index: %w", err)
	}

	volProgressF, err := os.Create(filepath.Join(dir, dirIndexes, fileVolumes))
	if err != nil {
		progressF.Close()
		return nil, fmt.Errorf("create volume progress index: %w", err)
	}

	return &DirWriter{
		dir:                  dir,
		progressFile:         progressF,
		progressWriter:       bufio.NewWriter(progressF),
		seenDigests:          make(map[string]int64),
		volumeProgressFile:   volProgressF,
		volumeProgressWriter: bufio.NewWriter(volProgressF),
	}, nil
}

func OpenForResume(dir string) (*DirWriter, map[string]ProgressRecord, map[string]VolumeProgressRecord, error) {
	existing, err := readProgressFile(filepath.Join(dir, dirIndexes, fileProgress))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read progress: %w", err)
	}

	existingVol, err := readVolumeProgressFile(filepath.Join(dir, dirIndexes, fileVolumes))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read volume progress: %w", err)
	}

	seenDigests := make(map[string]int64, len(existing)*4)

	for _, prec := range existing {
		for _, obj := range prec.Objects {
			seenDigests[obj.Digest] = obj.Size
		}
	}

	progressF, err := os.OpenFile(
		filepath.Join(dir, dirIndexes, fileProgress),
		os.O_APPEND|os.O_WRONLY|os.O_CREATE,
		0o644,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open progress for append: %w", err)
	}

	volProgressF, err := os.OpenFile(
		filepath.Join(dir, dirIndexes, fileVolumes),
		os.O_APPEND|os.O_WRONLY|os.O_CREATE,
		0o644,
	)
	if err != nil {
		progressF.Close()
		return nil, nil, nil, fmt.Errorf("open volume progress for append: %w", err)
	}

	precs := make([]ProgressRecord, 0, len(existing))
	for _, prec := range existing {
		precs = append(precs, prec)
	}

	volPrecs := make([]VolumeProgressRecord, 0, len(existingVol))
	for _, vrec := range existingVol {
		volPrecs = append(volPrecs, vrec)
	}

	w := &DirWriter{
		dir:                   dir,
		progressFile:          progressF,
		progressWriter:        bufio.NewWriter(progressF),
		progressRecords:       precs,
		seenDigests:           seenDigests,
		volumeProgressFile:    volProgressF,
		volumeProgressWriter:  bufio.NewWriter(volProgressF),
		volumeProgressRecords: volPrecs,
	}

	return w, existing, existingVol, nil
}

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

	written, err := writeGzipBlobAtomic(blobAbs, canonical)
	if err != nil {
		return ObjectRecord{}, fmt.Errorf("write blob %s: %w", digest, err)
	}

	rec.Size = written
	w.seenDigests[digest] = written

	return rec, nil
}

func (w *DirWriter) Close() {
	_ = w.progressWriter.Flush()
	_ = w.progressFile.Close()

	if w.volumeProgressWriter != nil {
		_ = w.volumeProgressWriter.Flush()
	}

	if w.volumeProgressFile != nil {
		_ = w.volumeProgressFile.Close()
	}
}

func (w *DirWriter) AppendVolumeProgress(rec VolumeProgressRecord) error {
	if err := appendJSONLRecord(w.volumeProgressWriter, rec); err != nil {
		return fmt.Errorf("append volume progress for %s/%s: %w", rec.NodeID, rec.VSCName, err)
	}

	if err := w.volumeProgressWriter.Flush(); err != nil {
		return fmt.Errorf("flush volume progress for %s/%s: %w", rec.NodeID, rec.VSCName, err)
	}

	if err := w.volumeProgressFile.Sync(); err != nil {
		return fmt.Errorf("sync volume progress for %s/%s: %w", rec.NodeID, rec.VSCName, err)
	}

	w.volumeProgressRecords = append(w.volumeProgressRecords, rec)

	return nil
}

func VolumeProgressKey(nodeID, vscName string) string {
	return nodeID + "/" + vscName
}

func (w *DirWriter) DataDir() string {
	return filepath.Join(w.dir, dirData)
}

func (w *DirWriter) AppendProgress(rec ProgressRecord) error {
	if err := appendJSONLRecord(w.progressWriter, rec); err != nil {
		return fmt.Errorf("append progress for %s: %w", rec.NodeID, err)
	}

	if err := w.progressWriter.Flush(); err != nil {
		return fmt.Errorf("flush progress for %s: %w", rec.NodeID, err)
	}

	if err := w.progressFile.Sync(); err != nil {
		return fmt.Errorf("sync progress for %s: %w", rec.NodeID, err)
	}

	w.progressRecords = append(w.progressRecords, rec)

	return nil
}

func (w *DirWriter) Finalize(idx Index, liveNodes []NodeRecord, complete bool) (IndexSummary, error) {
	if err := w.closeProgressFiles(); err != nil {
		return IndexSummary{}, err
	}

	finalByNode := finalProgressRecords(w.progressRecords)

	if err := w.writeNodeIndex(liveNodes); err != nil {
		return IndexSummary{}, fmt.Errorf("write nodes.jsonl: %w", err)
	}

	objects, err := w.writeObjectIndex(finalByNode)
	if err != nil {
		return IndexSummary{}, fmt.Errorf("write objects.jsonl: %w", err)
	}

	if err := gcBlobs(filepath.Join(w.dir, dirObjects), objects.referencedBlobs); err != nil {
		return IndexSummary{}, fmt.Errorf("gc blobs: %w", err)
	}

	summary := IndexSummary{
		Nodes:    len(liveNodes),
		Objects:  objects.count,
		Volumes:  w.completeVolumeCount(),
		Complete: complete,
	}

	if err := w.writeIndex(idx, summary); err != nil {
		return IndexSummary{}, err
	}

	if complete {
		if err := w.writeCompleteSentinel(); err != nil {
			return IndexSummary{}, err
		}
	}

	return summary, nil
}

func (w *DirWriter) closeProgressFiles() error {
	if err := w.progressWriter.Flush(); err != nil {
		return fmt.Errorf("flush progress file: %w", err)
	}

	if err := w.progressFile.Close(); err != nil {
		return fmt.Errorf("close progress file: %w", err)
	}

	if w.volumeProgressWriter != nil {
		if err := w.volumeProgressWriter.Flush(); err != nil {
			return fmt.Errorf("flush volume progress file: %w", err)
		}
	}

	if w.volumeProgressFile == nil {
		return nil
	}

	if err := w.volumeProgressFile.Close(); err != nil {
		return fmt.Errorf("close volume progress file: %w", err)
	}

	return nil
}

func finalProgressRecords(records []ProgressRecord) map[string]ProgressRecord {
	result := make(map[string]ProgressRecord, len(records))

	for _, rec := range records {
		result[rec.NodeID] = rec
	}

	return result
}

func (w *DirWriter) writeNodeIndex(liveNodes []NodeRecord) error {
	return writeJSONLFile(filepath.Join(w.dir, dirIndexes, fileNodes), func(enc *json.Encoder) error {
		for _, nr := range liveNodes {
			if err := enc.Encode(nr); err != nil {
				return err
			}
		}

		return nil
	})
}

func (w *DirWriter) writeObjectIndex(finalByNode map[string]ProgressRecord) (objectIndexResult, error) {
	result := objectIndexResult{
		referencedBlobs: make(map[string]struct{}, len(finalByNode)*4),
	}

	err := writeJSONLFile(filepath.Join(w.dir, dirIndexes, fileObjects), func(enc *json.Encoder) error {
		for _, prec := range finalByNode {
			for _, obj := range prec.Objects {
				if err := enc.Encode(obj); err != nil {
					return err
				}

				result.referencedBlobs[filepath.Join(w.dir, obj.Blob)] = struct{}{}
				result.count++
			}
		}

		return nil
	})

	return result, err
}

func (w *DirWriter) completeVolumeCount() int {
	count := 0

	for _, rec := range w.volumeProgressRecords {
		if rec.Complete {
			count++
		}
	}

	return count
}

func (w *DirWriter) writeIndex(idx Index, summary IndexSummary) error {
	idx.Summary = summary

	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}

	if err := os.WriteFile(filepath.Join(w.dir, fileIndex), data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", fileIndex, err)
	}

	return nil
}

func (w *DirWriter) writeCompleteSentinel() error {
	stamp := []byte(time.Now().UTC().Format(time.RFC3339) + "\n")

	if err := os.WriteFile(filepath.Join(w.dir, fileComplete), stamp, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", fileComplete, err)
	}

	return nil
}

func WipeDir(dir string) error {
	for _, f := range []string{fileArchive, fileIndex, fileComplete} {
		_ = os.Remove(filepath.Join(dir, f))
	}

	for _, sub := range []string{dirIndexes, dirObjects, dirData} {
		if err := os.RemoveAll(filepath.Join(dir, sub)); err != nil {
			return fmt.Errorf("remove %s: %w", sub, err)
		}
	}

	return nil
}

func writeJSONLFile(path string, fn func(*json.Encoder) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	defer f.Close()

	return fn(json.NewEncoder(f))
}

func readVolumeProgressFile(path string) (map[string]VolumeProgressRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]VolumeProgressRecord), nil
		}

		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	defer f.Close()

	result := make(map[string]VolumeProgressRecord)

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, jsonlInitialBufferSize), volumeProgressMaxBufferSize)

	for sc.Scan() {
		line := sc.Text()

		if line == "" {
			continue
		}

		var rec VolumeProgressRecord

		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			break
		}

		result[VolumeProgressKey(rec.NodeID, rec.VSCName)] = rec
	}

	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}

	return result, nil
}

func gcBlobs(blobDir string, referenced map[string]struct{}) error {
	return filepath.WalkDir(blobDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		if _, ok := referenced[path]; !ok {
			_ = os.Remove(path)
		}

		return nil
	})
}

func appendJSONLRecord(bw *bufio.Writer, rec any) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	if _, err = bw.Write(data); err != nil {
		return err
	}

	return bw.WriteByte('\n')
}

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

// writeGzipBlobAtomic writes data as gzip to path using a tmp→fsync→rename
// sequence so a crash leaves either the complete blob or nothing.
func writeGzipBlobAtomic(path string, data []byte) (int64, error) {
	tmpPath := path + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return 0, err
	}

	defer func() { _ = os.Remove(tmpPath) }()

	gz := gzip.NewWriter(f)

	if _, err = gz.Write(data); err != nil {
		f.Close()

		return 0, err
	}

	if err = gz.Close(); err != nil {
		f.Close()

		return 0, err
	}

	if err = f.Sync(); err != nil {
		f.Close()

		return 0, err
	}

	if err = f.Close(); err != nil {
		return 0, err
	}

	if err = os.Rename(tmpPath, path); err != nil {
		return 0, err
	}

	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}
