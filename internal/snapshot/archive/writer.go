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

// DirWriter writes a snapshot archive to a directory on the local filesystem.
// Progress is checkpointed durably to indexes/progress.jsonl after each node.
// Volume progress is checkpointed to indexes/volumes.jsonl after each volume download.
// Finalize regenerates nodes.jsonl, objects.jsonl, and index.json from the
// accumulated ProgressRecords, so indexes survive a crash and resume.
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

// NewDirWriter creates the archive directory structure, writes archive.json,
// and opens progress.jsonl for writing.
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

// OpenForResume opens an existing archive directory for incremental download.
// It reads the existing progress records and rebuilds the seenDigests set so
// blobs that are already on disk are not re-downloaded.
// Returns the writer, a map of existing manifest progress records keyed by node ID,
// and a map of volume progress records keyed by "nodeID/vscName".
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

// AddObject content-addresses rawJSON and writes a gzipped blob atomically
// (write to .tmp → fsync → rename) to prevent half-written blobs surviving a crash.
// Returns a populated ObjectRecord ready for inclusion in a ProgressRecord.
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

// Close closes the progress files without finalising the archive.
// Use it when aborting an in-progress write (e.g. noop early exit).
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

// AppendVolumeProgress records a volume download progress durably.
// It is appended to volumes.jsonl and fsynced before returning.
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

// VolumeProgressKey returns the map key for a VolumeProgressRecord.
func VolumeProgressKey(nodeID, vscName string) string {
	return nodeID + "/" + vscName
}

// DataDir returns the absolute path to the data/ directory within the archive.
func (w *DirWriter) DataDir() string {
	return filepath.Join(w.dir, dirData)
}

// AppendProgress records a node's completion durably. It must be called only
// after all blobs for the node are written and their writes are durable on disk.
// The record is appended to progress.jsonl and fsynced before returning.
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

// Finalize regenerates nodes.jsonl and objects.jsonl from the accumulated
// progress records and the live node list, writes index.json, GCs orphan blobs,
// and writes the COMPLETE sentinel when complete is true.
// It closes the progress files before writing the generated files.
func (w *DirWriter) Finalize(idx Index, liveNodes []NodeRecord, complete bool) (IndexSummary, error) {
	if err := w.progressFile.Close(); err != nil {
		return IndexSummary{}, fmt.Errorf("close progress file: %w", err)
	}

	if w.volumeProgressWriter != nil {
		if err := w.volumeProgressWriter.Flush(); err != nil {
			return IndexSummary{}, fmt.Errorf("flush volume progress file: %w", err)
		}
	}

	if w.volumeProgressFile != nil {
		if err := w.volumeProgressFile.Close(); err != nil {
			return IndexSummary{}, fmt.Errorf("close volume progress file: %w", err)
		}
	}

	// Deduplicate progress records by nodeID: last write wins (handles update case).
	finalByNode := make(map[string]ProgressRecord, len(w.progressRecords))

	for _, prec := range w.progressRecords {
		finalByNode[prec.NodeID] = prec
	}

	// Regenerate nodes.jsonl from liveNodes.
	if err := writeJSONLFile(filepath.Join(w.dir, dirIndexes, fileNodes), func(enc *json.Encoder) error {
		for _, nr := range liveNodes {
			if err := enc.Encode(nr); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return IndexSummary{}, fmt.Errorf("write nodes.jsonl: %w", err)
	}

	// Regenerate objects.jsonl from all final progress records.
	objectCount := 0

	referencedBlobs := make(map[string]struct{}, len(finalByNode)*4)

	if err := writeJSONLFile(filepath.Join(w.dir, dirIndexes, fileObjects), func(enc *json.Encoder) error {
		for _, prec := range finalByNode {
			for _, obj := range prec.Objects {
				if err := enc.Encode(obj); err != nil {
					return err
				}

				referencedBlobs[filepath.Join(w.dir, obj.Blob)] = struct{}{}
				objectCount++
			}
		}

		return nil
	}); err != nil {
		return IndexSummary{}, fmt.Errorf("write objects.jsonl: %w", err)
	}

	// GC blobs that are no longer referenced by any progress record.
	if err := gcBlobs(filepath.Join(w.dir, dirObjects), referencedBlobs); err != nil {
		return IndexSummary{}, fmt.Errorf("gc blobs: %w", err)
	}

	// Count complete volumes.
	volumeCount := 0

	for _, vrec := range w.volumeProgressRecords {
		if vrec.Complete {
			volumeCount++
		}
	}

	// Write index.json.
	summary := IndexSummary{
		Nodes:    len(liveNodes),
		Objects:  objectCount,
		Volumes:  volumeCount,
		Complete: complete,
	}
	idx.Summary = summary

	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return IndexSummary{}, fmt.Errorf("marshal index: %w", err)
	}

	if err = os.WriteFile(filepath.Join(w.dir, fileIndex), data, 0o644); err != nil {
		return IndexSummary{}, fmt.Errorf("write %s: %w", fileIndex, err)
	}

	// Write COMPLETE sentinel last; absent means download is incomplete.
	if complete {
		stamp := []byte(time.Now().UTC().Format(time.RFC3339) + "\n")

		if err = os.WriteFile(filepath.Join(w.dir, fileComplete), stamp, 0o644); err != nil {
			return IndexSummary{}, fmt.Errorf("write %s: %w", fileComplete, err)
		}
	}

	return summary, nil
}

// WipeDir removes all archive content from dir, keeping the directory itself,
// so that NewDirWriter can be called on the same path for a fresh download.
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

// writeJSONLFile creates (or truncates) path and calls fn with an encoder.
func writeJSONLFile(path string, fn func(*json.Encoder) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	defer f.Close()

	return fn(json.NewEncoder(f))
}

// readVolumeProgressFile reads volumes.jsonl and returns a map of VolumeProgressRecords
// keyed by VolumeProgressKey (nodeID+"/"+vscName). When multiple records for the same
// key are present, the last one wins. A truncated trailing line is silently skipped.
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
	sc.Buffer(make([]byte, 0, 64*1024), 1*1024*1024)

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

// gcBlobs removes blob files under blobDir that are not in referenced.
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

// appendJSONLRecord serializes rec as a JSONL line into bw.
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
