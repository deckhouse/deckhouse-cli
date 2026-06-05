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

package archive_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// newTestMeta returns a minimal Meta suitable for test archives.
func newTestMeta(archiveID string) archive.Meta {
	return archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     archiveID,
		CreatedAt:     time.Now().UTC(),
		CreatedBy:     archive.Creator{Tool: "d8", Version: "test"},
		Source: archive.Source{
			Namespace:    "demo",
			RootSnapshot: archive.SnapshotRef{Name: "snap"},
		},
		Selection: archive.Selection{Mode: archive.SelectionFull, RootNodeID: "Snapshot--snap"},
	}
}

// TestAppendVolumeProgress_RoundTrip verifies that a VolumeProgressRecord
// written via AppendVolumeProgress is readable via VolumeProgress().
func TestAppendVolumeProgress_RoundTrip(t *testing.T) {
	dir := t.TempDir()

	w, err := archive.NewDirWriter(dir, newTestMeta("vol-roundtrip"))
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	rec := archive.VolumeProgressRecord{
		NodeID:     "Snapshot--snap",
		VSCName:    "vsc-disk-1",
		PVCName:    "my-pvc",
		VolumeMode: "Block",
		BytesDone:  1024,
		BytesTotal: 4096,
		Complete:   false,
	}

	if err := w.AppendVolumeProgress(rec); err != nil {
		t.Fatalf("AppendVolumeProgress: %v", err)
	}

	w.Close()

	r, err := archive.OpenDir(dir)
	if err != nil {
		t.Fatalf("NewDirReader: %v", err)
	}

	progs, err := r.VolumeProgress()
	if err != nil {
		t.Fatalf("VolumeProgress: %v", err)
	}

	key := archive.VolumeProgressKey("Snapshot--snap", "vsc-disk-1")

	got, ok := progs[key]
	if !ok {
		t.Fatalf("expected key %q in volume progress, not found", key)
	}

	if got.VSCName != "vsc-disk-1" {
		t.Errorf("VSCName = %q, want %q", got.VSCName, "vsc-disk-1")
	}

	if got.BytesDone != 1024 {
		t.Errorf("BytesDone = %d, want 1024", got.BytesDone)
	}

	if got.Complete {
		t.Error("expected Complete = false")
	}
}

// TestAppendVolumeProgress_LastWins verifies that when multiple records for
// the same key exist, the last one wins on read.
func TestAppendVolumeProgress_LastWins(t *testing.T) {
	dir := t.TempDir()

	w, err := archive.NewDirWriter(dir, newTestMeta("vol-lastwins"))
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	key := archive.VolumeProgressKey("Snapshot--snap", "vsc-disk-1")

	partial := archive.VolumeProgressRecord{
		NodeID:     "Snapshot--snap",
		VSCName:    "vsc-disk-1",
		VolumeMode: "Block",
		BytesDone:  512,
		BytesTotal: 4096,
		Complete:   false,
	}

	if err := w.AppendVolumeProgress(partial); err != nil {
		t.Fatalf("AppendVolumeProgress partial: %v", err)
	}

	full := archive.VolumeProgressRecord{
		NodeID:     "Snapshot--snap",
		VSCName:    "vsc-disk-1",
		VolumeMode: "Block",
		BytesDone:  4096,
		BytesTotal: 4096,
		Complete:   true,
	}

	if err := w.AppendVolumeProgress(full); err != nil {
		t.Fatalf("AppendVolumeProgress full: %v", err)
	}

	w.Close()

	r, err := archive.OpenDir(dir)
	if err != nil {
		t.Fatalf("NewDirReader: %v", err)
	}

	progs, err := r.VolumeProgress()
	if err != nil {
		t.Fatalf("VolumeProgress: %v", err)
	}

	got, ok := progs[key]
	if !ok {
		t.Fatalf("key %q not found", key)
	}

	if !got.Complete {
		t.Error("expected Complete = true (last record wins)")
	}

	if got.BytesDone != 4096 {
		t.Errorf("BytesDone = %d, want 4096", got.BytesDone)
	}
}

// TestOpenForResume_VolumeProgress verifies that OpenForResume reads existing
// volume progress records and returns them in the map.
func TestOpenForResume_VolumeProgress(t *testing.T) {
	dir := t.TempDir()

	// First run: write a partial progress entry.
	w1, err := archive.NewDirWriter(dir, newTestMeta("vol-resume"))
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	partial := archive.VolumeProgressRecord{
		NodeID:     "Snapshot--snap",
		VSCName:    "vsc-disk-1",
		VolumeMode: "Block",
		BytesDone:  2048,
		BytesTotal: 8192,
		Complete:   false,
	}

	if err := w1.AppendVolumeProgress(partial); err != nil {
		t.Fatalf("AppendVolumeProgress: %v", err)
	}

	w1.Close()

	// Second run: open for resume.
	_, _, existingVol, err := archive.OpenForResume(dir)
	if err != nil {
		t.Fatalf("OpenForResume: %v", err)
	}

	key := archive.VolumeProgressKey("Snapshot--snap", "vsc-disk-1")

	got, ok := existingVol[key]
	if !ok {
		t.Fatalf("expected volume progress key %q in resume map", key)
	}

	if got.BytesDone != 2048 {
		t.Errorf("BytesDone = %d, want 2048", got.BytesDone)
	}
}

// TestFinalize_VolumesSummary verifies that Finalize counts complete volumes
// in IndexSummary.Volumes.
func TestFinalize_VolumesSummary(t *testing.T) {
	dir := t.TempDir()

	w, err := archive.NewDirWriter(dir, newTestMeta("vol-summary"))
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	recs := []archive.VolumeProgressRecord{
		{NodeID: "Snapshot--snap", VSCName: "vsc-1", VolumeMode: "Block", Complete: true},
		{NodeID: "Snapshot--snap", VSCName: "vsc-2", VolumeMode: "Filesystem", Complete: true},
		{NodeID: "Snapshot--snap", VSCName: "vsc-3", VolumeMode: "Block", Complete: false},
	}

	for _, rec := range recs {
		if err := w.AppendVolumeProgress(rec); err != nil {
			t.Fatalf("AppendVolumeProgress: %v", err)
		}
	}

	nodeRec := archive.NodeRecord{ID: "Snapshot--snap", Kind: "Snapshot", Name: "snap", Children: []string{}}

	summary, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}, []archive.NodeRecord{nodeRec}, true)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	if summary.Volumes != 2 {
		t.Errorf("summary.Volumes = %d, want 2 (complete only)", summary.Volumes)
	}
}

// TestFinalize_Capabilities_Volumes verifies that index.json reflects
// IndexCapabilities.Volumes = true when includeVolumes is indicated via the Index.
func TestFinalize_IndexWritten(t *testing.T) {
	dir := t.TempDir()

	w, err := archive.NewDirWriter(dir, newTestMeta("vol-idx"))
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	idx := archive.Index{
		SchemaVersion: archive.SchemaVersion,
		Capabilities:  archive.IndexCapabilities{Manifests: true, Volumes: true},
	}

	nodeRec := archive.NodeRecord{ID: "Snapshot--snap", Kind: "Snapshot", Name: "snap", Children: []string{}}

	if _, err := w.Finalize(idx, []archive.NodeRecord{nodeRec}, true); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	idxPath := filepath.Join(dir, "index.json")

	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("read index.json: %v", err)
	}

	content := string(data)
	if !contains(content, `"volumes": true`) {
		t.Errorf("index.json does not contain volumes capability, got:\n%s", content)
	}
}

// TestVolumeProgressTruncatedLineTolerance verifies that a truncated line in
// volumes.jsonl is silently ignored on OpenForResume.
func TestVolumeProgressTruncatedLineTolerance(t *testing.T) {
	dir := t.TempDir()

	w, err := archive.NewDirWriter(dir, newTestMeta("vol-trunc"))
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	rec := archive.VolumeProgressRecord{
		NodeID:     "Snapshot--snap",
		VSCName:    "vsc-good",
		VolumeMode: "Block",
		BytesDone:  100,
		BytesTotal: 500,
		Complete:   false,
	}

	if err := w.AppendVolumeProgress(rec); err != nil {
		t.Fatalf("AppendVolumeProgress: %v", err)
	}

	w.Close()

	// Append a truncated line to volumes.jsonl.
	volPath := filepath.Join(dir, "indexes", "volumes.jsonl")

	f, err := os.OpenFile(volPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open volumes.jsonl for truncation: %v", err)
	}

	_, _ = f.WriteString(`{"nodeId":"Snapshot--snap","vscName":"vsc-truncated","volumeMode":"Block"`) // truncated
	_ = f.Close()

	_, _, existingVol, err := archive.OpenForResume(dir)
	if err != nil {
		t.Fatalf("OpenForResume with truncated volume progress: %v", err)
	}

	// The good record must be present.
	key := archive.VolumeProgressKey("Snapshot--snap", "vsc-good")
	if _, ok := existingVol[key]; !ok {
		t.Errorf("good volume progress record not found after truncation tolerance test")
	}
}

// contains is a minimal strings.Contains helper to avoid importing strings.
func contains(s, sub string) bool {
	for i := range s {
		if len(s[i:]) >= len(sub) && s[i:i+len(sub)] == sub {
			return true
		}
	}

	return false
}
