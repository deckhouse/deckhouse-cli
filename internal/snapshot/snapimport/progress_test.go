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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/progress"
)

// recordingSink wraps a Sink and captures the name of every NewStream call so tests can
// assert the exact set of per-leaf streams that are created.
type recordingSink struct {
	inner   progress.Sink
	mu      sync.Mutex
	streams []string
}

func (s *recordingSink) NewStream(name string, total int64) progress.Stream {
	s.mu.Lock()
	s.streams = append(s.streams, name)
	s.mu.Unlock()

	return s.inner.NewStream(name, total)
}

func (s *recordingSink) Wait() { s.inner.Wait() }

func (s *recordingSink) streamNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, len(s.streams))
	copy(out, s.streams)

	return out
}

// progressReportingVolumes is a VolumeImporter stub that calls onProgress with a fixed byte
// count per leaf so tests can verify the aggregate progress accounting in the Sink.
type progressReportingVolumes struct {
	bytesPerLeaf int
}

func (v *progressReportingVolumes) DataImportName(leaf PlannedNode) string { return leaf.Name }

func (v *progressReportingVolumes) EnsureDataImport(_ context.Context, leaf PlannedNode, _ string) (string, error) {
	return leaf.Name, nil
}

func (v *progressReportingVolumes) UploadVolumeData(_ context.Context, _ PlannedNode, _, _ string, onProgress func(int)) error {
	if onProgress != nil && v.bytesPerLeaf > 0 {
		onProgress(v.bytesPerLeaf)
	}

	return nil
}

// TestRun_ProgressOneStreamPerDataLeaf verifies that exactly one progress Stream is
// created for each data leaf and that structural (non-data) nodes such as the root
// Snapshot do not receive a stream.
func TestRun_ProgressOneStreamPerDataLeaf(t *testing.T) {
	const numLeaves = 3

	root, leafNames := buildMultiLeafArchive(t, numLeaves)

	var buf bytes.Buffer

	inner := progress.New(&buf, false, progress.WithInterval(time.Hour))
	sink := &recordingSink{inner: inner}

	vol := &stubVolumes{}
	up := &stubUploader{}
	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	cfg := baseConfig(root, up, vol, dyn)
	cfg.Progress = sink

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	sink.Wait()

	streams := sink.streamNames()
	if len(streams) != len(leafNames) {
		t.Errorf("expected %d progress streams (one per data leaf, none for root Snapshot), got %d: %v",
			len(leafNames), len(streams), streams)
	}
}

// TestRun_NonTTYProgress_WritesAggregateLine verifies that the non-TTY fallback Sink
// emits a "downloaded X / total Y" aggregate line on Wait containing the sum of all
// bytes reported by the per-leaf onProgress hooks.
// Two data leaves each report bytesPerLeaf bytes; the final Wait line must contain
// the humanised sum (2 × 2048 B = 4096 B = 4.0 KiB in binary units).
func TestRun_NonTTYProgress_WritesAggregateLine(t *testing.T) {
	const numLeaves = 2
	const bytesPerLeaf = 2048 // 2 × 2048 B = 4096 B = 4.0 KiB

	root, _ := buildMultiLeafArchive(t, numLeaves)

	var buf bytes.Buffer

	sink := progress.New(&buf, false, progress.WithInterval(time.Hour))

	vol := &progressReportingVolumes{bytesPerLeaf: bytesPerLeaf}
	up := &stubUploader{}
	dyn := newFakeDynamic(readyRootSnapshot(), readyContent())

	cfg := baseConfig(root, up, vol, dyn)
	cfg.Progress = sink

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	sink.Wait()

	out := buf.String()
	if !strings.Contains(out, "downloaded") {
		t.Errorf("non-TTY sink output missing 'downloaded' line; got: %q", out)
	}

	// decor.SizeB1024(4096) with format % .1f renders as "4.0 KiB".
	if !strings.Contains(out, "4.0 KiB") {
		t.Errorf("expected '4.0 KiB' in non-TTY progress output (2 leaves × 2048 B = 4096 B); got: %q", out)
	}
}
