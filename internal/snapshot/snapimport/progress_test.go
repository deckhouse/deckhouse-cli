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
	"errors"
	"io"
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

func (s *recordingSink) SetVolumeTotal(n int) { s.inner.SetVolumeTotal(n) }

func (s *recordingSink) Wait() { s.inner.Wait() }

func (s *recordingSink) LogWriter() io.Writer { return s.inner.LogWriter() }

func (s *recordingSink) streamNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, len(s.streams))
	copy(out, s.streams)

	return out
}

// fakeStream is a progress.Stream stub that counts Activate, Done, and Fail
// calls, used to assert importNodeData's terminal-outcome contract (Done on
// success, Fail on error) independent of the real TTY/plain sink
// implementations. All methods are safe for concurrent use.
type fakeStream struct {
	mu          sync.Mutex
	activateCnt int
	doneCnt     int
	failCnt     int
}

func (s *fakeStream) IncrBy(_ int)       {}
func (s *fakeStream) SetTotal(_ int64)   {}
func (s *fakeStream) SetCurrent(_ int64) {}

func (s *fakeStream) Activate() {
	s.mu.Lock()
	s.activateCnt++
	s.mu.Unlock()
}

func (s *fakeStream) Done() {
	s.mu.Lock()
	s.doneCnt++
	s.mu.Unlock()
}

func (s *fakeStream) Fail() {
	s.mu.Lock()
	s.failCnt++
	s.mu.Unlock()
}

// fakeSink is a progress.Sink stub that hands out fakeStreams and records them
// in creation order, so a test can inspect exactly which terminal method each
// stream received.
type fakeSink struct {
	mu      sync.Mutex
	streams []*fakeStream
}

func (s *fakeSink) NewStream(_ string, _ int64) progress.Stream {
	fs := &fakeStream{}

	s.mu.Lock()
	s.streams = append(s.streams, fs)
	s.mu.Unlock()

	return fs
}

func (s *fakeSink) SetVolumeTotal(int) {}
func (s *fakeSink) Wait()              {}

func (s *fakeSink) LogWriter() io.Writer { return io.Discard }

func (s *fakeSink) snapshot() []*fakeStream {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*fakeStream, len(s.streams))
	copy(out, s.streams)

	return out
}

// failingVolumes is a VolumeImporter stub whose UploadVolumeData always fails,
// used to assert that importNodeData calls stream.Fail(), never stream.Done(),
// when the upload itself errors.
type failingVolumes struct{}

func (v *failingVolumes) DataImportName(leaf PlannedNode) string { return leaf.Name }

func (v *failingVolumes) EnsureDataImport(_ context.Context, leaf PlannedNode, _ string) (string, error) {
	return leaf.Name, nil
}

func (v *failingVolumes) UploadVolumeData(_ context.Context, _ PlannedNode, _, _ string, _ func(int64), _ func(int), _ func()) error {
	return errors.New("simulated upload failure")
}

// TestRun_UploadOutcome_CallsDoneOrFailByRealResult verifies importNodeData's
// terminal-outcome contract end to end via Run: a successful UploadVolumeData
// call must mark its stream Done and never Fail, while a failing
// UploadVolumeData call must mark its stream Fail and never Done — mirroring
// the analogous download-side fix. Before the fix, importNodeData deferred
// stream.Done() unconditionally right after creating the stream (before
// UploadVolumeData even ran), so this failure case would have observed
// doneCnt == 1 regardless of the upload outcome.
func TestRun_UploadOutcome_CallsDoneOrFailByRealResult(t *testing.T) {
	t.Run("success calls Done, never Fail", func(t *testing.T) {
		root, _ := buildMultiLeafArchive(t, 1)

		sink := &fakeSink{}
		vol := &stubVolumes{}
		up := &stubUploader{}
		dyn := newFakeDynamic(readyRootSnapshot())

		cfg := baseConfig(root, up, vol, dyn)
		cfg.Progress = sink

		if err := Run(context.Background(), cfg); err != nil {
			t.Fatalf("Run: %v", err)
		}

		streams := sink.snapshot()
		if len(streams) != 1 {
			t.Fatalf("expected exactly 1 stream, got %d", len(streams))
		}

		if streams[0].doneCnt != 1 {
			t.Errorf("doneCnt = %d, want 1 (a successful upload must call Done exactly once)", streams[0].doneCnt)
		}

		if streams[0].failCnt != 0 {
			t.Errorf("failCnt = %d, want 0 (a successful upload must never call Fail)", streams[0].failCnt)
		}
	})

	t.Run("failure calls Fail, never Done", func(t *testing.T) {
		root, _ := buildMultiLeafArchive(t, 1)

		sink := &fakeSink{}
		vol := &failingVolumes{}
		up := &stubUploader{}
		dyn := newFakeDynamic(readyRootSnapshot())

		cfg := baseConfig(root, up, vol, dyn)
		cfg.Progress = sink

		if err := Run(context.Background(), cfg); err == nil {
			t.Fatal("expected Run to fail when UploadVolumeData errors")
		}

		streams := sink.snapshot()
		if len(streams) != 1 {
			t.Fatalf("expected exactly 1 stream, got %d", len(streams))
		}

		if streams[0].doneCnt != 0 {
			t.Errorf("doneCnt = %d, want 0 (a failed upload must never call Done)", streams[0].doneCnt)
		}

		if streams[0].failCnt != 1 {
			t.Errorf("failCnt = %d, want 1 (a failed upload must call Fail exactly once)", streams[0].failCnt)
		}
	})
}

// activateControlledVolumes is a VolumeImporter stub whose UploadVolumeData invokes the
// activate callback exactly callActivate times, letting a test simulate a genuine transfer
// (callActivate > 0) versus a full server-side skip (callActivate == 0) independent of the
// real block/FS upload machinery, to verify importNodeData wires cfg.Progress's
// stream.Activate through to VolumeImporter's activate parameter (backlog #21 Bug A).
type activateControlledVolumes struct {
	callActivate int
}

func (v *activateControlledVolumes) DataImportName(leaf PlannedNode) string { return leaf.Name }

func (v *activateControlledVolumes) EnsureDataImport(_ context.Context, leaf PlannedNode, _ string) (string, error) {
	return leaf.Name, nil
}

func (v *activateControlledVolumes) UploadVolumeData(_ context.Context, _ PlannedNode, _, _ string, _ func(int64), _ func(int), activate func()) error {
	for range v.callActivate {
		if activate != nil {
			activate()
		}
	}

	return nil
}

// TestRun_ImportNodeData_WiresStreamActivate verifies that importNodeData passes its
// Stream's Activate method through to VolumeImporter.UploadVolumeData's activate parameter,
// and that a nil cfg.Progress never allocates a stream to begin with (no Activate to call).
func TestRun_ImportNodeData_WiresStreamActivate(t *testing.T) {
	t.Run("genuine transfer activates the stream", func(t *testing.T) {
		root, _ := buildMultiLeafArchive(t, 1)

		sink := &fakeSink{}
		vol := &activateControlledVolumes{callActivate: 1}
		up := &stubUploader{}
		dyn := newFakeDynamic(readyRootSnapshot())

		cfg := baseConfig(root, up, vol, dyn)
		cfg.Progress = sink

		if err := Run(context.Background(), cfg); err != nil {
			t.Fatalf("Run: %v", err)
		}

		streams := sink.snapshot()
		if len(streams) != 1 {
			t.Fatalf("expected exactly 1 stream, got %d", len(streams))
		}

		if streams[0].activateCnt != 1 {
			t.Errorf("activateCnt = %d, want 1 (importNodeData must wire stream.Activate through to VolumeImporter)", streams[0].activateCnt)
		}
	})

	t.Run("full skip never activates the stream", func(t *testing.T) {
		root, _ := buildMultiLeafArchive(t, 1)

		sink := &fakeSink{}
		vol := &activateControlledVolumes{callActivate: 0}
		up := &stubUploader{}
		dyn := newFakeDynamic(readyRootSnapshot())

		cfg := baseConfig(root, up, vol, dyn)
		cfg.Progress = sink

		if err := Run(context.Background(), cfg); err != nil {
			t.Fatalf("Run: %v", err)
		}

		streams := sink.snapshot()
		if len(streams) != 1 {
			t.Fatalf("expected exactly 1 stream, got %d", len(streams))
		}

		if streams[0].activateCnt != 0 {
			t.Errorf("activateCnt = %d, want 0 (a full server-side skip must never activate)", streams[0].activateCnt)
		}
	})
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

func (v *progressReportingVolumes) UploadVolumeData(_ context.Context, _ PlannedNode, _, _ string, setTotal func(int64), onProgress func(int), activate func()) error {
	if setTotal != nil && v.bytesPerLeaf > 0 {
		setTotal(int64(v.bytesPerLeaf))
	}

	if onProgress != nil && v.bytesPerLeaf > 0 {
		onProgress(v.bytesPerLeaf)
	}

	if activate != nil {
		activate()
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
	dyn := newFakeDynamic(readyRootSnapshot())

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

// TestRun_ProgressStreamName_PrefersSourceObjectRef verifies that a domain data leaf's
// progress stream is named after the ORIGINAL captured source object (SourceObjectRef,
// e.g. "DemoVirtualDisk/disk-a"), not the generated snapshot CR identity
// ("DemoVirtualDiskSnapshot/dvd-snap-1") — mirroring source.Node.DisplayLabel on the
// download side (see nodeDisplayLabel in import.go).
func TestRun_ProgressStreamName_PrefersSourceObjectRef(t *testing.T) {
	root := buildDomainDataLeafArchive(t)

	var buf bytes.Buffer

	inner := progress.New(&buf, false, progress.WithInterval(time.Hour))
	sink := &recordingSink{inner: inner}

	up := &stubUploader{}
	vol := &stubVolumes{}
	dyn := newFakeDynamic(readyRootSnapshot())

	cfg := baseConfig(root, up, vol, dyn)
	cfg.Mapper = testDomainMapper()
	cfg.Progress = sink

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	sink.Wait()

	streams := sink.streamNames()
	if len(streams) != 1 || streams[0] != "DemoVirtualDisk/disk-a" {
		t.Errorf("progress stream names = %v, want [DemoVirtualDisk/disk-a] (original captured source identity, not the CR name)", streams)
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
	dyn := newFakeDynamic(readyRootSnapshot())

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
