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

package restore

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

const unawaitedReportHeader = "The following restored PersistentVolumeClaims are not awaited:"

func wffcClass(name string) resolvedStorageClass {
	return resolvedStorageClass{name: name, bindingMode: volumeBindingModeWFC}
}

// TestReportUnawaitedPVCs renders the list directly, including the cross-namespace
// ordering that a single-namespace Run cannot produce.
func TestReportUnawaitedPVCs(t *testing.T) {
	t.Parallel()

	t.Run("no claims writes nothing", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer

		if err := reportUnawaitedPVCs(&out, nil); err != nil {
			t.Fatalf("reportUnawaitedPVCs with no claims: %v", err)
		}

		if out.Len() != 0 {
			t.Errorf("empty claim list wrote %q, want no output", out.String())
		}
	})

	t.Run("orders by namespace then name", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer

		claims := []unawaitedPVC{
			{namespace: "beta", name: "pvc-b", storageClass: wffcClass("sc")},
			{namespace: "alpha", name: "pvc-z", storageClass: wffcClass("sc")},
			{namespace: "beta", name: "pvc-a", storageClass: wffcClass("sc")},
			{namespace: "alpha", name: "pvc-a", storageClass: wffcClass("sc")},
		}

		if err := reportUnawaitedPVCs(&out, claims); err != nil {
			t.Fatalf("reportUnawaitedPVCs: %v", err)
		}

		assertOrderedIdentities(t, out.String(), []string{
			"alpha/pvc-a",
			"alpha/pvc-z",
			"beta/pvc-a",
			"beta/pvc-b",
		})
	})

	t.Run("failed write is reported to the caller", func(t *testing.T) {
		t.Parallel()

		claims := []unawaitedPVC{
			{namespace: testNS, name: "pvc-1", storageClass: wffcClass("sc")},
		}

		err := reportUnawaitedPVCs(failingWriter{}, claims)
		if err == nil {
			t.Fatal("expected a write failure to be returned")
		}

		if !strings.Contains(err.Error(), "pipe") {
			t.Errorf("error %q does not preserve the write failure", err.Error())
		}
	})
}

// TestRun_Wait_UnawaitedReport_NotWrittenWithoutWait proves the report is a property of
// the wait: without --wait no StorageClass is read for it and nothing is written.
func TestRun_Wait_UnawaitedReport_NotWrittenWithoutWait(t *testing.T) {
	t.Parallel()

	src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-1", "Pending", "wffc-sc"))}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		storageClassObj("wffc-sc", volumeBindingModeWFC, false),
		restoredPVCObject("pvc-1", pvcPhasePending, "wffc-sc", false),
	)

	var out bytes.Buffer

	cfg := baseConfig(src, dyn)
	cfg.Out = &out

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run without --wait: %v", err)
	}

	if out.Len() != 0 {
		t.Errorf("run without --wait wrote %q, want no output", out.String())
	}

	if got := countStorageClassCalls(dyn); got != 0 {
		t.Errorf("run without --wait made %d StorageClass calls, want none", got)
	}
}

// TestRun_Wait_UnawaitedReport_NoPVCsWritesNothing covers a restore whose manifest set
// holds no PersistentVolumeClaim at all.
func TestRun_Wait_UnawaitedReport_NoPVCsWritesNothing(t *testing.T) {
	t.Parallel()

	src := &stubSource{body: mustArray(t, configMapManifest("cm-1"))}
	dyn := newFakeDynamic(readySnapshot())

	var out bytes.Buffer

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Second
	cfg.Out = &out

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with --wait and no PVCs: %v", err)
	}

	if out.Len() != 0 {
		t.Errorf("restore without PVCs wrote %q, want no output", out.String())
	}
}

// TestRun_Wait_UnawaitedReport_SingleDormantWFFC is the reported case: one dormant WFFC
// claim is named once, the restore succeeds, and that claim is never polled.
func TestRun_Wait_UnawaitedReport_SingleDormantWFFC(t *testing.T) {
	t.Parallel()

	src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-1", "Pending", "wffc-sc"))}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		storageClassObj("wffc-sc", volumeBindingModeWFC, false),
		restoredPVCObject("pvc-1", pvcPhasePending, "wffc-sc", false),
	)

	var out bytes.Buffer

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Second
	cfg.Out = &out

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with one dormant WFFC PVC: %v", err)
	}

	report := out.String()

	if got := strings.Count(report, unawaitedReportHeader); got != 1 {
		t.Fatalf("report header appears %d times, want exactly one block: %q", got, report)
	}

	for _, want := range []string{
		testNS + "/pvc-1",
		`StorageClass "wffc-sc"`,
		volumeBindingModeWFC,
	} {
		if !strings.Contains(report, want) {
			t.Errorf("report %q does not contain %q", report, want)
		}
	}

	// Two preflight reads plus the one classification read: a polled claim would add more.
	if got := countPVCGets(dyn, "pvc-1"); got != 3 {
		t.Errorf("PVC GET count = %d, want two preflights and one classification read", got)
	}
}

// TestRun_Wait_UnawaitedReport_NamesTheResolvedDefaultStorageClass covers a claim with no
// spec.storageClassName: the report must name the default StorageClass the wait actually
// read, because that object is what the caller has to inspect.
func TestRun_Wait_UnawaitedReport_NamesTheResolvedDefaultStorageClass(t *testing.T) {
	t.Parallel()

	src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-1", "Pending", ""))}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		storageClassObj("cluster-default-sc", volumeBindingModeWFC, true),
		storageClassObj("other-sc", volumeBindingModeImmediate, false),
		restoredPVCObject("pvc-1", pvcPhasePending, "", false),
	)

	var out bytes.Buffer

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Second
	cfg.Out = &out

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with an empty storageClassName on a WFFC default: %v", err)
	}

	report := out.String()

	if !strings.Contains(report, `StorageClass "cluster-default-sc"`) {
		t.Errorf("report %q does not name the resolved default StorageClass", report)
	}

	if strings.Contains(report, `StorageClass ""`) {
		t.Errorf("report %q leaves the StorageClass name empty", report)
	}
}

// TestRun_Wait_UnawaitedReport_WriteFailureFailsTheRestore pins that the list is a
// required part of the result: if it cannot be written, the command reports that instead
// of exiting successfully with incomplete output.
func TestRun_Wait_UnawaitedReport_WriteFailureFailsTheRestore(t *testing.T) {
	t.Parallel()

	src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-1", "Pending", "wffc-sc"))}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		storageClassObj("wffc-sc", volumeBindingModeWFC, false),
		restoredPVCObject("pvc-1", pvcPhasePending, "wffc-sc", false),
	)

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Second
	cfg.Out = failingWriter{}

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected an unwritable result to fail the restore")
	}

	for _, want := range []string{"restored objects were applied", "broken pipe"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err.Error(), want)
		}
	}
}

// TestRun_Wait_UnawaitedReport_MultipleClaimsOneBlock verifies several dormant WFFC
// claims share one sorted block and do not re-read their common StorageClass.
func TestRun_Wait_UnawaitedReport_MultipleClaimsOneBlock(t *testing.T) {
	t.Parallel()

	src := &stubSource{body: mustArray(t,
		pvcManifestSC("pvc-c", "Pending", "wffc-sc"),
		pvcManifestSC("pvc-a", "Pending", "wffc-sc"),
		pvcManifestSC("pvc-b", "Pending", "wffc-sc"),
	)}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		storageClassObj("wffc-sc", volumeBindingModeWFC, false),
		restoredPVCObject("pvc-a", pvcPhasePending, "wffc-sc", false),
		restoredPVCObject("pvc-b", pvcPhasePending, "wffc-sc", false),
		restoredPVCObject("pvc-c", pvcPhasePending, "wffc-sc", false),
	)

	var out bytes.Buffer

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Second
	cfg.Out = &out

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with three dormant WFFC PVCs: %v", err)
	}

	report := out.String()

	if got := strings.Count(report, unawaitedReportHeader); got != 1 {
		t.Fatalf("report header appears %d times, want one block for all claims: %q", got, report)
	}

	assertOrderedIdentities(t, report, []string{
		testNS + "/pvc-a",
		testNS + "/pvc-b",
		testNS + "/pvc-c",
	})

	if got := countStorageClassCalls(dyn); got != 1 {
		t.Errorf("StorageClass calls = %d, want one cached lookup for the shared class", got)
	}

	for _, name := range []string{"pvc-a", "pvc-b", "pvc-c"} {
		if got := countPVCGets(dyn, name); got != 3 {
			t.Errorf("PVC %s GET count = %d, want no polling round", name, got)
		}
	}
}

// TestRun_Wait_UnawaitedReport_MixedSetListsOnlyUnawaited keeps the existing wait
// semantics for an Immediate claim while naming only the dormant WFFC one.
func TestRun_Wait_UnawaitedReport_MixedSetListsOnlyUnawaited(t *testing.T) {
	t.Parallel()

	src := &stubSource{body: mustArray(t,
		pvcManifestSC("pvc-wffc", "Pending", "wffc-sc"),
		pvcManifestSC("pvc-immediate", "", "immediate-sc"),
	)}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		storageClassObj("wffc-sc", volumeBindingModeWFC, false),
		storageClassObj("immediate-sc", volumeBindingModeImmediate, false),
		restoredPVCObject("pvc-wffc", pvcPhasePending, "wffc-sc", false),
		restoredPVCObject("pvc-immediate", pvcPhasePending, "immediate-sc", false),
		boundPVObject("pvc-immediate"),
	)

	immediateGets := 0
	dyn.PrependReactor("get", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(clienttesting.GetAction)
		if !ok || getAction.GetName() != "pvc-immediate" {
			return false, nil, nil
		}

		immediateGets++
		if immediateGets <= 2 {
			return false, nil, nil
		}

		return true, restoredPVCObject("pvc-immediate", pvcPhaseBound, "immediate-sc", false), nil
	})

	var out bytes.Buffer

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Second
	cfg.Out = &out

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with a mixed WFFC/Immediate set: %v", err)
	}

	report := out.String()

	if !strings.Contains(report, testNS+"/pvc-wffc") {
		t.Errorf("report %q does not name the unawaited WFFC claim", report)
	}

	if strings.Contains(report, "pvc-immediate") {
		t.Errorf("report %q names the Immediate claim, which is still awaited", report)
	}

	if immediateGets <= 2 {
		t.Errorf("Immediate PVC GET count = %d, want it to be awaited past the preflights", immediateGets)
	}
}

// TestRun_Wait_UnawaitedReport_ActiveWFFCIsStillAwaited pins that the classification is
// unchanged: a WFFC claim with a selected node is polled as before and is not listed.
func TestRun_Wait_UnawaitedReport_ActiveWFFCIsStillAwaited(t *testing.T) {
	t.Parallel()

	pvc := restoredPVCObject("pvc-active", pvcPhasePending, "wffc-sc", false)
	pvc.SetAnnotations(map[string]string{selectedNodeAnnotation: "node-a"})

	src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-active", "", "wffc-sc"))}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		storageClassObj("wffc-sc", volumeBindingModeWFC, false),
		pvc,
		boundPVObject("pvc-active"),
	)

	pvcGets := 0
	dyn.PrependReactor("get", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		pvcGets++
		if pvcGets <= 3 {
			return false, nil, nil
		}

		return true, restoredPVCObject("pvc-active", pvcPhaseBound, "wffc-sc", false), nil
	})

	var out bytes.Buffer

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Second
	cfg.Out = &out

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with an active WFFC PVC: %v", err)
	}

	if out.Len() != 0 {
		t.Errorf("an awaited WFFC claim was reported as unawaited: %q", out.String())
	}

	if pvcGets != 4 {
		t.Errorf("PVC GET count = %d, want two preflights, inspection, and Bound poll", pvcGets)
	}
}

// TestRun_Wait_UnawaitedReport_WrittenBeforeAWaitFailure proves the block survives a
// later failure of an awaited claim: it is written before the wait, and the error is
// returned unchanged.
func TestRun_Wait_UnawaitedReport_WrittenBeforeAWaitFailure(t *testing.T) {
	t.Parallel()

	src := &stubSource{body: mustArray(t,
		pvcManifestSC("pvc-wffc", "Pending", "wffc-sc"),
		pvcManifestSC("pvc-immediate", "Pending", "immediate-sc"),
	)}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		storageClassObj("wffc-sc", volumeBindingModeWFC, false),
		storageClassObj("immediate-sc", volumeBindingModeImmediate, false),
		restoredPVCObject("pvc-wffc", pvcPhasePending, "wffc-sc", false),
		restoredPVCObject("pvc-immediate", pvcPhasePending, "immediate-sc", false),
	)

	var out bytes.Buffer

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = 20 * time.Millisecond
	cfg.Out = &out

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected the never-binding Immediate PVC to time out")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error %q does not wrap context.DeadlineExceeded", err.Error())
	}

	if !strings.Contains(out.String(), testNS+"/pvc-wffc") {
		t.Errorf("report %q was not written before the wait failure", out.String())
	}
}

// TestRun_Wait_TimeoutNamesTheClaimAndAppliedObjects checks the timeout states the claim,
// its last observed phase, and that the apply already happened -- and claims no cause.
func TestRun_Wait_TimeoutNamesTheClaimAndAppliedObjects(t *testing.T) {
	t.Parallel()

	src := &stubSource{body: mustArray(t, pvcManifest("pvc-1", "Pending"))}
	dyn := newFakeDynamic(
		readySnapshot(),
		readyVolumeSnapshot("vs-1"),
		restoredPVCObject("pvc-1", pvcPhasePending, "", false),
	)

	cfg := baseConfig(src, dyn)
	cfg.Wait = true
	cfg.Timeout = time.Millisecond

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected a wait timeout, got nil")
	}

	for _, want := range []string{
		testNS + "/pvc-1",
		"Bound",
		`status.phase was "Pending"`,
		"already applied",
		"not rolled back",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("timeout error %q does not contain %q", err.Error(), want)
		}
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error %q does not wrap context.DeadlineExceeded", err.Error())
	}
}

// TestRun_Wait_UnawaitedReport_WriterIsInjectedAndDeterministic proves the report goes
// only to the injected writer and that the restore result does not depend on it: the same
// run with a discarding default (no terminal, no stdin) succeeds identically.
func TestRun_Wait_UnawaitedReport_WriterIsInjectedAndDeterministic(t *testing.T) {
	t.Parallel()

	run := func(t *testing.T, out *bytes.Buffer) {
		t.Helper()

		src := &stubSource{body: mustArray(t, pvcManifestSC("pvc-1", "Pending", "wffc-sc"))}
		dyn := newFakeDynamic(
			readySnapshot(),
			readyVolumeSnapshot("vs-1"),
			storageClassObj("wffc-sc", volumeBindingModeWFC, false),
			restoredPVCObject("pvc-1", pvcPhasePending, "wffc-sc", false),
		)

		cfg := baseConfig(src, dyn)
		cfg.Wait = true
		cfg.Timeout = time.Second

		if out != nil {
			cfg.Out = out
		}

		if err := Run(context.Background(), cfg); err != nil {
			t.Fatalf("Run with a dormant WFFC PVC: %v", err)
		}

		if got := countPVCGets(dyn, "pvc-1"); got != 3 {
			t.Errorf("PVC GET count = %d, want the same classification regardless of the writer", got)
		}
	}

	var captured bytes.Buffer

	run(t, &captured)

	if !strings.Contains(captured.String(), unawaitedReportHeader) {
		t.Errorf("injected writer received %q, want the report block", captured.String())
	}

	// No writer configured: the default discards instead of reaching the process stdout,
	// and the restore behaves identically.
	run(t, nil)
}

func assertOrderedIdentities(t *testing.T, report string, identities []string) {
	t.Helper()

	previous := -1

	for _, identity := range identities {
		index := strings.Index(report, identity)
		if index < 0 {
			t.Fatalf("report %q does not contain %q", report, identity)
		}

		if index < previous {
			t.Errorf("report %q does not list %q in namespace/name order", report, identity)
		}

		previous = index
	}
}

func countPVCGets(dyn *dynamicfake.FakeDynamicClient, name string) int {
	count := 0

	for _, action := range dyn.Actions() {
		if action.GetVerb() != "get" || action.GetResource() != pvcGVR {
			continue
		}

		if getAction, ok := action.(clienttesting.GetAction); ok && getAction.GetName() == name {
			count++
		}
	}

	return count
}

func countStorageClassCalls(dyn *dynamicfake.FakeDynamicClient) int {
	count := 0

	for _, action := range dyn.Actions() {
		if action.GetResource() == scGVR {
			count++
		}
	}

	return count
}

// failingWriter stands in for a closed result stream, such as a piped stdout whose reader
// exited.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("broken pipe")
}
