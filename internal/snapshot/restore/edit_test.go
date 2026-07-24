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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// fakeEditorScript writes a small shell script into dir that, when invoked with a
// file path as its argument, applies the given transformation to that file.
// For non-POSIX systems the test is skipped because os/exec of a shell script
// requires a POSIX shell.
func fakeEditorScript(t *testing.T, dir, script string) string {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("fake editor requires a POSIX shell; skipping on Windows")
	}

	path := filepath.Join(dir, "fake-editor.sh")
	content := "#!/bin/sh\n" + script + "\n"

	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write fake editor: %v", err)
	}

	return path
}

// writeEditorContent writes data to a file so a fake editor script can cp it over
// the temp file. This avoids inline printf/heredoc portability issues.
func writeEditorContent(t *testing.T, dir, name, data string) string {
	t.Helper()

	p := filepath.Join(dir, name)

	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatalf("write editor content file %s: %v", name, err)
	}

	return p
}

// TestEditManifests_MutatedYAMLApplied verifies that when the fake editor writes
// modified content back to the temp file, editManifests returns the edited objects.
func TestEditManifests_MutatedYAMLApplied(t *testing.T) {
	dir := t.TempDir()

	// Two-document YAML: original + an extra ConfigMap the editor appends.
	editedContent := `apiVersion: v1
kind: ConfigMap
metadata:
  name: original-cm
data:
  k: v
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: edited-cm
data:
  injected: "yes"
`
	contentFile := writeEditorContent(t, dir, "edited.yaml", editedContent)
	editor := fakeEditorScript(t, dir, fmt.Sprintf(`cp '%s' "$1"`, contentFile))

	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	input := []unstructured.Unstructured{
		{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "original-cm"},
			"data":       map[string]interface{}{"k": "v"},
		}},
	}

	result, err := editManifests(input)
	if err != nil {
		t.Fatalf("editManifests: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 objects after edit, got %d", len(result))
	}

	if result[0].GetName() != "original-cm" {
		t.Errorf("first object name: got %q, want %q", result[0].GetName(), "original-cm")
	}

	if result[1].GetName() != "edited-cm" {
		t.Errorf("second object name: got %q, want %q", result[1].GetName(), "edited-cm")
	}

	val, found, _ := unstructured.NestedString(result[1].Object, "data", "injected")
	if !found || val != "yes" {
		t.Errorf("edited-cm data.injected: found=%v val=%q, want found=true val=%q", found, val, "yes")
	}
}

// TestEditManifests_NonZeroExitAborts verifies that when the fake editor exits
// non-zero, editManifests returns an error and does not parse any content.
func TestEditManifests_NonZeroExitAborts(t *testing.T) {
	dir := t.TempDir()

	// Editor exits 1 without modifying the file.
	editor := fakeEditorScript(t, dir, "exit 1")

	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	input := []unstructured.Unstructured{
		{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "cm-1"},
		}},
	}

	_, err := editManifests(input)
	if err == nil {
		t.Fatal("expected error from non-zero editor exit, got nil")
	}

	if !contains(err.Error(), "editor exited") {
		t.Errorf("error %q does not mention 'editor exited'", err.Error())
	}
}

// TestEditManifests_UnchangedContentAborts verifies that when the editor writes
// back the same bytes as the original, editManifests returns an "unchanged" error.
func TestEditManifests_UnchangedContentAborts(t *testing.T) {
	dir := t.TempDir()

	// Editor preserves the file unchanged (cp to a temp backup and back).
	editor := fakeEditorScript(t, dir, `cp "$1" "${1}.bak" && mv "${1}.bak" "$1"`)

	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	input := []unstructured.Unstructured{
		{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "cm-1"},
		}},
	}

	_, err := editManifests(input)
	if err == nil {
		t.Fatal("expected error for unchanged content, got nil")
	}

	if !contains(err.Error(), "unchanged") {
		t.Errorf("error %q does not mention 'unchanged'", err.Error())
	}
}

// TestEditManifests_EmptyContentAborts verifies that when the editor truncates the
// file to empty, editManifests returns an "empty" error.
func TestEditManifests_EmptyContentAborts(t *testing.T) {
	dir := t.TempDir()

	// Editor truncates the file to empty.
	editor := fakeEditorScript(t, dir, `> "$1"`)

	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	input := []unstructured.Unstructured{
		{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "cm-1"},
		}},
	}

	_, err := editManifests(input)
	if err == nil {
		t.Fatal("expected error for empty content, got nil")
	}

	if !contains(err.Error(), "empty") {
		t.Errorf("error %q does not mention 'empty'", err.Error())
	}
}

// TestEditManifests_KubeEditorTakesPrecedence verifies that $KUBE_EDITOR is
// preferred over $EDITOR when both are set.
func TestEditManifests_KubeEditorTakesPrecedence(t *testing.T) {
	dir := t.TempDir()
	fallbackDir := t.TempDir()

	// KUBE_EDITOR writes a distinctly named ConfigMap.
	kubeContent := writeEditorContent(t, dir, "kube.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: from-kube-editor\n")
	kubeEditor := fakeEditorScript(t, dir, fmt.Sprintf(`cp '%s' "$1"`, kubeContent))

	// EDITOR writes a different name; if KUBE_EDITOR is preferred this must never run.
	fallbackContent := writeEditorContent(t, fallbackDir, "fallback.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: from-editor\n")
	fallbackEditor := fakeEditorScript(t, fallbackDir, fmt.Sprintf(`cp '%s' "$1"`, fallbackContent))

	t.Setenv("KUBE_EDITOR", kubeEditor)
	t.Setenv("EDITOR", fallbackEditor)

	input := []unstructured.Unstructured{
		{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "original"},
		}},
	}

	result, err := editManifests(input)
	if err != nil {
		t.Fatalf("editManifests: %v", err)
	}

	if len(result) == 0 {
		t.Fatal("expected at least one object")
	}

	if result[0].GetName() != "from-kube-editor" {
		t.Errorf("KUBE_EDITOR did not take precedence: got name %q, want %q", result[0].GetName(), "from-kube-editor")
	}
}

// TestEditManifests_WhitespaceOnlyAborts verifies that an editor writing only
// whitespace is treated as empty content and aborts.
func TestEditManifests_WhitespaceOnlyAborts(t *testing.T) {
	dir := t.TempDir()

	// Editor writes only whitespace (three spaces and newlines).
	wsContent := writeEditorContent(t, dir, "ws.txt", "   \n\n  \n")
	editor := fakeEditorScript(t, dir, fmt.Sprintf(`cp '%s' "$1"`, wsContent))

	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	input := []unstructured.Unstructured{
		{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "cm-1"},
		}},
	}

	_, err := editManifests(input)
	if err == nil {
		t.Fatal("expected error for whitespace-only content, got nil")
	}

	if !contains(err.Error(), "empty") {
		t.Errorf("error %q does not mention 'empty'", err.Error())
	}
}

// TestRun_Edit_MutatedManifestApplied exercises the full Run flow with Edit=true.
// The fake editor renames the ConfigMap from "original-cm" to "edited-cm"; the test
// asserts that "edited-cm" is applied and "original-cm" is absent.
func TestRun_Edit_MutatedManifestApplied(t *testing.T) {
	dir := t.TempDir()

	editedYAML := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: edited-cm\ndata:\n  k: edited\n"
	contentFile := writeEditorContent(t, dir, "renamed.yaml", editedYAML)
	editor := fakeEditorScript(t, dir, fmt.Sprintf(`cp '%s' "$1"`, contentFile))

	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	src := &stubSource{body: mustArray(t, configMapManifest("original-cm"))}
	dyn := newFakeDynamic(readySnapshot())

	cfg := baseConfig(src, dyn)
	cfg.Edit = true

	if err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run with Edit=true: %v", err)
	}

	// "edited-cm" must have been applied.
	cm, err := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "edited-cm", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("edited-cm not applied: %v", err)
	}

	val, _, _ := unstructured.NestedString(cm.Object, "data", "k")
	if val != "edited" {
		t.Errorf("edited-cm data.k: got %q, want %q", val, "edited")
	}

	// "original-cm" must NOT have been applied.
	_, origErr := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "original-cm", metav1.GetOptions{})
	if origErr == nil {
		t.Error("original-cm should not exist after rename edit, but it was found")
	}
}

func TestRun_Edit_CrossNamespaceMutationAbortsBeforePatch(t *testing.T) {
	dir := t.TempDir()

	editedYAML := `apiVersion: v1
kind: ConfigMap
metadata:
  name: valid
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: foreign
  namespace: other
`
	contentFile := writeEditorContent(t, dir, "cross-namespace.yaml", editedYAML)
	editor := fakeEditorScript(t, dir, fmt.Sprintf(`cp '%s' "$1"`, contentFile))

	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	src := &stubSource{body: mustArray(t, configMapManifest("original"))}
	dyn := newFakeDynamic(readySnapshot())
	cfg := baseConfig(src, dyn)
	cfg.Edit = true

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected cross-namespace edit error, got nil")
	}

	for _, value := range []string{
		`apiVersion="v1"`,
		`kind="ConfigMap"`,
		`name="foreign"`,
		`namespace "other"`,
		`required namespace is "default"`,
	} {
		if !contains(err.Error(), value) {
			t.Errorf("error %q does not contain %q", err.Error(), value)
		}
	}

	assertNoPatchActions(t, dyn)
}

// TestRun_Edit_NonZeroEditorAborts verifies that when the editor exits non-zero during
// a full Run with Edit=true, no object is applied.
func TestRun_Edit_NonZeroEditorAborts(t *testing.T) {
	dir := t.TempDir()

	editor := fakeEditorScript(t, dir, "exit 1")

	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	src := &stubSource{body: mustArray(t, configMapManifest("cm-abort"))}
	dyn := newFakeDynamic(readySnapshot())

	cfg := baseConfig(src, dyn)
	cfg.Edit = true

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error from non-zero editor exit in Run, got nil")
	}

	if !contains(err.Error(), "restore edit") {
		t.Errorf("error %q does not mention 'restore edit'", err.Error())
	}

	// Nothing must be applied.
	_, getErr := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "cm-abort", metav1.GetOptions{})
	if getErr == nil {
		t.Error("cm-abort should not be applied when editor aborts, but it was found")
	}
}

// TestRun_Edit_UnchangedAborts verifies that unchanged content from the editor aborts
// the restore and applies nothing.
func TestRun_Edit_UnchangedAborts(t *testing.T) {
	dir := t.TempDir()

	// Editor preserves the file unchanged.
	editor := fakeEditorScript(t, dir, `cp "$1" "${1}.bak" && mv "${1}.bak" "$1"`)

	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	src := &stubSource{body: mustArray(t, configMapManifest("cm-unchanged"))}
	dyn := newFakeDynamic(readySnapshot())

	cfg := baseConfig(src, dyn)
	cfg.Edit = true

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for unchanged content, got nil")
	}

	if !contains(err.Error(), "unchanged") {
		t.Errorf("error %q does not mention 'unchanged'", err.Error())
	}

	_, getErr := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "cm-unchanged", metav1.GetOptions{})
	if getErr == nil {
		t.Error("cm-unchanged should not be applied on unchanged-abort, but it was found")
	}
}

// TestRun_Edit_EmptyContentAborts verifies that an editor that empties the file aborts
// the restore without applying anything.
func TestRun_Edit_EmptyContentAborts(t *testing.T) {
	dir := t.TempDir()

	editor := fakeEditorScript(t, dir, `> "$1"`)

	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	src := &stubSource{body: mustArray(t, configMapManifest("cm-empty"))}
	dyn := newFakeDynamic(readySnapshot())

	cfg := baseConfig(src, dyn)
	cfg.Edit = true

	err := Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for empty content, got nil")
	}

	if !contains(err.Error(), "empty") {
		t.Errorf("error %q does not mention 'empty'", err.Error())
	}

	_, getErr := dyn.Resource(cmGVR).Namespace(testNS).Get(context.Background(), "cm-empty", metav1.GetOptions{})
	if getErr == nil {
		t.Error("cm-empty should not be applied on empty-abort, but it was found")
	}
}

// TestEditManifests_WhitespaceEditorEnvReturnsError verifies that setting $EDITOR to a
// whitespace-only string (e.g. " ") returns an error and does not panic.
// Before the fix, strings.Fields(" ") returns an empty slice and fields[0] panics.
func TestEditManifests_WhitespaceEditorEnvReturnsError(t *testing.T) {
	t.Setenv("EDITOR", " ")
	t.Setenv("KUBE_EDITOR", "")

	input := []unstructured.Unstructured{
		{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "cm-ws-editor"},
		}},
	}

	_, err := editManifests(input)
	if err == nil {
		t.Fatal("expected error for whitespace-only EDITOR env, got nil")
	}
}

// TestMarshalDecodeRoundTrip verifies that marshalMultiDocYAML and decodeMultiDocYAML
// are inverses of each other for a multi-document input.
func TestMarshalDecodeRoundTrip(t *testing.T) {
	input := []unstructured.Unstructured{
		{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "cm-1"},
			"data":       map[string]interface{}{"a": "1"},
		}},
		{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "cm-2"},
			"data":       map[string]interface{}{"b": "2"},
		}},
	}

	data, err := marshalMultiDocYAML(input)
	if err != nil {
		t.Fatalf("marshalMultiDocYAML: %v", err)
	}

	result, err := decodeMultiDocYAML(data)
	if err != nil {
		t.Fatalf("decodeMultiDocYAML: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("got %d objects, want 2", len(result))
	}

	for i, want := range []string{"cm-1", "cm-2"} {
		if result[i].GetName() != want {
			t.Errorf("object[%d].name = %q, want %q", i, result[i].GetName(), want)
		}
	}
}

// TestDecodeMultiDocYAML_EmptyDocsSkipped verifies that blank documents in a YAML
// stream are silently skipped and do not produce empty unstructured objects.
func TestDecodeMultiDocYAML_EmptyDocsSkipped(t *testing.T) {
	yaml := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm-1\n---\n   \n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm-2\n")

	result, err := decodeMultiDocYAML(yaml)
	if err != nil {
		t.Fatalf("decodeMultiDocYAML: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("got %d objects (want 2); empty doc must be skipped", len(result))
	}
}

// TestSplitYAMLDocs verifies the --- separator splitting logic for edge cases.
func TestSplitYAMLDocs(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		wantDocs int
	}{
		{
			name:     "single doc no separator",
			input:    "apiVersion: v1\nkind: ConfigMap\n",
			wantDocs: 1,
		},
		{
			name:     "two docs with separator",
			input:    "apiVersion: v1\nkind: ConfigMap\n---\napiVersion: v1\nkind: Secret\n",
			wantDocs: 2,
		},
		{
			name:     "leading separator",
			input:    "---\napiVersion: v1\nkind: ConfigMap\n",
			wantDocs: 2,
		},
		{
			name:     "empty input",
			input:    "",
			wantDocs: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			docs := splitYAMLDocs([]byte(tc.input))

			if len(docs) != tc.wantDocs {
				t.Errorf("splitYAMLDocs(%q) = %d docs, want %d", tc.input, len(docs), tc.wantDocs)
			}
		})
	}
}

// TestResolveEditor verifies editor resolution priority:
// KUBE_EDITOR > EDITOR > "vi".
func TestResolveEditor(t *testing.T) {
	cases := []struct {
		name       string
		kubeEditor string
		editor     string
		want       string
	}{
		{
			name:       "KUBE_EDITOR set",
			kubeEditor: "kube-ed",
			editor:     "ed",
			want:       "kube-ed",
		},
		{
			name:       "only EDITOR set",
			kubeEditor: "",
			editor:     "my-editor",
			want:       "my-editor",
		},
		{
			name:       "neither set falls back to vi",
			kubeEditor: "",
			editor:     "",
			want:       "vi",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KUBE_EDITOR", tc.kubeEditor)
			t.Setenv("EDITOR", tc.editor)

			got := resolveEditor()
			if got != tc.want {
				t.Errorf("resolveEditor() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestEditManifests_MultipleObjectsEdited verifies that a multi-document YAML input
// round-trips through the editor and both objects are present after a single-field edit.
func TestEditManifests_MultipleObjectsEdited(t *testing.T) {
	dir := t.TempDir()

	// Editor uses sed to change the data field value in the first ConfigMap only.
	// sed is portable POSIX; the file is modified in-place via a temp file.
	editor := fakeEditorScript(t, dir,
		fmt.Sprintf(`sed 's/k: v/k: changed/' "$1" > '%s/out.yaml' && mv '%s/out.yaml' "$1"`, dir, dir),
	)

	t.Setenv("EDITOR", editor)
	t.Setenv("KUBE_EDITOR", "")

	input := []unstructured.Unstructured{
		{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "cm-1"},
			"data":       map[string]interface{}{"k": "v"},
		}},
		{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]interface{}{"name": "cm-2"},
			"data":       map[string]interface{}{"x": "y"},
		}},
	}

	result, err := editManifests(input)
	if err != nil {
		t.Fatalf("editManifests: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(result))
	}

	val, _, _ := unstructured.NestedString(result[0].Object, "data", "k")
	if val != "changed" {
		t.Errorf("cm-1 data.k: got %q, want %q", val, "changed")
	}

	if result[1].GetName() != "cm-2" {
		t.Errorf("cm-2 name not preserved: got %q", result[1].GetName())
	}
}
