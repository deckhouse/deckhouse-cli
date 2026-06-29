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
	"fmt"
	"os"
	"os/exec"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	sigsyaml "sigs.k8s.io/yaml"
)

// editManifests opens objs in the user's preferred editor (kubectl-style) and
// returns the parsed result of the editor session. The editor is chosen from
// $KUBE_EDITOR, $EDITOR, then "vi".
//
// It returns an error (and applies nothing) when:
//   - the editor process exits non-zero,
//   - the saved content is byte-for-byte identical to what was written, or
//   - the saved content is empty or whitespace-only.
func editManifests(objs []unstructured.Unstructured) ([]unstructured.Unstructured, error) {
	yamlData, err := marshalMultiDocYAML(objs)
	if err != nil {
		return nil, fmt.Errorf("serialize manifests for editing: %w", err)
	}

	tmp, err := os.CreateTemp("", "d8-restore-edit-*.yaml")
	if err != nil {
		return nil, fmt.Errorf("create temp file for editing: %w", err)
	}

	tmpPath := tmp.Name()

	defer func() { _ = os.Remove(tmpPath) }()

	if _, err = tmp.Write(yamlData); err != nil {
		_ = tmp.Close()

		return nil, fmt.Errorf("write manifests to temp file: %w", err)
	}

	if err = tmp.Close(); err != nil {
		return nil, fmt.Errorf("close temp file before editing: %w", err)
	}

	if err = runEditor(tmpPath); err != nil {
		return nil, err
	}

	edited, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("read edited file: %w", err)
	}

	if bytes.Equal(edited, yamlData) {
		return nil, fmt.Errorf("edit aborted: content is unchanged")
	}

	if len(strings.TrimSpace(string(edited))) == 0 {
		return nil, fmt.Errorf("edit aborted: content is empty")
	}

	result, err := decodeMultiDocYAML(edited)
	if err != nil {
		return nil, fmt.Errorf("decode edited manifests: %w", err)
	}

	return result, nil
}

// marshalMultiDocYAML serializes a slice of unstructured objects to a YAML
// multi-document stream. Documents are separated by "---" lines.
func marshalMultiDocYAML(objs []unstructured.Unstructured) ([]byte, error) {
	var buf bytes.Buffer

	for i, obj := range objs {
		if i > 0 {
			buf.WriteString("---\n")
		}

		data, err := sigsyaml.Marshal(obj.Object)
		if err != nil {
			return nil, fmt.Errorf("marshal object %d (%s/%s): %w", i, obj.GetKind(), obj.GetName(), err)
		}

		buf.Write(data)
	}

	return buf.Bytes(), nil
}

// decodeMultiDocYAML parses a YAML multi-document stream into unstructured
// objects. Empty documents are skipped.
func decodeMultiDocYAML(data []byte) ([]unstructured.Unstructured, error) {
	docs := splitYAMLDocs(data)
	result := make([]unstructured.Unstructured, 0, len(docs))

	for i, doc := range docs {
		if len(bytes.TrimSpace(doc)) == 0 {
			continue
		}

		var obj map[string]interface{}

		if err := sigsyaml.Unmarshal(doc, &obj); err != nil {
			return nil, fmt.Errorf("decode YAML document %d: %w", i, err)
		}

		if len(obj) == 0 {
			continue
		}

		result = append(result, unstructured.Unstructured{Object: obj})
	}

	return result, nil
}

// splitYAMLDocs splits a YAML multi-document byte stream on "---" separator
// lines. Each returned element contains the raw YAML bytes of one document.
func splitYAMLDocs(data []byte) [][]byte {
	if len(data) > 0 && data[len(data)-1] != '\n' {
		data = append(data, '\n')
	}

	// Prepend a sentinel newline so a leading "---\n" at offset 0 is matched by "\n---\n".
	normalized := append([]byte{'\n'}, data...)

	parts := bytes.Split(normalized, []byte("\n---\n"))

	docs := make([][]byte, 0, len(parts))

	for i, p := range parts {
		if i == 0 {
			// Remove the sentinel newline prepended above.
			p = bytes.TrimPrefix(p, []byte("\n"))
		}

		docs = append(docs, p)
	}

	return docs
}

// runEditor opens path in the user's preferred editor and blocks until the
// editor exits. Returns an error if the editor exits non-zero.
// Editor selection: $KUBE_EDITOR → $EDITOR → "vi".
// Simple field-splitting is used to support editors with flags (e.g. "code --wait").
func runEditor(path string) error {
	editor := resolveEditor()
	fields := strings.Fields(editor)

	if len(fields) == 0 {
		return fmt.Errorf("resolved editor command %q is empty: set $KUBE_EDITOR or $EDITOR to a non-blank value", editor)
	}

	cmdArgs := make([]string, 0, len(fields))
	cmdArgs = append(cmdArgs, fields[1:]...)
	cmdArgs = append(cmdArgs, path)

	cmd := exec.Command(fields[0], cmdArgs...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("editor exited with error: %w", err)
	}

	return nil
}

// resolveEditor returns the editor command from $KUBE_EDITOR, $EDITOR, or "vi".
func resolveEditor() string {
	if e := os.Getenv("KUBE_EDITOR"); e != "" {
		return e
	}

	if e := os.Getenv("EDITOR"); e != "" {
		return e
	}

	return "vi"
}
