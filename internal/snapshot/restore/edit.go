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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
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
	return editManifestsContext(context.Background(), objs)
}

func editManifestsContext(
	ctx context.Context,
	objs []unstructured.Unstructured,
) ([]unstructured.Unstructured, error) {
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

	if err = runEditor(ctx, tmpPath); err != nil {
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
// objects. Document splitting is delegated to utilyaml.YAMLReader — the same
// reader kubectl apply -f uses — instead of a literal "\n---\n" byte split, so
// editor output that doesn't match that exact byte sequence (CRLF line
// endings, a trailing space after "---", or a "#" comment on the separator
// line) still splits correctly instead of being decoded as a single document.
// The trade-off: a line starting with "---" at column zero that continues
// with something other than whitespace or a "#" comment (e.g. "----" or
// "---foo") is now a syntax error rather than silently staying part of the
// document — matching kubectl apply -f's behavior. Empty documents, and
// documents that decode to an empty map (e.g. a fully commented-out object),
// are skipped without error so commenting out an object is a valid way to
// remove it during editing.
func decodeMultiDocYAML(data []byte) ([]unstructured.Unstructured, error) {
	reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	result := make([]unstructured.Unstructured, 0, bytes.Count(data, []byte("\n---"))+1)

	for i := 0; ; i++ {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("split YAML document %d: %w", i, err)
		}

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

// objectIdentity formats obj's apiVersion, kind, and name for diagnostic
// messages, matching the format restore.go already uses for its cluster-scope
// and duplicate-object errors so log output and error text stay consistent.
func objectIdentity(obj unstructured.Unstructured) string {
	return fmt.Sprintf("apiVersion=%q kind=%q name=%q", obj.GetAPIVersion(), obj.GetKind(), obj.GetName())
}

// diffObjectIdentities compares before and after as multisets of object
// identities (apiVersion+kind+name) and returns the identities that were
// removed and added between them. An identity repeated more times in one
// slice than the other is reported once per unmatched occurrence, since a
// restore set can legitimately contain duplicate identities. removed follows
// the order identities first become unmatched while scanning before; added
// follows the same while scanning after. Neither input slice is mutated.
func diffObjectIdentities(before, after []unstructured.Unstructured) ([]string, []string) {
	afterCounts := make(map[string]int, len(after))
	for i := range after {
		afterCounts[objectIdentity(after[i])]++
	}

	beforeCounts := make(map[string]int, len(before))
	for i := range before {
		beforeCounts[objectIdentity(before[i])]++
	}

	removed := make([]string, 0, len(before))

	for i := range before {
		identity := objectIdentity(before[i])

		if afterCounts[identity] > 0 {
			afterCounts[identity]--

			continue
		}

		removed = append(removed, identity)
	}

	added := make([]string, 0, len(after))

	for i := range after {
		identity := objectIdentity(after[i])

		if beforeCounts[identity] > 0 {
			beforeCounts[identity]--

			continue
		}

		added = append(added, identity)
	}

	return removed, added
}

// runEditor opens path in the user's preferred editor and blocks until the
// editor exits. Returns an error if the editor exits non-zero or ctx is canceled.
// Editor selection: $KUBE_EDITOR → $EDITOR → "vi".
// Simple field-splitting is used to support editors with flags (e.g. "code --wait").
func runEditor(ctx context.Context, path string) error {
	editor := resolveEditor()
	fields := strings.Fields(editor)

	if len(fields) == 0 {
		return fmt.Errorf("resolved editor command %q is empty: set $KUBE_EDITOR or $EDITOR to a non-blank value", editor)
	}

	cmdArgs := make([]string, 0, len(fields))
	cmdArgs = append(cmdArgs, fields[1:]...)
	cmdArgs = append(cmdArgs, path)

	if err := context.Cause(ctx); err != nil {
		return fmt.Errorf("editor canceled before startup: %w", err)
	}

	cmd := exec.CommandContext(ctx, fields[0], cmdArgs...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return fmt.Errorf("editor canceled: %w", cause)
		}

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
