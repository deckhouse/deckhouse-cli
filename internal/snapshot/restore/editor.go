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
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// OpenEditorYAML writes data to a temporary YAML file, opens $KUBE_EDITOR or
// $EDITOR (fallback: vi), waits for the user to save and exit, then returns
// the (possibly modified) file content.
func OpenEditorYAML(data []byte) ([]byte, error) {
	tmpFile, err := os.CreateTemp("", "d8-snap-restore-*.yaml")
	if err != nil {
		return nil, fmt.Errorf("create temp file for editor: %w", err)
	}

	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return nil, fmt.Errorf("write temp file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("close temp file: %w", err)
	}

	cmdArgs := resolveEditor()
	cmd := exec.Command(cmdArgs[0], append(cmdArgs[1:], tmpPath)...) //nolint:gosec
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("editor exited with error: %w", err)
	}

	result, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("read edited file: %w", err)
	}

	return result, nil
}

// resolveEditor returns the editor command and arguments.
// It checks KUBE_EDITOR, then EDITOR, then falls back to "vi".
func resolveEditor() []string {
	for _, env := range []string{"KUBE_EDITOR", "EDITOR"} {
		if v := os.Getenv(env); v != "" {
			parts := strings.Fields(v)
			if len(parts) > 0 {
				return parts
			}
		}
	}

	return []string{"vi"}
}
