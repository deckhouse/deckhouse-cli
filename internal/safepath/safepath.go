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

// Package safepath confines untrusted relative paths, such as archive entry names, to a root directory.
package safepath

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Join joins name under root and rejects a result that escapes root (CWE-22).
// The check is lexical: callers must not materialize symlinks from the same untrusted input.
func Join(root, name string) (string, error) {
	target := filepath.Join(root, name)

	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes the target directory", name)
	}

	return target, nil
}
