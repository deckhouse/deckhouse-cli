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

package push

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// workDirName is the directory push creates inside the temp dir and keeps all
// of its temporary files in. The temp dir itself may be any user directory
// (even "/"), so push never writes or removes anything outside workDirName.
const workDirName = ".d8-mirror-tmp"

// createWorkDir creates <parent>/.d8-mirror-tmp for the current run and returns
// its absolute path with a cleanup func that removes it.
//
// The directory is created with os.Mkdir, so a run owns it only if it created
// it. An existing one, left by an interrupted run, used by a concurrent one or
// unrelated to d8 at all, is reported and never reused or removed.
//
// When removeEmptyParent is set, cleanup also removes parent if nothing else is
// left in it. Use it only for a parent push picked itself, not for --tmp-dir.
func createWorkDir(parent string, removeEmptyParent bool) (string, func() error, error) {
	parent, err := filepath.Abs(parent)
	if err != nil {
		return "", nil, fmt.Errorf("resolve temp directory %s: %w", parent, err)
	}

	if err = os.MkdirAll(parent, 0o755); err != nil {
		return "", nil, fmt.Errorf("create temp directory %s: %w", parent, err)
	}

	dir := filepath.Join(parent, workDirName)

	if err = os.Mkdir(dir, 0o755); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return "", nil, workDirExistsError(dir)
		}

		return "", nil, fmt.Errorf("create temporary directory %s: %w", dir, err)
	}

	cleanup := func() error {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove temporary directory %s: %w", dir, err)
		}

		if removeEmptyParent {
			// os.Remove only deletes an empty directory, so anything else in
			// parent stays. A non-empty parent is not an error.
			_ = os.Remove(parent)
		}

		return nil
	}

	return dir, cleanup, nil
}

// workDirExistsError describes a work dir push found in place. It only reads
// the directory to tell an empty one from one with content.
func workDirExistsError(dir string) error {
	const hint = "it may be left by an interrupted push or used by a running one; " +
		"remove it manually or set --tmp-dir to another directory"

	// Lstat, not Stat: a symlink in place of the work dir is reported as is
	// and never followed.
	if info, err := os.Lstat(dir); err == nil && !info.IsDir() {
		return fmt.Errorf("temporary directory path %s already exists and is not a directory: %s", dir, hint)
	}

	if entries, err := os.ReadDir(dir); err == nil && len(entries) == 0 {
		return fmt.Errorf("temporary directory %s already exists: %s", dir, hint)
	}

	return fmt.Errorf("temporary directory %s already exists and is not empty: %s", dir, hint)
}
