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

package archive

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

type moveFileExFunc func(*uint16, *uint16, uint32) error

func renameDurably(oldPath, newPath string) error {
	return moveFileDurably(oldPath, newPath, windows.MoveFileEx)
}

func moveFileDurably(oldPath, newPath string, move moveFileExFunc) error {
	oldPathPtr, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return fmt.Errorf("encoding old path %s: %w", oldPath, err)
	}

	newPathPtr, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return fmt.Errorf("encoding new path %s: %w", newPath, err)
	}

	// MoveFileEx is also the primitive behind Go's os.Rename on Windows.
	// WRITE_THROUGH adds the documented guarantee that the move has reached
	// disk before success; it does not claim POSIX directory-fsync semantics.
	flags := uint32(windows.MOVEFILE_REPLACE_EXISTING | windows.MOVEFILE_WRITE_THROUGH)
	if err := move(oldPathPtr, newPathPtr, flags); err != nil {
		return fmt.Errorf("moving %s to %s with write-through: %w", oldPath, newPath, err)
	}

	return nil
}

// syncDir is deliberately a no-op on Windows. FlushFileBuffers requires a
// GENERIC_WRITE file handle and is not documented for directory handles.
// AtomicWriter instead publishes with MOVEFILE_WRITE_THROUGH, whose successful
// return guarantees that the move reached disk. Windows exposes no equivalent
// supported operation for separately confirming MkdirAll directory creation.
func syncDir(string) error {
	return nil
}

// ensureDirDurably deliberately provides only MkdirAll semantics on Windows.
// Windows exposes no documented unprivileged equivalent of syncing every
// containing directory entry, so success does not claim POSIX-style namespace
// durability.
func ensureDirDurably(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("creating dir %s: %w", path, err)
	}

	return nil
}
