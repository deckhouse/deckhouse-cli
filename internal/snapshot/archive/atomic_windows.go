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
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

type moveFileExFunc func(*uint16, *uint16, uint32) error
type pathExistsFunc func(string) (bool, error)

func renameDurably(oldPath, newPath string) error {
	return moveFileDurably(oldPath, newPath, pathExists, windows.MoveFileEx)
}

func moveFileDurably(oldPath, newPath string, exists pathExistsFunc, move moveFileExFunc) error {
	destinationExists, err := exists(newPath)
	if err != nil {
		return fmt.Errorf("checking replacement destination %s: %w", newPath, err)
	}

	if destinationExists {
		return fmt.Errorf("%w: destination %s already exists", ErrAtomicReplaceUnsupported, newPath)
	}

	oldPathPtr, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return fmt.Errorf("encoding old path %s: %w", oldPath, err)
	}

	newPathPtr, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return fmt.Errorf("encoding new path %s: %w", newPath, err)
	}

	// MoveFileEx documents WRITE_THROUGH durability but not replacement
	// atomicity. ReplaceFile is Microsoft's atomic single-document alternative,
	// but its WRITE_THROUGH flag is unsupported and documented failure states
	// can relocate or remove the old final. A create-only move avoids both
	// overclaims: no COPY_ALLOWED fallback and no replacement flag are used.
	flags := uint32(windows.MOVEFILE_WRITE_THROUGH)
	if err := move(oldPathPtr, newPathPtr, flags); err != nil {
		if errors.Is(err, windows.ERROR_ALREADY_EXISTS) || errors.Is(err, windows.ERROR_FILE_EXISTS) {
			return fmt.Errorf(
				"%w: destination %s won the publication race: %w",
				ErrAtomicReplaceUnsupported,
				newPath,
				err,
			)
		}

		return fmt.Errorf("moving %s to %s with write-through: %w", oldPath, newPath, err)
	}

	return nil
}

func pathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return false, err
}

// syncDir is deliberately a no-op on Windows. FlushFileBuffers requires a
// GENERIC_WRITE file handle and is not documented for directory handles.
// A successful create-only AtomicWriter publication uses MOVEFILE_WRITE_THROUGH,
// whose return guarantees that move reached disk. Windows exposes no equivalent
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
