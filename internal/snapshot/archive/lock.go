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
	"path/filepath"

	"github.com/gofrs/flock"
)

const archiveLockFileName = ".d8-snapshot-download.lock"

// ErrArchiveLocked is returned when an incompatible archive reader or writer owns the lock.
var ErrArchiveLocked = errors.New("snapshot archive is locked")

// Lock is a held cooperative archive lock. Callers must Unlock it.
type Lock struct {
	file *flock.Flock
}

// AcquireReadLock takes a non-blocking shared lock. Multiple uploads may coexist, while an
// upload and a download writer exclude one another.
func AcquireReadLock(root string) (*Lock, error) {
	return acquireLock(root, false)
}

// AcquireWriteLock takes a non-blocking exclusive lock used by archive writers.
func AcquireWriteLock(root string) (*Lock, error) {
	return acquireLock(root, true)
}

func acquireLock(root string, exclusive bool) (*Lock, error) {
	if exclusive {
		if err := os.MkdirAll(root, 0o755); err != nil {
			return nil, fmt.Errorf("create archive root %s: %w", root, err)
		}
	}

	lockPath := filepath.Join(root, archiveLockFileName)
	file := flock.New(lockPath)

	var (
		locked bool
		err    error
	)

	if exclusive {
		locked, err = file.TryLock()
	} else {
		locked, err = file.TryRLock()
	}

	if err != nil {
		return nil, fmt.Errorf("lock snapshot archive %s: %w", root, err)
	}

	if !locked {
		return nil, fmt.Errorf("%w: %s", ErrArchiveLocked, root)
	}

	return &Lock{file: file}, nil
}

// Unlock releases the held archive lock.
func (l *Lock) Unlock() error {
	if l == nil || l.file == nil {
		return nil
	}

	return l.file.Unlock()
}
