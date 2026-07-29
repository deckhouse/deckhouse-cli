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

package lockfile

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// ErrLocked means another process holds a live lock at the path.
// Callers should fail fast with a user-facing message instead of waiting.
var ErrLocked = errors.New("lock is already held")

// Acquire creates an exclusive lock file at path for the duration of a mutating
// operation (self-update or plugin install). See package lockfile for call sites,
// the usage pattern, and reclaim semantics.
//
//   - Returns release; call it exactly once when done (typically defer release()).
//   - Returns ErrLocked if another process already holds a non-stale lock.
//   - Reclaims a lock older than staleAfter before trying to acquire.
//   - Calls onReclaim (optional) with the orphan's age after a successful reclaim.
func Acquire(path string, staleAfter time.Duration, onReclaim func(age time.Duration)) (func(), error) {
	info, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("check lock file %s: %w", path, err)
	}

	stale := err == nil && time.Since(info.ModTime()) > staleAfter
	if stale {
		if err := reclaim(path, info, onReclaim); err != nil {
			return nil, err
		}
	}

	// O_EXCL makes create atomic: a racer between reclaim and create loses here.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if os.IsExist(err) {
		return nil, fmt.Errorf("%w: %s", ErrLocked, path)
	}

	if err != nil {
		return nil, fmt.Errorf("create lock file %s: %w", path, err)
	}

	_ = file.Close()

	return func() { _ = os.Remove(path) }, nil
}

// reclaim removes the orphaned lock described by stale.
//
//   - Renames path to a unique scratch path; exactly one concurrent reclaimer wins.
//   - Checks file identity: if the moved file is not stale, a concurrent acquirer
//     created a fresh lock after our staleness check - restore it and return ErrLocked.
//   - Otherwise removes the scratch file and optionally calls onReclaim.
func reclaim(path string, stale os.FileInfo, onReclaim func(age time.Duration)) error {
	reclaimScratch := fmt.Sprintf("%s.reclaim.%d", path, os.Getpid())

	err := os.Rename(path, reclaimScratch)
	if os.IsNotExist(err) {
		// Another reclaimer won the rename; fall through to O_EXCL create.
		return nil
	}

	if err != nil {
		return fmt.Errorf("reclaim stale lock %s: %w", path, err)
	}

	moved, err := os.Stat(reclaimScratch)
	// Rename may have moved a fresh lock created after our Stat in Acquire.
	grabbedFresh := err == nil && !os.SameFile(stale, moved)

	if grabbedFresh {
		if err := os.Rename(reclaimScratch, path); err != nil {
			_ = os.Remove(reclaimScratch)
		}

		return fmt.Errorf("%w: %s", ErrLocked, path)
	}

	if onReclaim != nil {
		onReclaim(time.Since(stale.ModTime()))
	}

	return os.Remove(reclaimScratch)
}
