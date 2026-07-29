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

// Package lockfile serializes mutating d8 operations across concurrent processes.
//
// # Problem
//
// Self-update and plugin install/update rewrite binaries, symlinks, and caches on
// disk. Two such commands can run concurrently (separate terminals or scripts);
// without a cross-process lock they would step on each other.
//
// # Call sites
//
// Two features share this package (same mechanism, different paths):
//
//   - CLI self-update: ~/.deckhouse-cli/cli/install.lock
//   - Plugin install:  <plugins-root>/<name>/install.lock
//
// # Usage
//
// Hold the lock for the whole critical section. Do not block waiting - if another
// process holds a live lock, return a user-facing error:
//
//	release, err := lockfile.Acquire(path, staleAfter, onReclaim)
//	if errors.Is(err, lockfile.ErrLocked) {
//	    return fmt.Errorf("operation already in progress")
//	}
//	if err != nil {
//	    return err
//	}
//	defer release()
//
// # Stale locks
//
// A holder killed with SIGKILL never runs release(). Acquire treats a lock file
// older than staleAfter as orphaned, reclaims it, and optionally notifies via
// onReclaim. Callers typically use one hour.
//
// # Implementation
//
//   - Capture: atomic O_EXCL create (empty file at path; no open fd required).
//   - Reclaim: rename to a scratch path, verify file identity, then remove.
//     Plain Remove(path) races with a concurrent acquirer and can delete their
//     fresh lock; identity-checked rename closes that hole.
package lockfile
