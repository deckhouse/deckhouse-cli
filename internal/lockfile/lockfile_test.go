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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const staleAfter = time.Hour

func TestAcquireIsExclusive(t *testing.T) {
	lock := filepath.Join(t.TempDir(), "x.lock")

	release, err := Acquire(lock, staleAfter, nil)
	require.NoError(t, err)
	require.NotNil(t, release)

	_, err = Acquire(lock, staleAfter, nil)
	require.ErrorIs(t, err, ErrLocked, "a held lock blocks a second acquirer")

	release()

	release2, err := Acquire(lock, staleAfter, nil)
	require.NoError(t, err, "a released lock can be re-acquired")
	release2()
}

func TestAcquireReclaimsStale(t *testing.T) {
	lock := filepath.Join(t.TempDir(), "x.lock")

	require.NoError(t, os.WriteFile(lock, nil, 0o644))
	old := time.Now().Add(-2 * staleAfter)
	require.NoError(t, os.Chtimes(lock, old, old))

	var reclaimedAge time.Duration

	release, err := Acquire(lock, staleAfter, func(age time.Duration) { reclaimedAge = age })
	require.NoError(t, err, "a stale lock is reclaimed (orphaned by a hard kill)")
	assert.Greater(t, reclaimedAge, staleAfter, "onReclaim reports the orphan's age")

	release()

	_, statErr := os.Stat(lock)
	assert.True(t, os.IsNotExist(statErr), "release removes the lock")
}

func TestReclaimDoesNotStealFreshLock(t *testing.T) {
	// remove-by-path race: A and B both judge the lock stale.
	//
	//   - A reclaims and creates a fresh lock.
	//   - B continues with its old FileInfo, must detect the mismatch,
	//     leave A's lock in place, and return ErrLocked.
	lock := filepath.Join(t.TempDir(), "x.lock")

	require.NoError(t, os.WriteFile(lock, nil, 0o644))
	old := time.Now().Add(-2 * staleAfter)
	require.NoError(t, os.Chtimes(lock, old, old))

	staleInfo, err := os.Stat(lock)
	require.NoError(t, err)

	// A wins reclaim and holds a fresh lock.
	releaseA, err := Acquire(lock, staleAfter, nil)
	require.NoError(t, err)

	// B replays reclaim from the stale FileInfo captured earlier.
	err = reclaim(lock, staleInfo, nil)
	require.ErrorIs(t, err, ErrLocked, "B must report the lock held, not reclaim it")

	_, statErr := os.Stat(lock)
	require.NoError(t, statErr, "A's fresh lock is still in place")

	releaseA()
}
