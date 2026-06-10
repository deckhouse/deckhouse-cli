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

package selfupdate

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
)

func TestIsNewer(t *testing.T) {
	assert.True(t, isNewer("v0.13.1", "v0.14.0"))
	assert.False(t, isNewer("v0.14.0", "v0.14.0"))
	assert.False(t, isNewer("v0.15.0", "v0.14.0"))
	assert.False(t, isNewer("dev", "v0.14.0"), "non-semver current must not nag")
	assert.False(t, isNewer("v0.13.1", "latest"), "non-semver latest is ignored")
}

// redirectCache points os.UserCacheDir at a temp dir so the test controls the cache.
func redirectCache(t *testing.T) {
	t.Helper()

	tmp := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
	t.Setenv("HOME", tmp)
}

func TestNotifyIfUpdateAvailable(t *testing.T) {
	redirectCache(t)

	path, err := cachePath()
	require.NoError(t, err)
	require.NoError(t, saveCache(path, updateCheckCache{CheckedAt: time.Now(), LatestVersion: "v0.14.0"}))

	var buf bytes.Buffer
	NotifyIfUpdateAvailable(&buf, "v0.13.1")
	assert.Contains(t, buf.String(), "v0.14.0")
}

func TestNotifyIfUpdateAvailableUpToDate(t *testing.T) {
	redirectCache(t)

	path, err := cachePath()
	require.NoError(t, err)
	require.NoError(t, saveCache(path, updateCheckCache{CheckedAt: time.Now(), LatestVersion: "v0.14.0"}))

	var buf bytes.Buffer
	NotifyIfUpdateAvailable(&buf, "v0.14.0")
	assert.Empty(t, buf.String())
}

func TestNotifyIfUpdateAvailableDisabled(t *testing.T) {
	redirectCache(t)
	t.Setenv(EnvDisableUpdateNotify, "1")

	path, err := cachePath()
	require.NoError(t, err)
	require.NoError(t, saveCache(path, updateCheckCache{CheckedAt: time.Now(), LatestVersion: "v0.14.0"}))

	var buf bytes.Buffer
	NotifyIfUpdateAvailable(&buf, "v0.13.1")
	assert.Empty(t, buf.String(), "notification must be suppressed when disabled")
}

func TestNotifyIfUpdateAvailableNoCache(t *testing.T) {
	redirectCache(t)

	var buf bytes.Buffer
	NotifyIfUpdateAvailable(&buf, "v0.13.1")
	assert.Empty(t, buf.String())
}

func TestRefreshCachePreservesVersionOnFailure(t *testing.T) {
	redirectCache(t)

	path, err := cachePath()
	require.NoError(t, err)

	old := time.Now().Add(-48 * time.Hour)
	require.NoError(t, saveCache(path, updateCheckCache{CheckedAt: old, LatestVersion: "v0.13.1"}))

	updater := NewUpdater(fakeSource{err: errors.New("unreachable")}, nil, dkplog.NewNop())
	require.NoError(t, RefreshCache(context.Background(), updater, "v0.13.0"))

	cache, err := loadCache(path)
	require.NoError(t, err)
	assert.Equal(t, "v0.13.1", cache.LatestVersion, "last-known version preserved on failure")
	assert.True(t, cache.CheckedAt.After(old), "checked_at advanced even on failure")
}

func TestRefreshCacheUpdatesVersionOnSuccess(t *testing.T) {
	redirectCache(t)

	path, err := cachePath()
	require.NoError(t, err)
	require.NoError(t, saveCache(path, updateCheckCache{LatestVersion: "v0.13.1"}))

	updater := NewUpdater(fakeSource{tags: []string{"v0.13.1", "v0.14.0"}}, nil, dkplog.NewNop())
	require.NoError(t, RefreshCache(context.Background(), updater, "v0.13.1"))

	cache, err := loadCache(path)
	require.NoError(t, err)
	assert.Equal(t, "v0.14.0", cache.LatestVersion)
}
