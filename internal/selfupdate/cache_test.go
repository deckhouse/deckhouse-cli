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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheSaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "update-check.json")

	want := updateCheckCache{CheckedAt: time.Now().UTC().Truncate(time.Second), LatestVersion: "v0.14.0"}
	require.NoError(t, saveCache(path, want))

	got, err := loadCache(path)
	require.NoError(t, err)
	assert.Equal(t, want.LatestVersion, got.LatestVersion)
	assert.WithinDuration(t, want.CheckedAt, got.CheckedAt, time.Second)
}

func TestLoadCacheMissing(t *testing.T) {
	_, err := loadCache(filepath.Join(t.TempDir(), "absent.json"))
	require.Error(t, err)
}

func TestCacheIsStale(t *testing.T) {
	assert.False(t, updateCheckCache{CheckedAt: time.Now()}.isStale())
	assert.True(t, updateCheckCache{CheckedAt: time.Now().Add(-2 * updateCheckTTL)}.isStale())
	assert.True(t, updateCheckCache{}.isStale(), "zero time must be stale")
}
