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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/version"
)

// TestSwitchToStoredVersionRepointsSymlink exercises the store-hit half of
// `d8 cli use` for a managed install: no stage function, no .old churn - just
// an atomic repoint of the `current` symlink.
func TestSwitchToStoredVersionRepointsSymlink(t *testing.T) {
	dir := t.TempDir()
	store := &Store{root: filepath.Join(dir, "cli")}

	for _, marker := range []string{"v1", "v2"} {
		src := writeTestBinary(t, dir, marker, marker)
		require.NoError(t, store.Archive(context.Background(), src, marker+".0.0"))
	}

	require.NoError(t, store.SwitchCurrent("v1.0.0"))

	// A managed install: the running binary resolves into the store.
	exePath, err := filepath.EvalSymlinks(store.BinaryPath("v1.0.0"))
	require.NoError(t, err)

	res, err := SwitchTo(context.Background(), exePath, "v2.0.0", store, dkplog.NewNop(), nil)
	require.NoError(t, err)

	assert.False(t, res.Migrated, "a managed install must not be migrated again")
	assert.Equal(t, "v1.0.0", res.PrevTag)
	assert.Equal(t, "v2.0.0", store.CurrentTag())

	assert.True(t, store.Has("v1.0.0"), "the displaced version stays installed")

	got, err := os.ReadFile(store.CurrentLinkPath())
	require.NoError(t, err)
	assert.Equal(t, testScript("v2"), string(got))
}

// TestSwitchToMissingVersionWithoutStageFails covers `use` of a version that is
// neither stored nor downloadable (stage == nil): a clear error, no changes.
func TestSwitchToMissingVersionWithoutStageFails(t *testing.T) {
	dir := t.TempDir()
	store := &Store{root: filepath.Join(dir, "cli")}

	exePath := writeTestBinary(t, dir, "d8", "RUNNING")

	_, err := SwitchTo(context.Background(), exePath, "v9.9.9", store, dkplog.NewNop(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in the local store")
}

// TestSwitchToSeedsRunningVersion verifies migration retention: a plain-file
// install with a semver version is archived under its own tag, so the displaced
// version remains switchable (and `use <running version>` needs no download).
func TestSwitchToSeedsRunningVersion(t *testing.T) {
	prev := version.Version
	version.Version = "v0.9.0"
	t.Cleanup(func() { version.Version = prev })

	dir := t.TempDir()
	store := &Store{root: filepath.Join(dir, "cli")}

	exePath := writeTestBinary(t, dir, "d8", "RUNNING")

	// Switching to the running binary's own version: seeded by retention, no stage.
	res, err := SwitchTo(context.Background(), exePath, "v0.9.0", store, dkplog.NewNop(), nil)
	require.NoError(t, err)

	assert.True(t, res.Migrated)
	assert.True(t, store.Has("v0.9.0"), "the running binary must be seeded into the store")
	assert.Equal(t, "v0.9.0", store.CurrentTag())

	got, err := os.ReadFile(exePath) // PATH symlink -> current -> stored binary
	require.NoError(t, err)
	assert.Equal(t, testScript("RUNNING"), string(got))
}
