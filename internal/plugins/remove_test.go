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

package plugins

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

func TestRemoveIsIdempotentWhenNotInstalled(t *testing.T) {
	m := testManager()
	m.pluginDirectory = t.TempDir()

	require.NoError(t, m.Remove("ghost"), "removing a plugin that is not installed is a no-op")
}

func TestRemoveRejectsInvalidName(t *testing.T) {
	m := testManager()
	m.pluginDirectory = t.TempDir()

	require.Error(t, m.Remove("../escape"))
}

// TestRemoveBlockedByHeldInstallLock proves the fix: a remove cannot delete a
// plugin while an install holds its lock - it fails fast and leaves the files in
// place instead of wiping the directory out from under the install.
func TestRemoveBlockedByHeldInstallLock(t *testing.T) {
	root := t.TempDir()
	m := testManager()
	m.pluginDirectory = root

	bin := layout.BinaryPath(root, "p", 1)
	require.NoError(t, os.MkdirAll(filepath.Dir(bin), 0o755))
	require.NoError(t, os.WriteFile(bin, []byte("x"), 0o755))

	// A held (fresh, non-stale) install lock, as a concurrent install would create.
	require.NoError(t, os.WriteFile(layout.InstallLockPath(root, "p"), nil, 0o644))

	require.Error(t, m.Remove("p"), "remove must not proceed while the install lock is held")

	_, err := os.Stat(bin)
	require.NoError(t, err, "the binary is left in place when the lock is held")
}
