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
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

// TestEnsureInstallRootFallsBackWhenRootReadOnly: the system root already exists
// but belongs to root (d8 has no setuid), so MkdirAll succeeds there and only a
// write probe reveals that installs must go to ~/.deckhouse-cli.
func TestEnsureInstallRootFallsBackWhenRootReadOnly(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs POSIX permissions enforced for a non-root user")
	}

	t.Setenv("HOME", t.TempDir())

	system := t.TempDir()
	require.NoError(t, os.MkdirAll(layout.PluginsRoot(system), 0o755))
	require.NoError(t, os.Chmod(layout.PluginsRoot(system), 0o555))
	t.Cleanup(func() { _ = os.Chmod(layout.PluginsRoot(system), 0o755) })

	m := testManager()
	m.pluginDirectory = system

	require.NoError(t, m.EnsureInstallRoot())

	fallback, err := layout.HomeFallbackPath()
	require.NoError(t, err)
	assert.Equal(t, fallback, m.pluginDirectory)
	assert.DirExists(t, layout.PluginsRoot(fallback))
}
