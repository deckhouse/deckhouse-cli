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

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

func testManager() *Manager {
	return &Manager{logger: dkplog.NewNop()}
}

// installPluginFixture creates plugins/<name>/v<major>/<name> under root and the
// `current` symlink pointing at it, i.e. a minimally-installed plugin.
func installPluginFixture(t *testing.T, root, name string, major int) {
	t.Helper()

	binary := layout.BinaryPath(root, name, major)
	require.NoError(t, os.MkdirAll(filepath.Dir(binary), 0o755))
	require.NoError(t, os.WriteFile(binary, []byte("#!/bin/sh\n"), 0o755))
	require.NoError(t, os.Symlink(binary, layout.CurrentLinkPath(root, name)))
}
