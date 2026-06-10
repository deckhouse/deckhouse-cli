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

package selfupdatecmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/selfupdate"
)

// TestStoredVersionCompletions checks the `d8 cli use <TAB>` source: stored
// versions newest-first, filtered by the typed prefix, with a description.
func TestStoredVersionCompletions(t *testing.T) {
	dir := t.TempDir()
	store := selfupdate.NewStoreAt(filepath.Join(dir, "cli"))

	// Store entries are smoke-tested with --version, so the payload is a script.
	src := filepath.Join(dir, "binary")
	require.NoError(t, os.WriteFile(src, []byte("#!/bin/sh\nexit 0\n"), 0o755))

	for _, tag := range []string{"v0.13.0", "v0.14.0", "v1.0.0"} {
		require.NoError(t, store.Archive(context.Background(), src, tag))
	}

	all := storedVersionCompletions(store, "")
	assert.Equal(t, []string{
		"v1.0.0\tinstalled locally, switches offline",
		"v0.14.0\tinstalled locally, switches offline",
		"v0.13.0\tinstalled locally, switches offline",
	}, all)

	filtered := storedVersionCompletions(store, "v0.1")
	assert.Len(t, filtered, 2, "completions must honor the typed prefix")

	assert.Empty(t, storedVersionCompletions(nil, ""), "a nil store completes to nothing")
}
