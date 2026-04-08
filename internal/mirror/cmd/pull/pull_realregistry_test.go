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

package pull

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pullflags "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/flags"
)

// TestDryRunRealRegistry is an integration smoke-test that hits a real Deckhouse
// registry. It is skipped automatically unless D8_TEST_REGISTRY and
// D8_TEST_LICENSE_TOKEN environment variables are set.
//
// Example:
//
//	D8_TEST_REGISTRY=registry.deckhouse.io/deckhouse/fe \
//	D8_TEST_LICENSE_TOKEN=<token> \
//	go test ./internal/mirror/cmd/pull/ -run TestDryRunRealRegistry -v -timeout 300s
func TestDryRunRealRegistry(t *testing.T) {
	registry := os.Getenv("D8_TEST_REGISTRY")
	licenseToken := os.Getenv("D8_TEST_LICENSE_TOKEN")
	if registry == "" || licenseToken == "" {
		t.Skip("skipping real-registry test: D8_TEST_REGISTRY and D8_TEST_LICENSE_TOKEN must be set")
	}

	bundleDir := t.TempDir()
	tmpDir := t.TempDir()

	cmd := NewCommand()
	defer saveFlagsAndRestore(t)()

	pullflags.ImagesBundlePath = bundleDir
	pullflags.TempDir = tmpDir
	pullflags.SourceRegistryRepo = registry
	pullflags.DeckhouseLicenseToken = licenseToken
	pullflags.DeckhouseTag = "v1.69.0"
	pullflags.InstallerTag = "v1.69.0"
	pullflags.NoPlatform = false
	pullflags.NoSecurityDB = true
	pullflags.NoModules = true
	pullflags.NoInstaller = false
	pullflags.DryRun = true
	pullflags.DoGOSTDigest = false
	pullflags.NoPullResume = true
	pullflags.SkipVexImages = true

	ctx := context.Background()
	cmd.SetContext(ctx)

	puller := NewPuller(cmd)
	err := puller.Execute(ctx)
	require.NoError(t, err, "dry-run against real registry must succeed")

	// bundleDir must have NO .tar / .chunk output files
	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	for _, e := range entries {
		ext := filepath.Ext(e.Name())
		assert.NotEqual(t, ".tar", ext, "dry-run must not write .tar to bundle dir, found: %s", e.Name())
		assert.NotEqual(t, ".chunk", ext, "dry-run must not write .chunk to bundle dir, found: %s", e.Name())
	}

	// tmpDir MUST have content: installer OCI layouts land here
	var tmpFiles []string
	_ = filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			tmpFiles = append(tmpFiles, path)
		}
		return nil
	})
	assert.NotEmpty(t, tmpFiles, "dry-run must write installer OCI layouts to tmpDir so images_digests.json can be read")

	t.Logf("tmpDir files written during dry-run: %d", len(tmpFiles))
	t.Logf("bundleDir entries (must be 0): %d", len(entries))
}
