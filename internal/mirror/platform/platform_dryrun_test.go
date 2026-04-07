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

package platform

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	"github.com/deckhouse/deckhouse-cli/pkg/stub"
)

// TestDryRun_NoBundleFilesWritten verifies that PullPlatform in dry-run mode does
// not write any files to the bundle directory. Temporary OCI layout data may only
// land under the working/tmp directory.
func TestDryRun_NoBundleFilesWritten(t *testing.T) {
	workingDir := t.TempDir()
	bundleDir := t.TempDir() // must stay empty after dry-run

	stubClient := stub.NewRegistryClientStub()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	regSvc := registryservice.NewService(stubClient, pkg.FEEdition, logger)

	svc := NewService(
		regSvc,
		workingDir,
		&Options{
			TargetTag: "v1.69.0",
			BundleDir: bundleDir,
			DryRun:    true,
		},
		logger,
		userLogger,
	)

	err := svc.PullPlatform(context.Background())
	require.NoError(t, err)

	// bundleDir must contain nothing after dry-run
	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run must not write any files to the bundle directory; found: %v", entries)
}

// TestDryRun_InstallerPulledToTmpDir verifies that in dry-run mode the installer
// image IS pulled into the working (tmp) directory so that images_digests.json can
// be read from it. This produces the complete list of images that would be
// downloaded in a real run.
func TestDryRun_InstallerPulledToTmpDir(t *testing.T) {
	workingDir := t.TempDir()
	bundleDir := t.TempDir()

	stubClient := stub.NewRegistryClientStub()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	regSvc := registryservice.NewService(stubClient, pkg.FEEdition, logger)

	svc := NewService(
		regSvc,
		workingDir,
		&Options{
			TargetTag: "v1.69.0",
			BundleDir: bundleDir,
			DryRun:    true,
		},
		logger,
		userLogger,
	)

	err := svc.PullPlatform(context.Background())
	require.NoError(t, err)

	// The installer OCI layout directory must exist under workingDir, proving
	// that pullInstallers was executed (so images_digests.json extraction was
	// attempted).
	installerLayoutDir := filepath.Join(workingDir, "platform", "install")
	_, statErr := os.Stat(installerLayoutDir)
	assert.NoError(t, statErr, "installer OCI layout must be created in tmpDir during dry-run; dir: %s", installerLayoutDir)

	// bundleDir must remain empty – no platform.tar, no deckhousereleases.yaml
	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run must not write any files to the bundle directory; found: %v", entries)
}
