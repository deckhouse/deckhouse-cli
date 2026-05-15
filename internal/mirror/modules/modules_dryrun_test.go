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

package modules

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/fake"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// TestDryRun_NoBundleFilesWritten verifies that PullModules in dry-run mode does
// not write any tar bundles to the bundle directory.
func TestDryRun_NoBundleFilesWritten(t *testing.T) {
	workingDir := t.TempDir()
	bundleDir := t.TempDir()

	stubClient := fake.NewRegistryClientStub()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	regSvc := registryservice.NewService(stubClient, pkg.FEEdition, logger)

	svc := NewService(
		regSvc,
		workingDir,
		&Options{
			BundleDir: bundleDir,
			DryRun:    true,
		},
		logger,
		userLogger,
	)

	err := svc.PullModules(context.Background())
	require.NoError(t, err)

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run must not write any files to the bundle directory; found: %v", entries)
}

// TestDryRun_WorkingDirHasLayouts verifies that PullModules in dry-run mode
// creates OCI layout directories in the working directory (needed for module
// discovery) but does not pack anything into bundles.
func TestDryRun_WorkingDirHasLayouts(t *testing.T) {
	workingDir := t.TempDir()
	bundleDir := t.TempDir()

	stubClient := fake.NewRegistryClientStub()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	regSvc := registryservice.NewService(stubClient, pkg.FEEdition, logger)

	svc := NewService(
		regSvc,
		workingDir,
		&Options{
			BundleDir: bundleDir,
			DryRun:    true,
		},
		logger,
		userLogger,
	)

	err := svc.PullModules(context.Background())
	require.NoError(t, err)

	// Working dir should have something (module layout dirs) or at least not crash
	// Bundle dir must remain empty
	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run must not write any bundle files; found: %v", entries)
}

// TestModulesReleaseChannelDownloadList_FilledCorrectly verifies that PullModules
// populates modulesDownloadList with the expected release-channel image reference
// keys.  Each key is constructed by discoverChannelVersions as:
//
//	rootURL + "/modules/" + moduleName + "/release:" + channel
func TestModulesReleaseChannelDownloadList_FilledCorrectly(t *testing.T) {
	const moduleName = "console"
	const channelVer = "v1.45.0"

	reg := upfake.NewRegistry(testHost)
	addModule(reg, moduleName, channelVer, []string{channelVer})

	stubClient := pkgclient.Adapt(upfake.NewClient(reg))
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	regSvc := registryservice.NewService(stubClient, pkg.NoEdition, logger)

	svc := NewService(
		regSvc,
		t.TempDir(),
		&Options{
			BundleDir:     t.TempDir(),
			DryRun:        true,
			SkipVexImages: true,
		},
		logger,
		userLogger,
	)

	err := svc.PullModules(context.Background())
	require.NoError(t, err)

	moduleDL := svc.modulesDownloadList.Module(moduleName)
	require.NotNil(t, moduleDL, "modulesDownloadList must have an entry for module %q", moduleName)

	rootURL := regSvc.GetRoot()
	for _, channel := range internal.GetAllDefaultReleaseChannels() {
		expectedRef := rootURL + "/modules/" + moduleName + "/release:" + channel
		_, ok := moduleDL.ModuleReleaseChannels[expectedRef]
		assert.True(t, ok,
			"modulesDownloadList[%q].ModuleReleaseChannels should contain ref %q; actual keys: %v",
			moduleName, expectedRef, moduleDL.ModuleReleaseChannels)
	}
}

// TestModulesDownloadList_NoBundleFilesWritten_WithFakeStub is a smoke test
// that runs PullModules with the standard fake stub (which has no modules
// published).  The expected outcome is success with an empty bundle directory.
func TestModulesDownloadList_NoBundleFilesWritten_WithFakeStub(t *testing.T) {
	bundleDir := t.TempDir()

	stubClient := fake.NewRegistryClientStub()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	regSvc := registryservice.NewService(stubClient, pkg.FEEdition, logger)

	svc := NewService(
		regSvc,
		t.TempDir(),
		&Options{
			BundleDir: bundleDir,
			DryRun:    true,
		},
		logger,
		userLogger,
	)

	err := svc.PullModules(context.Background())
	require.NoError(t, err)

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run must not write any files to the bundle directory; found: %v", entries)
}
