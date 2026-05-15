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

package installer

import (
	"context"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/fake"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// TestDryRun_NoBundleFilesWritten verifies that PullInstaller in dry-run mode
// does not write any tar bundles to the bundle directory.
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
			TargetTag: "v1.69.0",
			BundleDir: bundleDir,
			DryRun:    true,
		},
		logger,
		userLogger,
	)

	err := svc.PullInstaller(context.Background())
	require.NoError(t, err)

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run must not write any files to the bundle directory; found: %v", entries)
}

// TestDryRun_WorkingDirHasLayouts verifies that PullInstaller in dry-run mode
// creates the installer OCI layout directory under the working directory but
// does not pack anything into bundles.
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
			TargetTag: "v1.69.0",
			BundleDir: bundleDir,
			DryRun:    true,
		},
		logger,
		userLogger,
	)

	err := svc.PullInstaller(context.Background())
	require.NoError(t, err)

	// Installer OCI layout should be created under workingDir/installer
	installerDir := filepath.Join(workingDir, "installer")
	_, statErr := os.Stat(installerDir)
	assert.NoError(t, statErr, "installer OCI layout should be created in working dir during dry-run; dir: %s", installerDir)

	// Bundle dir must remain empty
	bundleEntries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, bundleEntries, "dry-run must not write any bundle files; found: %v", bundleEntries)
}

// TestInstallerDownloadList_FilledCorrectly verifies that PullInstaller populates
// downloadList.Installer with exactly the key
// "{rootURL}/installer:{tag}" (i.e. the line
//
//	l.Installer[path.Join(l.rootURL, internal.InstallerSegment)+":"+tag] = nil
//
// produces the expected reference) when the installer is accessible in the registry.
func TestInstallerDownloadList_FilledCorrectly(t *testing.T) {
	workingDir := t.TempDir()
	bundleDir := t.TempDir()

	stubClient := fake.NewRegistryClientStub()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	regSvc := registryservice.NewService(stubClient, pkg.FEEdition, logger)

	const targetTag = "v1.69.0"
	svc := NewService(
		regSvc,
		workingDir,
		&Options{
			TargetTag: targetTag,
			BundleDir: bundleDir,
			DryRun:    true,
		},
		logger,
		userLogger,
	)

	err := svc.PullInstaller(context.Background())
	require.NoError(t, err)

	expectedRef := path.Join(regSvc.GetRoot(), internal.InstallerSegment) + ":" + targetTag
	_, ok := svc.downloadList.Installer[expectedRef]
	assert.True(t, ok,
		"downloadList.Installer should contain %q; actual keys: %v",
		expectedRef, svc.downloadList.Installer,
	)
}
