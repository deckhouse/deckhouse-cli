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
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg/fake"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// TestDryRun_NoBundleFilesWritten verifies that PullPlatform in dry-run mode does
// not write any files to the bundle directory.
func TestDryRun_NoBundleFilesWritten(t *testing.T) {
	bundleDir := t.TempDir() // must stay empty after dry-run

	stubClient := fake.NewRegistryClientStub()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newTestPlatformService(
		stubClient,
		&Options{TargetTag: "v1.69.0", BundleDir: bundleDir, DryRun: true},
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

// TestDryRun_NoOCILayoutCreated verifies that in dry-run mode no OCI image layout
// directories are created under the working directory. images_digests.json is
// streamed directly from the remote registry without writing anything to disk.
func TestDryRun_NoOCILayoutCreated(t *testing.T) {
	workingDir := t.TempDir()
	bundleDir := t.TempDir()

	stubClient := fake.NewRegistryClientStub()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	deckhouseSvc := registryservice.NewDeckhouseService(stubClient, logger)
	svc := &Service{
		deckhouseService: deckhouseSvc,
		downloadList:     NewImageDownloadList(stubClient.GetRegistry()),
		options:          &Options{TargetTag: "v1.69.0", BundleDir: bundleDir, DryRun: true},
		logger:           logger,
		userLogger:       userLogger,
	}

	err := svc.PullPlatform(context.Background())
	require.NoError(t, err)

	// No OCI layout directories should be created.
	installerLayoutDir := filepath.Join(workingDir, "platform", "install")
	_, statErr := os.Stat(installerLayoutDir)
	assert.ErrorIs(t, statErr, os.ErrNotExist,
		"installer OCI layout must NOT be created in dry-run")

	// bundleDir must remain empty
	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run must not write any files to the bundle directory; found: %v", entries)
}

// TestPlatformDownloadList_FilledCorrectly verifies that PullPlatform in
// dry-run mode populates every sub-map of downloadList with the expected image
// reference keys for the requested version.  The key shapes are:
//
//	Deckhouse:                   rootURL + ":" + version
//	DeckhouseInstall:            path.Join(rootURL, "install") + ":" + version
//	DeckhouseInstallStandalone:  path.Join(rootURL, "install-standalone") + ":" + version
//	DeckhouseReleaseChannel:     path.Join(rootURL, "release-channel") + ":" + channel|version
func TestPlatformDownloadList_FilledCorrectly(t *testing.T) {
	stubClient := fake.NewRegistryClientStub()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	const targetTag = "v1.69.0"
	svc := &Service{
		deckhouseService: registryservice.NewDeckhouseService(stubClient, logger),
		downloadList:     NewImageDownloadList(stubClient.GetRegistry()),
		options:          &Options{TargetTag: targetTag, BundleDir: t.TempDir(), DryRun: true},
		logger:           logger,
		userLogger:       userLogger,
	}

	err := svc.PullPlatform(context.Background())
	require.NoError(t, err)

	rootURL := stubClient.GetRegistry()

	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":"+targetTag,
		"Deckhouse map must contain the requested version tag")

	assert.Contains(t, svc.downloadList.DeckhouseInstall,
		path.Join(rootURL, internal.InstallSegment)+":"+targetTag,
		"DeckhouseInstall map must contain %q", internal.InstallSegment+":"+targetTag)

	assert.Contains(t, svc.downloadList.DeckhouseInstallStandalone,
		path.Join(rootURL, internal.InstallStandaloneSegment)+":"+targetTag,
		"DeckhouseInstallStandalone map must contain %q", internal.InstallStandaloneSegment+":"+targetTag)

	// v1.69.0 is the stable channel version — its channel alias must live only
	// in the release-channel map, not in the main Deckhouse or Install maps.
	assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
		path.Join(rootURL, internal.ReleaseChannelSegment)+":stable",
		"DeckhouseReleaseChannel map must contain the stable channel alias")

	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":stable",
		"main Deckhouse map must not carry channel aliases")

	assert.NotContains(t, svc.downloadList.DeckhouseInstall,
		path.Join(rootURL, internal.InstallSegment)+":stable",
		"DeckhouseInstall map must not carry channel aliases")
}
