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

package security

import (
	"context"
	"log/slog"
	"os"
	"path"
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

// TestDryRun_NoBundleFilesWritten verifies that PullSecurity in dry-run mode does
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

	err := svc.PullSecurity(context.Background())
	require.NoError(t, err)

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run must not write any files to the bundle directory; found: %v", entries)
}

// TestDryRun_WorkingDirHasLayouts verifies that PullSecurity in dry-run mode
// creates OCI layout directories under the working directory but does not
// pack anything into bundles.
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

	err := svc.PullSecurity(context.Background())
	require.NoError(t, err)

	// Security OCI layouts should be created under workingDir
	entries, err := os.ReadDir(workingDir)
	require.NoError(t, err)
	assert.NotEmpty(t, entries, "security OCI layouts should be created in working dir during dry-run")

	// Bundle dir must remain empty
	bundleEntries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, bundleEntries, "dry-run must not write any bundle files; found: %v", bundleEntries)
}

// TestSecurityDownloadList_FilledCorrectly verifies that PullSecurity populates
// downloadList.Security with the expected image reference keys, one entry per
// database name.  Each key is constructed by FillSecurityImages as:
//
//	path.Join(rootURL, "security", <segment>) + ":" + <tag>
func TestSecurityDownloadList_FilledCorrectly(t *testing.T) {
	workingDir := t.TempDir()
	bundleDir := t.TempDir()

	// pkg.NoEdition avoids double-scoping: the stub root already contains the
	// full path "registry.deckhouse.ru/deckhouse/fe", so using FEEdition would
	// prepend an extra "fe" segment when resolving security image paths.
	stubClient := fake.NewRegistryClientStub()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	regSvc := registryservice.NewService(stubClient, pkg.NoEdition, logger)

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

	err := svc.PullSecurity(context.Background())
	require.NoError(t, err)

	rootURL := regSvc.GetRoot()

	// Each database must appear as its own sub-map key inside downloadList.Security.
	expectedDatabases := []struct {
		name string
		tag  string
	}{
		{internal.SecurityTrivyDBSegment, "2"},
		{internal.SecurityTrivyBDUSegment, "1"},
		{internal.SecurityTrivyJavaDBSegment, "1"},
		{internal.SecurityTrivyChecksSegment, "0"},
	}

	for _, db := range expectedDatabases {
		imageSet, ok := svc.downloadList.Security[db.name]
		assert.True(t, ok,
			"downloadList.Security must contain an entry for database %q", db.name)
		if !ok {
			continue
		}

		expectedRef := path.Join(rootURL, internal.SecuritySegment, db.name) + ":" + db.tag
		_, refOK := imageSet[expectedRef]
		assert.True(t, refOK,
			"downloadList.Security[%q] should contain ref %q; actual keys: %v",
			db.name, expectedRef, imageSet)
	}
}
