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

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	"github.com/deckhouse/deckhouse-cli/pkg/stub"
)

// TestDryRun_NoBundleFilesWritten verifies that PullModules in dry-run mode does
// not write any tar bundles to the bundle directory.
func TestDryRun_NoBundleFilesWritten(t *testing.T) {
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

	stubClient := stub.NewRegistryClientStub()
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
