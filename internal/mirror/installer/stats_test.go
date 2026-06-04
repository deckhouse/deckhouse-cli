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
	"testing"

	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/fake"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// TestStats_DryRun verifies that, in dry-run mode, Stats reports the planned
// installer image count from the download list (a single installer tag).
func TestStats_DryRun(t *testing.T) {
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

	require.NoError(t, svc.PullInstaller(context.Background()))

	stats := svc.Stats()
	require.True(t, stats.Attempted)
	require.Equal(t, 1, stats.Images)
}

// TestStats_RealPull_SurvivesPacking is the regression test for the bug where
// Stats reported 0 images after a successful real pull. The pack step deletes
// every OCI layout file as it tars it (see bundle.Pack), so counting manifests
// in Stats() - which runs after packing - read an emptied layout and returned
// zero. The fix captures the count before packing; this test asserts the
// installer count is non-zero after a full PullInstaller (pull + pack).
func TestStats_RealPull_SurvivesPacking(t *testing.T) {
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
			DryRun:    false,
		},
		logger,
		userLogger,
	)

	require.NoError(t, svc.PullInstaller(context.Background()))

	stats := svc.Stats()
	require.True(t, stats.Attempted)
	require.Equal(t, 1, stats.Images,
		"installer count must survive packing (captured before bundle.Pack deletes the layout)")
}
