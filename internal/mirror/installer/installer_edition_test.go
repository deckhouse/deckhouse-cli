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
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// TestInstallerService_RootURL_StaysAtBareRoot_WithEdition is a regression test
// guarding the asymmetry in the registry service: while deckhouse, modules and
// security live UNDER the edition segment, the installer (and plugins) live
// OUTSIDE it (at <root>/installer). The fix that introduced GetEditionRoot()
// must NOT spill over into the installer wiring — installer.NewService must
// keep using registryService.GetRoot() (the bare root) for its downloadList.
//
// Without this regression test it would be easy to "uniformly" switch every
// service to GetEditionRoot() and silently break installer paths
// (<root>/<edition>/installer:tag does not exist).
func TestInstallerService_RootURL_StaysAtBareRoot_WithEdition(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	const bareRoot = "registry.deckhouse.ru/deckhouse"
	c := pkgclient.NewFromOptions(bareRoot)
	regSvc := registryservice.NewService(c, pkg.FEEdition, logger)

	svc := NewService(
		regSvc,
		t.TempDir(),
		&Options{BundleDir: t.TempDir(), DryRun: true},
		logger,
		userLogger,
	)

	const tag = "v1.75.7"
	svc.downloadList.FillInstallerImages([]string{tag})

	expected := bareRoot + "/" + internal.InstallerSegment + ":" + tag
	_, ok := svc.downloadList.Installer[expected]
	assert.Truef(t, ok,
		"installer downloadList must contain %q (bare-root, no edition); actual keys: %v",
		expected, svc.downloadList.Installer)

	// The forbidden shape: <root>/<edition>/installer:tag. The installer
	// repository does not exist under the edition sub-tree, so any such key
	// would point at a 404 in production.
	forbidden := bareRoot + "/fe/" + internal.InstallerSegment + ":" + tag
	_, badFound := svc.downloadList.Installer[forbidden]
	assert.Falsef(t, badFound,
		"installer downloadList must NOT contain edition-scoped key %q; "+
			"installer lives at <root>/installer regardless of edition",
		forbidden)

	for key := range svc.downloadList.Installer {
		assert.Falsef(t,
			strings.HasPrefix(key, bareRoot+"/fe/"),
			"installer downloadList key %q must not start with the edition-scoped prefix", key)
	}
}

// TestInstallerService_RootURL_NoEdition pins down the unchanged behaviour
// when edition is not set: installer paths still sit at <root>/installer.
func TestInstallerService_RootURL_NoEdition(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	const bareRoot = "registry.example.com/deckhouse"
	c := pkgclient.NewFromOptions(bareRoot)
	regSvc := registryservice.NewService(c, pkg.NoEdition, logger)

	svc := NewService(
		regSvc,
		t.TempDir(),
		&Options{BundleDir: t.TempDir(), DryRun: true},
		logger,
		userLogger,
	)

	const tag = "v1.75.7"
	svc.downloadList.FillInstallerImages([]string{tag})

	expected := bareRoot + "/" + internal.InstallerSegment + ":" + tag
	_, ok := svc.downloadList.Installer[expected]
	assert.Truef(t, ok,
		"installer downloadList must contain %q; actual keys: %v",
		expected, svc.downloadList.Installer)
}
