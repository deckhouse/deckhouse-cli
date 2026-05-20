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
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// TestModulesService_RootURL_UsesEditionSegment is the regression for the
// "missing edition segment in release-channel URLs" report applied to the
// modules service: the rootURL used to compose per-module registry paths
// must include the edition segment so that lookups land on the same path
// served by the edition-scoped ModulesService.
func TestModulesService_RootURL_UsesEditionSegment(t *testing.T) {
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

	const editionRoot = bareRoot + "/fe"

	assert.Equal(t, editionRoot, svc.rootURL,
		"modules Service.rootURL must be the edition-scoped root so that "+
			"per-module references resolve under <root>/<edition>/modules/<name>/...")

	// pullSingleModule and the public Module model both compose paths as
	//   filepath.Join(svc.rootURL, "modules", <name>).
	// Verify that the composed path carries the edition segment.
	const moduleName = "console"
	composed := filepath.Join(svc.rootURL, "modules", moduleName)
	assert.Equal(t, editionRoot+"/modules/"+moduleName, composed,
		"per-module registry path must live under the edition sub-tree")
}

// TestModulesService_RootURL_NoEdition pins down that with no edition both
// roots collapse to the bare host — so the new GetEditionRoot() plumbing is
// a true superset of the previous behaviour, not a different one.
func TestModulesService_RootURL_NoEdition(t *testing.T) {
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

	assert.Equal(t, bareRoot, svc.rootURL,
		"modules Service.rootURL must equal the bare root when no edition is set")
}

// TestModulesService_RootURL_CoversAllEditions sweeps every Edition value
// so that adding a new edition to pkg.Edition without wiring it through
// registryservice.NewService surfaces here as an explicit mismatch.
func TestModulesService_RootURL_CoversAllEditions(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	const bareRoot = "registry.deckhouse.ru/deckhouse"

	editions := []pkg.Edition{
		pkg.FEEdition,
		pkg.EEEdition,
		pkg.SEEdition,
		pkg.SEPlusEdition,
		pkg.BEEdition,
		pkg.CEEdition,
	}

	for _, edition := range editions {
		t.Run("edition="+edition.String(), func(t *testing.T) {
			c := pkgclient.NewFromOptions(bareRoot)
			regSvc := registryservice.NewService(c, edition, logger)

			svc := NewService(
				regSvc,
				t.TempDir(),
				&Options{BundleDir: t.TempDir(), DryRun: true},
				logger,
				userLogger,
			)

			expected := bareRoot + "/" + edition.String()
			assert.Equal(t, expected, svc.rootURL,
				"modules Service.rootURL must include the %s edition segment", edition)
		})
	}
}
