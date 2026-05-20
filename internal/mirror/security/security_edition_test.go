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
	"log/slog"
	"path"
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

// TestSecurityService_RootURL_UsesEditionSegment is the regression for the
// "missing edition segment in release-channel URLs" report: it asserts that
// the security downloadList is seeded with the edition-scoped root so that
// every FillSecurityImages key sits under <root>/<edition>/security/...
// rather than the bare <root>/security/... path.
//
// We exercise the contract directly at the security.NewService boundary so
// the test stays independent of the in-memory fake registry's
// GetRegistry semantics (the upfake intentionally exposes only the host,
// which would mask the bug).
func TestSecurityService_RootURL_UsesEditionSegment(t *testing.T) {
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

	svc.downloadList.FillSecurityImages()

	const editionRoot = bareRoot + "/fe"

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
		assert.Truef(t, ok,
			"downloadList.Security must contain an entry for database %q", db.name)
		if !ok {
			continue
		}

		expectedRef := path.Join(editionRoot, internal.SecuritySegment, db.name) + ":" + db.tag
		_, refOK := imageSet[expectedRef]
		assert.Truef(t, refOK,
			"downloadList.Security[%q] must contain edition-scoped ref %q; actual keys: %v",
			db.name, expectedRef, imageSet)

		// No key may be written under the bare (non-edition) root — that would
		// be the duplicate that caused the user-visible
		// "registry.deckhouse.ru/deckhouse/security/..." pulls.
		bareRef := path.Join(bareRoot, internal.SecuritySegment, db.name) + ":" + db.tag
		_, bareFound := imageSet[bareRef]
		assert.Falsef(t, bareFound,
			"downloadList.Security[%q] must NOT contain non-edition-scoped ref %q",
			db.name, bareRef)

		// Cross-check: every key in the set is edition-scoped.
		for key := range imageSet {
			assert.Truef(t, strings.HasPrefix(key, editionRoot+"/"),
				"downloadList.Security[%q] key %q must start with edition-scoped root %q",
				db.name, key, editionRoot)
		}
	}
}

// TestSecurityService_RootURL_NoEdition pins down that when no edition is
// configured the security downloadList stays at the bare root — i.e. the
// new GetEditionRoot() plumbing must not introduce a phantom segment when
// edition == NoEdition.
func TestSecurityService_RootURL_NoEdition(t *testing.T) {
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

	svc.downloadList.FillSecurityImages()

	for dbName, imageSet := range svc.downloadList.Security {
		for key := range imageSet {
			assert.Truef(t, strings.HasPrefix(key, bareRoot+"/"+internal.SecuritySegment+"/"),
				"downloadList.Security[%q] key %q must start with the bare root + security/ when no edition is set",
				dbName, key)
		}
	}
}
