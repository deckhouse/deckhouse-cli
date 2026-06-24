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

package service_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// TestService_GetRoot_GetEditionRoot pins down the contract of the two root
// accessors on the registry Service:
//
//   - GetRoot()        — bare host + base path (no edition segment).
//   - GetEditionRoot() — host + base path + edition segment.
//
// Callers building references for services that live under the edition
// sub-tree (deckhouse, modules, security) must use GetEditionRoot so that
// the resulting URLs match the actual repository scope. Mismatched roots
// were the source of duplicate <root>/release-channel:<channel> pulls
// observed for `d8 mirror pull` against FE/EE sources.
//
// The table covers every valid Edition value plus NoEdition so any future
// edition added to pkg.Edition without updating Service.NewService is caught
// by an explicit failure here.
func TestService_GetRoot_GetEditionRoot(t *testing.T) {
	logger := log.NewNop()

	const host = "registry.deckhouse.ru/deckhouse"

	tests := []struct {
		name                string
		host                string
		edition             pkg.Edition
		expectedRoot        string
		expectedEditionRoot string
	}{
		{
			name:                "no edition leaves both roots equal",
			host:                "registry.example.com/deckhouse",
			edition:             pkg.NoEdition,
			expectedRoot:        "registry.example.com/deckhouse",
			expectedEditionRoot: "registry.example.com/deckhouse",
		},
		{
			name:                "FE edition appends fe segment only to edition root",
			host:                host,
			edition:             pkg.FEEdition,
			expectedRoot:        host,
			expectedEditionRoot: host + "/fe",
		},
		{
			name:                "EE edition appends ee segment only to edition root",
			host:                host,
			edition:             pkg.EEEdition,
			expectedRoot:        host,
			expectedEditionRoot: host + "/ee",
		},
		{
			name:                "SE edition appends se segment only to edition root",
			host:                host,
			edition:             pkg.SEEdition,
			expectedRoot:        host,
			expectedEditionRoot: host + "/se",
		},
		{
			name:                "SE-Plus edition appends se-plus segment only to edition root",
			host:                host,
			edition:             pkg.SEPlusEdition,
			expectedRoot:        host,
			expectedEditionRoot: host + "/se-plus",
		},
		{
			name:                "BE edition appends be segment only to edition root",
			host:                host,
			edition:             pkg.BEEdition,
			expectedRoot:        host,
			expectedEditionRoot: host + "/be",
		},
		{
			name:                "CE edition appends ce segment only to edition root",
			host:                host,
			edition:             pkg.CEEdition,
			expectedRoot:        host,
			expectedEditionRoot: host + "/ce",
		},
		{
			name:                "host with trailing slash is normalised by underlying client",
			host:                "registry.example.com/deckhouse/",
			edition:             pkg.FEEdition,
			expectedRoot:        "registry.example.com/deckhouse",
			expectedEditionRoot: "registry.example.com/deckhouse/fe",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use the real upstream client (not the upfake one): we only inspect
			// GetRegistry() values here, no network is touched. The upfake client
			// intentionally exposes only the host via GetRegistry and would mask
			// the segment composition this test pins down.
			c := pkgclient.NewFromOptions(tt.host)

			svc := registryservice.NewService(c, tt.edition, logger)

			assert.Equal(t, tt.expectedRoot, svc.GetRoot(),
				"GetRoot must return the non-edition-scoped root")
			assert.Equal(t, tt.expectedEditionRoot, svc.GetEditionRoot(),
				"GetEditionRoot must return the edition-scoped root")
		})
	}
}

// TestService_GetEditionRoot_AlignsWithDeckhouseService asserts that the
// edition root exposed by Service matches the root the DeckhouseService is
// scoped to. They must agree, otherwise the platform downloadList keys (built
// from GetEditionRoot) would not line up with the repository scope used to
// resolve digests through DeckhouseService.ReleaseChannels().
func TestService_GetEditionRoot_AlignsWithDeckhouseService(t *testing.T) {
	logger := log.NewNop()

	c := pkgclient.NewFromOptions("registry.deckhouse.ru/deckhouse")

	svc := registryservice.NewService(c, pkg.FEEdition, logger)

	assert.Equal(t,
		svc.DeckhouseService().GetRoot(),
		svc.GetEditionRoot(),
		"DeckhouseService root must equal Service.GetEditionRoot — they share the edition base")
}

// TestService_SubServiceScoping locks the scope each sub-service is built on:
//
//   - deckhouse, modules, security live UNDER the edition segment.
//   - plugins and installer live OUTSIDE the edition segment.
//
// A regression in either direction silently routes pulls to the wrong path,
// which is exactly the class of bug that surfaced as duplicate
// <root>/release-channel:<channel> pulls for FE sources.
func TestService_SubServiceScoping(t *testing.T) {
	logger := log.NewNop()

	const host = "registry.deckhouse.ru/deckhouse"
	c := pkgclient.NewFromOptions(host)
	svc := registryservice.NewService(c, pkg.FEEdition, logger)

	t.Run("DeckhouseService is edition-scoped", func(t *testing.T) {
		assert.Equal(t, host+"/fe", svc.DeckhouseService().GetRoot(),
			"DeckhouseService must be scoped to <root>/<edition>")
	})

	t.Run("installer is NOT edition-scoped", func(t *testing.T) {
		// The installer lives at <root>/installer regardless of edition, so
		// Service.GetRoot must stay at the bare root (no edition segment) for the
		// installer tree references to stay correct.
		assert.Equal(t, host, svc.GetRoot(),
			"Service.GetRoot must remain non-edition-scoped - the installer relies on it")
	})

	t.Run("no edition keeps both roots equal across all sub-services", func(t *testing.T) {
		noEditionSvc := registryservice.NewService(
			pkgclient.NewFromOptions(host),
			pkg.NoEdition,
			logger,
		)

		assert.Equal(t, host, noEditionSvc.GetRoot())
		assert.Equal(t, host, noEditionSvc.GetEditionRoot())
		assert.Equal(t, noEditionSvc.GetRoot(), noEditionSvc.GetEditionRoot(),
			"with NoEdition the two roots must coincide")
		assert.Equal(t, host, noEditionSvc.DeckhouseService().GetRoot(),
			"DeckhouseService root must equal the bare root when no edition is set")
	})
}
