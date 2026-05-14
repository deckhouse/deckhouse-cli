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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg"
	registryclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

func TestNewService_RootScopes(t *testing.T) {
	tests := []struct {
		name               string
		registryRoot       string
		edition            pkg.Edition
		wantServiceRoot    string
		wantDeckhouseRoot  string
	}{
		{
			name:              "edition layout keeps root split",
			registryRoot:      "registry.example.com/deckhouse",
			edition:           pkg.FEEdition,
			wantServiceRoot:   "registry.example.com/deckhouse",
			wantDeckhouseRoot: "registry.example.com/deckhouse/fe",
		},
		{
			name:              "flat layout keeps same root for both services",
			registryRoot:      "registry.example.com/deckhouse",
			edition:           pkg.NoEdition,
			wantServiceRoot:   "registry.example.com/deckhouse",
			wantDeckhouseRoot: "registry.example.com/deckhouse",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
			client := registryclient.NewFromOptions(tc.registryRoot)

			svc := registryservice.NewService(client, tc.edition, logger)

			require.Equal(t, tc.wantServiceRoot, svc.GetRoot())
			require.Equal(t, tc.wantDeckhouseRoot, svc.DeckhouseService().GetRoot())
		})
	}
}
