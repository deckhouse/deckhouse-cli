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
	"testing"

	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

func TestNewService_UsesDeckhouseRoot(t *testing.T) {
	tests := []struct {
		name        string
		edition     pkg.Edition
		wantRootURL string
	}{
		{
			name:        "edition layout uses edition-scoped root",
			edition:     pkg.FEEdition,
			wantRootURL: "registry.example.com/deckhouse/fe",
		},
		{
			name:        "flat layout uses root without edition",
			edition:     pkg.NoEdition,
			wantRootURL: "registry.example.com/deckhouse",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
			userLogger := log.NewSLogger(slog.LevelWarn)
			client := registryclient.NewFromOptions("registry.example.com/deckhouse")

			regSvc := registryservice.NewService(client, tc.edition, logger)
			svc := NewService(regSvc, t.TempDir(), &Options{DryRun: true}, logger, userLogger)

			require.Equal(t, tc.wantRootURL, svc.downloadList.rootURL)
		})
	}
}
