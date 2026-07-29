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
	"testing"

	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// TestService_ModulesPathSuffix verifies that --modules-path-suffix moves the
// source location where pull discovers modules and builds their references.
// The default ("/modules") keeps the historical <repo>/modules layout.
func TestService_ModulesPathSuffix(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	const repoHost = "registry.example.com/deckhouse/ee"

	tests := []struct {
		name string
		// suffix is the --modules-path-suffix flag value.
		suffix string
		// sourceRepo is where modules must live in the source registry for that
		// suffix (relative to repoHost). Empty means the repo root.
		sourceRepo string
	}{
		{name: "default", suffix: "/modules", sourceRepo: "modules"},
		{name: "empty keeps default", suffix: "", sourceRepo: "modules"},
		{name: "repo root", suffix: "/", sourceRepo: ""},
		{name: "multi segment", suffix: "/custom/mods", sourceRepo: "custom/mods"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Seed the source registry with two module discovery tags at the
			// suffix-derived location and nowhere else.
			reg := upfake.NewRegistry(repoHost)
			placeholder := upfake.NewImageBuilder().MustBuild()
			reg.MustAddImage(tt.sourceRepo, "console", placeholder)
			reg.MustAddImage(tt.sourceRepo, "ingress-nginx", placeholder)

			client := pkgclient.Adapt(upfake.NewClient(reg))
			regSvc := registryservice.NewService(client, pkg.NoEdition, logger,
				registryservice.WithModulesPathSuffix(tt.suffix))
			svc := NewService(regSvc, t.TempDir(), &Options{}, logger, userLogger)

			// Discovery reads from the suffix-derived repo. If the suffix were
			// ignored, a non-default case would list the empty "modules" repo
			// and find nothing.
			names, err := svc.discoverModuleNames(context.Background())
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"console", "ingress-nginx"}, names)

			// References honor the suffix too, so they match the discovery scope.
			wantRoot := repoHost
			if tt.sourceRepo != "" {
				wantRoot += "/" + tt.sourceRepo
			}

			require.Equal(t, wantRoot+"/console", svc.moduleRegistryPath("console"))
			require.Equal(t, wantRoot+"/console/release", svc.moduleRegistryPath("console", "release"))
		})
	}
}
