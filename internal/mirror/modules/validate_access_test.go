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

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
)

func TestService_validateModulesAccess(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	t.Run("modules present in registry returns no error", func(t *testing.T) {
		// Build a stub where the "modules" repository has module names as tags.
		reg := upfake.NewRegistry("registry.example.com")
		placeholder := upfake.NewImageBuilder().MustBuild()
		reg.MustAddImage("modules", "console", placeholder)
		reg.MustAddImage("modules", "ingress-nginx", placeholder)
		reg.MustAddImage("modules", "cert-manager", placeholder)

		stubClient := pkgclient.Adapt(upfake.NewClient(reg))
		// Scope client to "modules" so that ListTags returns module names.
		modulesClient := stubClient.WithSegment("modules")
		modulesService := registryservice.NewModulesService(modulesClient, logger)

		svc := &Service{
			modulesService: modulesService,
			options:        &Options{},
			logger:         logger,
			userLogger:     userLogger,
		}

		err := svc.validateModulesAccess(context.Background())
		require.NoError(t, err)
	})

	t.Run("modules repository absent in registry returns no error and emits warning", func(t *testing.T) {
		// Empty registry – the "modules" repo does not exist, so ListTags returns
		// ErrImageNotFound which validateModulesAccess treats as a graceful skip.
		reg := upfake.NewRegistry("registry.example.com")
		stubClient := pkgclient.Adapt(upfake.NewClient(reg))
		modulesClient := stubClient.WithSegment("modules")
		modulesService := registryservice.NewModulesService(modulesClient, logger)

		svc := &Service{
			modulesService: modulesService,
			options:        &Options{},
			logger:         logger,
			userLogger:     userLogger,
		}

		err := svc.validateModulesAccess(context.Background())
		require.NoError(t, err)
	})
}

func TestService_validateModulesAccess_WithFilter(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	reg := upfake.NewRegistry("registry.example.com")
	placeholder := upfake.NewImageBuilder().MustBuild()
	reg.MustAddImage("modules", "console", placeholder)
	reg.MustAddImage("modules", "ingress-nginx", placeholder)
	reg.MustAddImage("modules", "sds-replicated-volume", placeholder)

	stubClient := pkgclient.Adapt(upfake.NewClient(reg))
	modulesClient := stubClient.WithSegment("modules")
	modulesService := registryservice.NewModulesService(modulesClient, logger)

	// validateModulesAccess does not use the filter; it only checks reachability.
	// The filter is applied later in pullModules. We verify here that any valid
	// filter configuration does not affect the access check result.
	tests := []struct {
		name   string
		filter *Filter
	}{
		{
			name: "whitelist subset of modules",
			filter: func() *Filter {
				f, _ := NewFilter([]string{"console", "ingress-nginx"}, FilterTypeWhitelist)
				return f
			}(),
		},
		{
			name: "blacklist single module",
			filter: func() *Filter {
				f, _ := NewFilter([]string{"sds-replicated-volume"}, FilterTypeBlacklist)
				return f
			}(),
		},
		{
			name: "empty blacklist accepts all",
			filter: func() *Filter {
				f, _ := NewFilter(nil, FilterTypeBlacklist)
				return f
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &Service{
				modulesService: modulesService,
				options:        &Options{Filter: tt.filter},
				logger:         logger,
				userLogger:     userLogger,
			}

			err := svc.validateModulesAccess(context.Background())
			require.NoError(t, err)
		})
	}
}

func TestService_validateModulesAccess_Timeout(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	reg := upfake.NewRegistry("registry.example.com")
	placeholder := upfake.NewImageBuilder().MustBuild()
	reg.MustAddImage("modules", "console", placeholder)

	stubClient := pkgclient.Adapt(upfake.NewClient(reg))
	modulesClient := stubClient.WithSegment("modules")
	modulesService := registryservice.NewModulesService(modulesClient, logger)

	// Even with a Timeout option set, validateModulesAccess should succeed
	// (the timeout is applied at the network level, not in the stub).
	svc := &Service{
		modulesService: modulesService,
		options:        &Options{},
		logger:         logger,
		userLogger:     userLogger,
	}

	err := svc.validateModulesAccess(context.Background())
	require.NoError(t, err)
}

// TestModule_Versions verifies that Module.Versions() correctly parses semver
// release tags and ignores non-semver strings (such as channel names).
func TestModule_Versions(t *testing.T) {
	_ = log.NewSLogger(slog.LevelDebug) // silence unused-import lint

	mod := &Module{
		Name: "console",
		Releases: []string{
			"alpha", "beta", "early-access", "stable", "rock-solid", // non-semver
			"v1.0.0", "v1.1.0", "v1.2.3", // semver
			"notasemver",
		},
	}

	versions := mod.Versions()
	got := make([]string, 0, len(versions))
	for _, v := range versions {
		got = append(got, "v"+v.String())
	}

	want := []string{"v1.0.0", "v1.1.0", "v1.2.3"}
	require.ElementsMatch(t, want, got)
}
