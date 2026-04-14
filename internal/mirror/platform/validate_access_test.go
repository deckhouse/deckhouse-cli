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

package platform

import (
	"context"
	"log/slog"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	localreg "github.com/deckhouse/deckhouse/pkg/registry"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"
	localfake "github.com/deckhouse/deckhouse-cli/pkg/fake"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
)

// newTestPlatformService is a test helper that builds a Service with only the
// fields required by validatePlatformAccess and findTagsToMirror.
func newTestPlatformService(
	stubClient localreg.Client,
	options *Options,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *Service {
	return &Service{
		deckhouseService: registryservice.NewDeckhouseService(stubClient, logger),
		downloadList:     NewImageDownloadList(stubClient.GetRegistry()),
		options:          options,
		logger:           logger,
		userLogger:       userLogger,
	}
}

// ltsOnlyStub returns a stub registry that has only the LTS channel in
// release-channel/, simulating CSE-edition registries lacking standard channels.
func ltsOnlyStub() localreg.Client {
	reg := upfake.NewRegistry("registry.deckhouse.ru/deckhouse/fe")
	img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.68.0"}`).
		MustBuild()
	reg.MustAddImage("release-channel", "lts", img)
	return pkgclient.Adapt(upfake.NewClient(reg))
}

// emptyStub returns a stub registry with no release channels at all.
func emptyStub() localreg.Client {
	return pkgclient.Adapt(upfake.NewClient(upfake.NewRegistry("registry.deckhouse.ru/deckhouse/fe")))
}

func TestService_validatePlatformAccess(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	tests := []struct {
		name        string
		makeClient  func() localreg.Client
		targetTag   string
		wantErr     bool
		errContains string
	}{
		{
			name:       "no target tag defaults to stable channel which exists",
			makeClient: localfake.NewRegistryClientStub,
			targetTag:  "",
			wantErr:    false,
		},
		{
			name:       "explicit channel tag exists",
			makeClient: localfake.NewRegistryClientStub,
			targetTag:  "alpha",
			wantErr:    false,
		},
		{
			name:       "early-access channel exists",
			makeClient: localfake.NewRegistryClientStub,
			targetTag:  "early-access",
			wantErr:    false,
		},
		{
			name:       "rock-solid channel exists",
			makeClient: localfake.NewRegistryClientStub,
			targetTag:  "rock-solid",
			wantErr:    false,
		},
		{
			name:       "channel not found falls back to LTS successfully",
			makeClient: ltsOnlyStub,
			targetTag:  "stable",
			wantErr:    false,
		},
		{
			name:        "channel not found and LTS also missing returns error",
			makeClient:  emptyStub,
			targetTag:   "stable",
			wantErr:     true,
			errContains: "release channel",
		},
		{
			name:        "LTS channel missing with empty registry",
			makeClient:  emptyStub,
			targetTag:   "beta",
			wantErr:     true,
			errContains: "lts",
		},
		{
			name:       "semver tag exists in root repository",
			makeClient: localfake.NewRegistryClientStub,
			targetTag:  "v1.72.10",
			wantErr:    false,
		},
		{
			name:       "another semver tag exists",
			makeClient: localfake.NewRegistryClientStub,
			targetTag:  "v1.69.0",
			wantErr:    false,
		},
		{
			name:        "semver tag not present in registry returns error",
			makeClient:  localfake.NewRegistryClientStub,
			targetTag:   "v9.99.0",
			wantErr:     true,
			errContains: "v9.99.0",
		},
		{
			name:       "custom non-channel tag exists in root repository",
			makeClient: localfake.NewRegistryClientStub,
			targetTag:  "pr12345",
			wantErr:    false,
		},
		{
			name:        "custom non-channel tag not present in registry returns error",
			makeClient:  localfake.NewRegistryClientStub,
			targetTag:   "does-not-exist",
			wantErr:     true,
			errContains: "does-not-exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := tt.makeClient()
			svc := newTestPlatformService(client, &Options{TargetTag: tt.targetTag}, logger, userLogger)

			err := svc.validatePlatformAccess(context.Background())

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestService_findTagsToMirror(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	tests := []struct {
		name         string
		options      *Options
		wantVersions []string // expected in result[0] (both "vX.Y.Z" and custom tags)
		wantChannels []string
		wantErr      bool
	}{
		{
			name:    "empty TargetTag returns all channel versions",
			options: &Options{},
			wantVersions: []string{
				"v1.72.10",
				"v1.71.0",
				"v1.70.0",
				"v1.69.0",
				"v1.68.0",
			},
			wantChannels: []string{
				"alpha", "beta", "early-access", "stable", "rock-solid",
			},
			wantErr: false,
		},
		{
			name:         "TargetTag is specific semver returns only that version",
			options:      &Options{TargetTag: "v1.72.10"},
			wantVersions: []string{"v1.72.10"},
			wantChannels: []string{"alpha"},
			wantErr:      false,
		},
		{
			name:         "TargetTag is channel name returns that channel version",
			options:      &Options{TargetTag: "stable"},
			wantVersions: []string{"v1.69.0"},
			wantChannels: []string{"stable"},
			wantErr:      false,
		},
		{
			name:         "TargetTag is custom non-semver tag passes through as-is",
			options:      &Options{TargetTag: "pr12345"},
			wantVersions: []string{"pr12345"},
			wantChannels: []string{},
			wantErr:      false,
		},
		{
			name:    "TargetTag is semver with matching channel returns version and channel",
			options: &Options{TargetTag: "v1.71.0"},
			// v1.71.0 is the beta channel version
			wantVersions: []string{"v1.71.0"},
			wantChannels: []string{"beta"},
			wantErr:      false,
		},
		{
			name: "empty TargetTag with SinceVersion filters old channels",
			options: &Options{
				SinceVersion: mustParseSemver("1.70.0"),
			},
			// rock-solid (v1.68.0) and stable (v1.69.0) are below SinceVersion 1.70.0
			wantVersions: []string{
				"v1.72.10",
				"v1.71.0",
				"v1.70.0",
			},
			wantChannels: []string{"alpha", "beta", "early-access"},
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := localfake.NewRegistryClientStub()
			svc := newTestPlatformService(client, tt.options, logger, userLogger)

			versions, channels, err := svc.findTagsToMirror(context.Background())

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			assert.ElementsMatch(t, tt.wantVersions, versions, "versions mismatch")
			assert.ElementsMatch(t, tt.wantChannels, channels, "channels mismatch")
		})
	}
}

// mustParseSemver is a test helper that panics if version parsing fails.
func mustParseSemver(v string) *semver.Version {
	ver, err := semver.NewVersion(v)
	if err != nil {
		panic("mustParseSemver: " + err.Error())
	}
	return ver
}
