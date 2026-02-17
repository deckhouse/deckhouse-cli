/*
Copyright 2025 Flant JSC

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
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	"github.com/deckhouse/deckhouse-cli/pkg/stub"
)

func TestService_versionsToMirror(t *testing.T) {
	tests := []struct {
		name            string
		strictTags      []string
		options         *Options
		wantVersions    []string // expected versions in format "v1.72.10", etc.
		wantChannels    []string // expected channels
		wantCustomTags  []string // expected custom tags
		wantErr         bool
		wantErrContains string
	}{
		{
			name:       "with strict tags - single semver version",
			strictTags: []string{"v1.72.10"},
			options:    &Options{},
			wantVersions: []string{
				"v1.72.10",
			},
			// v1.72.10 matches only alpha channel version
			wantChannels:   []string{"alpha"},
			wantCustomTags: []string{},
			wantErr:        false,
		},
		{
			name:       "with strict tags - multiple semver versions",
			strictTags: []string{"v1.72.10", "v1.71.0", "v1.70.0"},
			options:    &Options{},
			wantVersions: []string{
				"v1.72.10",
				"v1.71.0",
				"v1.70.0",
			},
			// v1.72.10 matches alpha, v1.71.0 matches beta, v1.70.0 matches early-access
			wantChannels: []string{
				"alpha", "beta", "early-access",
			},
			wantCustomTags: []string{},
			wantErr:        false,
		},
		{
			name:       "with strict tags - channel name",
			strictTags: []string{"stable"},
			options:    &Options{},
			wantVersions: []string{
				"v1.69.0", // stable channel version
			},
			wantChannels: []string{
				"stable",
			},
			wantCustomTags: []string{},
			wantErr:        false,
		},
		{
			name:         "with strict tags - custom tag",
			strictTags:   []string{"pr12345"},
			options:      &Options{},
			wantVersions: []string{
				// No semver versions for custom tag
			},
			wantChannels: []string{
				// No channels matched
			},
			wantCustomTags: []string{
				"pr12345", // custom tag is returned as-is
			},
			wantErr: false,
		},
		{
			name:       "no strict tags - full discovery",
			strictTags: []string{},
			options:    &Options{},
			// When no strict tags, it returns all channel versions
			wantVersions: []string{
				"v1.72.10", // alpha
				"v1.71.0",  // beta
				"v1.70.0",  // early-access
				"v1.69.0",  // stable
				"v1.68.0",  // rock-solid
			},
			wantChannels: []string{
				"alpha", "beta", "early-access", "stable", "rock-solid",
			},
			wantCustomTags: []string{},
			wantErr:        false,
		},
		{
			name:       "no strict tags with SinceVersion",
			strictTags: []string{},
			options: &Options{
				SinceVersion: semver.MustParse("v1.69.0"),
			},
			// When no strict tags, it returns all channel versions
			wantVersions: []string{
				"v1.72.10", // alpha
				"v1.71.0",  // beta
				"v1.70.0",  // early-access
				"v1.69.0",  // stable
				"v1.68.0",  // rock-solid
			},
			wantChannels: []string{
				"alpha", "beta", "early-access", "stable", "rock-solid",
			},
			wantCustomTags: []string{},
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create stub registry client
			stubClient := stub.NewRegistryClientStub()

			// Create logger
			logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelInfo))
			userLogger := log.NewSLogger(slog.LevelInfo)

			// Create DeckhouseService with stub client
			deckhouseService := registryservice.NewDeckhouseService(stubClient, logger)

			// Create Service instance
			svc := &Service{
				deckhouseService: deckhouseService,
				downloadList:     NewImageDownloadList(stubClient.GetRegistry()),
				options:          tt.options,
				logger:           logger,
				userLogger:       userLogger,
			}

			// Call the function under test
			result, err := svc.versionsToMirror(context.Background(), tt.strictTags)

			// Check error
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrContains != "" {
					assert.Contains(t, err.Error(), tt.wantErrContains)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)

			// Convert versions to strings for easier comparison
			gotVersions := make([]string, len(result.Versions))
			for i, v := range result.Versions {
				gotVersions[i] = "v" + v.String()
			}

			// Check versions (order doesn't matter for this test)
			assert.ElementsMatch(t, tt.wantVersions, gotVersions, "versions mismatch")

			// Check channels (order doesn't matter)
			assert.ElementsMatch(t, tt.wantChannels, result.Channels, "channels mismatch")

			// Check custom tags (order doesn't matter)
			assert.ElementsMatch(t, tt.wantCustomTags, result.CustomTags, "custom tags mismatch")
		})
	}
}

func TestService_versionsToMirror_WithTargetTag(t *testing.T) {
	// Create stub registry client
	stubClient := stub.NewRegistryClientStub()

	// Create logger
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelInfo))
	userLogger := log.NewSLogger(slog.LevelInfo)

	// Create DeckhouseService with stub client
	deckhouseService := registryservice.NewDeckhouseService(stubClient, logger)

	// Create Service instance with TargetTag
	svc := &Service{
		deckhouseService: deckhouseService,
		downloadList:     NewImageDownloadList(stubClient.GetRegistry()),
		options: &Options{
			TargetTag: "v1.72.10",
		},
		logger:     logger,
		userLogger: userLogger,
	}

	// Call the function under test
	result, err := svc.versionsToMirror(context.Background(), []string{"v1.72.10"})
	require.NoError(t, err)
	require.NotNil(t, result)

	// Convert versions to strings for easier comparison
	gotVersions := make([]string, len(result.Versions))
	for i, v := range result.Versions {
		gotVersions[i] = "v" + v.String()
	}

	// Should only return the specific tag
	assert.ElementsMatch(t, []string{"v1.72.10"}, gotVersions)
	// Channels should match the version (alpha channel has v1.72.10)
	assert.ElementsMatch(t, []string{"alpha"}, result.Channels)
	// No custom tags
	assert.Empty(t, result.CustomTags)
}

func TestService_versionsToMirror_CustomTagWithSemver(t *testing.T) {
	// Create stub registry client
	stubClient := stub.NewRegistryClientStub()

	// Create logger
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelInfo))
	userLogger := log.NewSLogger(slog.LevelInfo)

	// Create DeckhouseService with stub client
	deckhouseService := registryservice.NewDeckhouseService(stubClient, logger)

	// Create Service instance
	svc := &Service{
		deckhouseService: deckhouseService,
		downloadList:     NewImageDownloadList(stubClient.GetRegistry()),
		options:          &Options{},
		logger:           logger,
		userLogger:       userLogger,
	}

	// Call with mix of semver version and custom tag
	strictTags := []string{"v1.72.10", "pr12345"}
	result, err := svc.versionsToMirror(context.Background(), strictTags)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Convert versions to strings for easier comparison
	gotVersions := make([]string, len(result.Versions))
	for i, v := range result.Versions {
		gotVersions[i] = "v" + v.String()
	}

	// Should have both semver version and custom tag
	assert.Contains(t, gotVersions, "v1.72.10", "should include semver version")
	assert.Contains(t, result.CustomTags, "pr12345", "should include custom tag")
	assert.Contains(t, result.Channels, "alpha", "should include alpha channel")
}

func TestService_versionsToMirror_Deduplication(t *testing.T) {
	// Create stub registry client
	stubClient := stub.NewRegistryClientStub()

	// Create logger
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelInfo))
	userLogger := log.NewSLogger(slog.LevelInfo)

	// Create DeckhouseService with stub client
	deckhouseService := registryservice.NewDeckhouseService(stubClient, logger)

	// Create Service instance
	svc := &Service{
		deckhouseService: deckhouseService,
		downloadList:     NewImageDownloadList(stubClient.GetRegistry()),
		options:          &Options{},
		logger:           logger,
		userLogger:       userLogger,
	}

	// Call with duplicate versions in strictTags
	strictTags := []string{"v1.72.10", "v1.72.10", "alpha"} // alpha also points to v1.72.10
	result, err := svc.versionsToMirror(context.Background(), strictTags)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Convert versions to strings for easier comparison
	gotVersions := make([]string, len(result.Versions))
	for i, v := range result.Versions {
		gotVersions[i] = "v" + v.String()
	}

	// Should deduplicate
	assert.Equal(t, 1, len(gotVersions), "expected deduplicated versions")
	assert.Contains(t, gotVersions, "v1.72.10")

	// Channels should include alpha
	assert.Contains(t, result.Channels, "alpha")

	// No custom tags
	assert.Empty(t, result.CustomTags)
}
