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

package modulereleases

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeVersion(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty version",
			input:    "",
			expected: "",
		},
		{
			name:     "version without v prefix",
			input:    "1.2.3",
			expected: "v1.2.3",
		},
		{
			name:     "version with v prefix",
			input:    "v1.2.3",
			expected: "v1.2.3",
		},
		{
			name:     "version with uppercase V prefix",
			input:    "V1.2.3",
			expected: "vV1.2.3", // just adds v prefix since V != v
		},
		{
			name:     "prerelease version without v",
			input:    "1.2.3-alpha.1",
			expected: "v1.2.3-alpha.1",
		},
		{
			name:     "prerelease version with v",
			input:    "v1.2.3-beta.2",
			expected: "v1.2.3-beta.2",
		},
		{
			name:     "version with build metadata",
			input:    "1.2.3+build.123",
			expected: "v1.2.3+build.123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeVersion(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSortReleasesByVersion(t *testing.T) {
	tests := []struct {
		name     string
		input    []ModuleReleaseInfo
		expected []string // expected versions in order
	}{
		{
			name:     "empty releases",
			input:    []ModuleReleaseInfo{},
			expected: []string{},
		},
		{
			name: "single release",
			input: []ModuleReleaseInfo{
				{Version: "v1.0.0"},
			},
			expected: []string{"v1.0.0"},
		},
		{
			name: "already sorted",
			input: []ModuleReleaseInfo{
				{Version: "v1.0.0"},
				{Version: "v1.1.0"},
				{Version: "v2.0.0"},
			},
			expected: []string{"v1.0.0", "v1.1.0", "v2.0.0"},
		},
		{
			name: "reverse order",
			input: []ModuleReleaseInfo{
				{Version: "v2.0.0"},
				{Version: "v1.1.0"},
				{Version: "v1.0.0"},
			},
			expected: []string{"v1.0.0", "v1.1.0", "v2.0.0"},
		},
		{
			name: "mixed order with patches",
			input: []ModuleReleaseInfo{
				{Version: "v1.0.2"},
				{Version: "v1.0.0"},
				{Version: "v1.0.10"},
				{Version: "v1.0.1"},
			},
			expected: []string{"v1.0.0", "v1.0.1", "v1.0.2", "v1.0.10"},
		},
		{
			name: "versions without v prefix",
			input: []ModuleReleaseInfo{
				{Version: "2.0.0"},
				{Version: "1.0.0"},
			},
			expected: []string{"1.0.0", "2.0.0"},
		},
		{
			name: "mixed v prefix and without",
			input: []ModuleReleaseInfo{
				{Version: "v2.0.0"},
				{Version: "1.5.0"},
				{Version: "v1.0.0"},
			},
			expected: []string{"v1.0.0", "1.5.0", "v2.0.0"},
		},
		{
			name: "prerelease versions",
			input: []ModuleReleaseInfo{
				{Version: "v1.0.0"},
				{Version: "v1.0.0-alpha.1"},
				{Version: "v1.0.0-beta.1"},
			},
			expected: []string{"v1.0.0-alpha.1", "v1.0.0-beta.1", "v1.0.0"},
		},
		{
			name: "invalid versions fallback to lexicographic",
			input: []ModuleReleaseInfo{
				{Version: "not-a-version"},
				{Version: "also-not-version"},
			},
			expected: []string{"also-not-version", "not-a-version"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SortReleasesByVersion(tt.input)

			result := make([]string, len(tt.input))
			for i, r := range tt.input {
				result[i] = r.Version
			}

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFindNearestVersions(t *testing.T) {
	tests := []struct {
		name          string
		releases      []ModuleReleaseInfo
		targetVersion string
		expectLower   string
		expectUpper   string
	}{
		{
			name:          "empty releases",
			releases:      []ModuleReleaseInfo{},
			targetVersion: "v1.0.0",
			expectLower:   "",
			expectUpper:   "",
		},
		{
			name: "no lower version",
			releases: []ModuleReleaseInfo{
				{Version: "v2.0.0"},
				{Version: "v3.0.0"},
			},
			targetVersion: "v1.0.0",
			expectLower:   "",
			expectUpper:   "v2.0.0",
		},
		{
			name: "no upper version",
			releases: []ModuleReleaseInfo{
				{Version: "v0.5.0"},
				{Version: "v0.9.0"},
			},
			targetVersion: "v1.0.0",
			expectLower:   "v0.9.0",
			expectUpper:   "",
		},
		{
			name: "both lower and upper exist",
			releases: []ModuleReleaseInfo{
				{Version: "v0.9.0"},
				{Version: "v1.1.0"},
				{Version: "v2.0.0"},
			},
			targetVersion: "v1.0.0",
			expectLower:   "v0.9.0",
			expectUpper:   "v1.1.0",
		},
		{
			name: "exact version exists - find neighbors",
			releases: []ModuleReleaseInfo{
				{Version: "v0.9.0"},
				{Version: "v1.0.0"},
				{Version: "v1.1.0"},
			},
			targetVersion: "v1.0.0",
			expectLower:   "v0.9.0",
			expectUpper:   "v1.1.0",
		},
		{
			name: "finds closest lower version",
			releases: []ModuleReleaseInfo{
				{Version: "v0.1.0"},
				{Version: "v0.5.0"},
				{Version: "v0.9.0"},
			},
			targetVersion: "v1.0.0",
			expectLower:   "v0.9.0",
			expectUpper:   "",
		},
		{
			name: "finds closest upper version",
			releases: []ModuleReleaseInfo{
				{Version: "v1.1.0"},
				{Version: "v1.5.0"},
				{Version: "v2.0.0"},
			},
			targetVersion: "v1.0.0",
			expectLower:   "",
			expectUpper:   "v1.1.0",
		},
		{
			name: "target version without v prefix",
			releases: []ModuleReleaseInfo{
				{Version: "v0.9.0"},
				{Version: "v1.1.0"},
			},
			targetVersion: "1.0.0",
			expectLower:   "v0.9.0",
			expectUpper:   "v1.1.0",
		},
		{
			name: "invalid target version",
			releases: []ModuleReleaseInfo{
				{Version: "v1.0.0"},
			},
			targetVersion: "not-a-version",
			expectLower:   "",
			expectUpper:   "",
		},
		{
			name: "invalid release versions are skipped",
			releases: []ModuleReleaseInfo{
				{Version: "not-valid"},
				{Version: "v0.9.0"},
				{Version: "also-invalid"},
				{Version: "v1.1.0"},
			},
			targetVersion: "v1.0.0",
			expectLower:   "v0.9.0",
			expectUpper:   "v1.1.0",
		},
		{
			name: "patch versions",
			releases: []ModuleReleaseInfo{
				{Version: "v1.0.1"},
				{Version: "v1.0.3"},
				{Version: "v1.0.5"},
			},
			targetVersion: "v1.0.2",
			expectLower:   "v1.0.1",
			expectUpper:   "v1.0.3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FindNearestVersions(tt.releases, tt.targetVersion)

			if tt.expectLower == "" {
				assert.Nil(t, result.Lower, "expected no lower version")
			} else {
				require.NotNil(t, result.Lower, "expected lower version to exist")
				assert.Equal(t, tt.expectLower, result.Lower.Version)
			}

			if tt.expectUpper == "" {
				assert.Nil(t, result.Upper, "expected no upper version")
			} else {
				require.NotNil(t, result.Upper, "expected upper version to exist")
				assert.Equal(t, tt.expectUpper, result.Upper.Version)
			}
		})
	}
}

func TestExtractVersionFromName(t *testing.T) {
	tests := []struct {
		name       string
		relName    string
		moduleName string
		expected   string
	}{
		{
			name:       "standard format",
			relName:    "csi-hpe-v0.3.10",
			moduleName: "csi-hpe",
			expected:   "v0.3.10",
		},
		{
			name:       "module with dashes",
			relName:    "my-cool-module-v1.2.3",
			moduleName: "my-cool-module",
			expected:   "v1.2.3",
		},
		{
			name:       "module name not matching",
			relName:    "other-module-v1.0.0",
			moduleName: "csi-hpe",
			expected:   "",
		},
		{
			name:       "empty module name",
			relName:    "csi-hpe-v0.3.10",
			moduleName: "",
			expected:   "",
		},
		{
			name:       "empty release name",
			relName:    "",
			moduleName: "csi-hpe",
			expected:   "",
		},
		{
			name:       "release name equals module name",
			relName:    "csi-hpe",
			moduleName: "csi-hpe",
			expected:   "",
		},
		{
			name:       "version without v prefix",
			relName:    "mymodule-1.0.0",
			moduleName: "mymodule",
			expected:   "1.0.0",
		},
		{
			name:       "prerelease version",
			relName:    "mymodule-v1.0.0-alpha.1",
			moduleName: "mymodule",
			expected:   "v1.0.0-alpha.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractVersionFromName(tt.relName, tt.moduleName)
			assert.Equal(t, tt.expected, result)
		})
	}
}
