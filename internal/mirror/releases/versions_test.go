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

package releases

import (
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/require"
)

func TestParseAndFilterVersionsAboveMinimalAnbBelowAlpha(t *testing.T) {
	minVersion := semver.MustParse("v1.50.0")
	alphaVersion := semver.MustParse("v1.60.0")

	tests := []struct {
		name     string
		tags     []string
		expected []string
	}{
		{
			name:     "empty tags",
			tags:     []string{},
			expected: []string{},
		},
		{
			name:     "tags below minimum",
			tags:     []string{"v1.49.0", "v1.48.0"},
			expected: []string{},
		},
		{
			name:     "tags above alpha",
			tags:     []string{"v1.61.0", "v1.62.0"},
			expected: []string{},
		},
		{
			name:     "tags in range",
			tags:     []string{"v1.50.0", "v1.51.0", "v1.52.0", "v1.59.0"},
			expected: []string{"v1.50.0", "v1.51.0", "v1.52.0", "v1.59.0"},
		},
		{
			name:     "mixed valid and invalid tags",
			tags:     []string{"v1.49.0", "v1.50.0", "invalid", "v1.51.0", "v1.61.0"},
			expected: []string{"v1.50.0", "v1.51.0"},
		},
		{
			name:     "exact boundary values",
			tags:     []string{"v1.50.0", "v1.60.0"},
			expected: []string{"v1.50.0", "v1.60.0"}, // alpha version is included (not GreaterThan)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseAndFilterVersionsAboveMinimalAnbBelowAlpha(minVersion, tt.tags, alphaVersion)

			resultStrs := make([]string, len(result))
			for i, v := range result {
				resultStrs[i] = "v" + v.String()
			}

			require.Equal(t, tt.expected, resultStrs)
		})
	}
}

func TestFilterOnlyLatestPatches(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "empty input",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "single version",
			input:    []string{"v1.50.0"},
			expected: []string{"v1.50.0"},
		},
		{
			name:     "multiple patches same major.minor",
			input:    []string{"v1.50.0", "v1.50.1", "v1.50.2", "v1.50.3"},
			expected: []string{"v1.50.3"},
		},
		{
			name:     "different major.minor versions",
			input:    []string{"v1.50.0", "v1.51.0", "v1.52.0", "v2.0.0"},
			expected: []string{"v1.50.0", "v1.51.0", "v1.52.0", "v2.0.0"},
		},
		{
			name:     "mixed patches",
			input:    []string{"v1.50.0", "v1.50.1", "v1.51.0", "v1.51.2", "v1.51.1"},
			expected: []string{"v1.50.1", "v1.51.2"},
		},
		{
			name:     "unsorted input",
			input:    []string{"v1.51.1", "v1.50.0", "v1.51.0", "v1.50.2"},
			expected: []string{"v1.50.2", "v1.51.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputVersions := make([]*semver.Version, len(tt.input))
			for i, v := range tt.input {
				inputVersions[i] = semver.MustParse(v)
			}

			result := FilterOnlyLatestPatches(inputVersions)

			resultStrs := make([]string, len(result))
			for i, v := range result {
				resultStrs[i] = "v" + v.String()
			}

			// Sort both slices for comparison since map iteration order is not guaranteed
			require.ElementsMatch(t, tt.expected, resultStrs)
		})
	}
}

func TestDeduplicateVersions(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "empty input",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "no duplicates",
			input:    []string{"v1.50.0", "v1.51.0", "v1.52.0"},
			expected: []string{"v1.50.0", "v1.51.0", "v1.52.0"},
		},
		{
			name:     "with duplicates",
			input:    []string{"v1.50.0", "v1.51.0", "v1.50.0", "v1.51.0", "v1.52.0"},
			expected: []string{"v1.50.0", "v1.51.0", "v1.52.0"},
		},
		{
			name:     "all duplicates",
			input:    []string{"v1.50.0", "v1.50.0", "v1.50.0"},
			expected: []string{"v1.50.0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputVersions := make([]*semver.Version, len(tt.input))
			for i, v := range tt.input {
				inputVersions[i] = semver.MustParse(v)
			}

			result := deduplicateVersions(inputVersions)

			resultStrs := make([]string, len(result))
			for i, v := range result {
				resultStrs[i] = "v" + v.String()
			}

			require.ElementsMatch(t, tt.expected, resultStrs)
		})
	}
}

// Benchmark tests
func BenchmarkParseAndFilterVersionsAboveMinimalAnbBelowAlpha(b *testing.B) {
	minVersion := semver.MustParse("v1.50.0")
	alphaVersion := semver.MustParse("v1.60.0")
	tags := []string{"v1.49.0", "v1.50.0", "v1.51.0", "v1.52.0", "v1.59.0", "v1.60.0", "v1.61.0"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseAndFilterVersionsAboveMinimalAnbBelowAlpha(minVersion, tags, alphaVersion)
	}
}

func BenchmarkFilterOnlyLatestPatches(b *testing.B) {
	versions := []*semver.Version{
		semver.MustParse("v1.50.0"),
		semver.MustParse("v1.50.1"),
		semver.MustParse("v1.51.0"),
		semver.MustParse("v1.51.2"),
		semver.MustParse("v1.52.0"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FilterOnlyLatestPatches(versions)
	}
}

func BenchmarkDeduplicateVersions(b *testing.B) {
	versions := []*semver.Version{
		semver.MustParse("v1.50.0"),
		semver.MustParse("v1.51.0"),
		semver.MustParse("v1.50.0"),
		semver.MustParse("v1.51.0"),
		semver.MustParse("v1.52.0"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deduplicateVersions(versions)
	}
}
