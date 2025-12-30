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

package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterByPrefix(t *testing.T) {
	tests := []struct {
		name     string
		items    []string
		prefix   string
		expected []string
	}{
		{
			name:     "empty prefix returns all",
			items:    []string{"alpha", "beta", "gamma"},
			prefix:   "",
			expected: []string{"alpha", "beta", "gamma"},
		},
		{
			name:     "empty items returns empty",
			items:    []string{},
			prefix:   "a",
			expected: nil,
		},
		{
			name:     "matching prefix",
			items:    []string{"alpha", "apex", "beta"},
			prefix:   "a",
			expected: []string{"alpha", "apex"},
		},
		{
			name:     "exact match",
			items:    []string{"alpha", "beta", "gamma"},
			prefix:   "alpha",
			expected: []string{"alpha"},
		},
		{
			name:     "no matches",
			items:    []string{"alpha", "beta", "gamma"},
			prefix:   "delta",
			expected: nil,
		},
		{
			name:     "case sensitive",
			items:    []string{"Alpha", "alpha", "ALPHA"},
			prefix:   "a",
			expected: []string{"alpha"},
		},
		{
			name:     "prefix longer than items",
			items:    []string{"a", "ab", "abc"},
			prefix:   "abcd",
			expected: nil,
		},
		{
			name:     "module names with dashes",
			items:    []string{"csi-hpe", "csi-nfs", "prometheus"},
			prefix:   "csi",
			expected: []string{"csi-hpe", "csi-nfs"},
		},
		{
			name:     "module names exact prefix with dash",
			items:    []string{"csi-hpe", "csi-nfs", "csi"},
			prefix:   "csi-",
			expected: []string{"csi-hpe", "csi-nfs"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterByPrefix(tt.items, tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFilterVersionsByPrefix(t *testing.T) {
	tests := []struct {
		name     string
		versions []string
		prefix   string
		expected []string
	}{
		{
			name:     "empty prefix returns all versions as-is",
			versions: []string{"v1.0.0", "v2.0.0"},
			prefix:   "",
			expected: []string{"v1.0.0", "v2.0.0"},
		},
		{
			name:     "empty versions returns empty",
			versions: []string{},
			prefix:   "v1",
			expected: nil,
		},
		{
			name:     "prefix with v returns versions with v",
			versions: []string{"v1.0.0", "v1.1.0", "v2.0.0"},
			prefix:   "v1",
			expected: []string{"v1.0.0", "v1.1.0"},
		},
		{
			name:     "prefix without v returns versions without v",
			versions: []string{"v1.0.0", "v1.1.0", "v2.0.0"},
			prefix:   "1",
			expected: []string{"1.0.0", "1.1.0"},
		},
		{
			name:     "prefix v1.0 narrows down",
			versions: []string{"v1.0.0", "v1.0.1", "v1.1.0"},
			prefix:   "v1.0",
			expected: []string{"v1.0.0", "v1.0.1"},
		},
		{
			name:     "prefix 1.0 narrows down without v",
			versions: []string{"v1.0.0", "v1.0.1", "v1.1.0"},
			prefix:   "1.0",
			expected: []string{"1.0.0", "1.0.1"},
		},
		{
			name:     "exact version match with v",
			versions: []string{"v1.0.0", "v1.1.0"},
			prefix:   "v1.0.0",
			expected: []string{"v1.0.0"},
		},
		{
			name:     "exact version match without v",
			versions: []string{"v1.0.0", "v1.1.0"},
			prefix:   "1.0.0",
			expected: []string{"1.0.0"},
		},
		{
			name:     "no matches",
			versions: []string{"v1.0.0", "v2.0.0"},
			prefix:   "v3",
			expected: nil,
		},
		{
			name:     "versions without v prefix in source",
			versions: []string{"1.0.0", "1.1.0", "2.0.0"},
			prefix:   "v1",
			expected: []string{"v1.0.0", "v1.1.0"},
		},
		{
			name:     "versions without v prefix user types without v",
			versions: []string{"1.0.0", "1.1.0", "2.0.0"},
			prefix:   "1",
			expected: []string{"1.0.0", "1.1.0"},
		},
		{
			name:     "mixed v and no-v versions",
			versions: []string{"v1.0.0", "2.0.0", "v3.0.0"},
			prefix:   "v",
			expected: []string{"v1.0.0", "v2.0.0", "v3.0.0"},
		},
		{
			name:     "patch version filtering",
			versions: []string{"v1.0.0", "v1.0.1", "v1.0.10", "v1.0.2"},
			prefix:   "v1.0.1",
			expected: []string{"v1.0.1", "v1.0.10"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterVersionsByPrefix(tt.versions, tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFilterVersionsByPrefixConsistency(t *testing.T) {
	// Test that the function maintains consistency between input format and output format
	versions := []string{"v1.0.0", "v1.1.0", "v2.0.0"}

	t.Run("user starts with v gets v back", func(t *testing.T) {
		result := filterVersionsByPrefix(versions, "v")
		for _, v := range result {
			assert.True(t, len(v) > 0 && v[0] == 'v', "expected version to start with v: %s", v)
		}
	})

	t.Run("user starts without v gets no v back", func(t *testing.T) {
		result := filterVersionsByPrefix(versions, "1")
		for _, v := range result {
			assert.True(t, len(v) > 0 && v[0] != 'v', "expected version to not start with v: %s", v)
		}
	})
}
