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

package mirror

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPluginsRootPath: the CLI artifacts root is the push target without its
// trailing edition segment. The rule must match the one the
// registry-packages-proxy applies to the cluster's imagesRepo, including the
// cases where nothing is cut.
func TestPluginsRootPath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		target      string
		wantRoot    string
		wantEdition string
	}{
		// An edition segment is cut off.
		{"deckhouse/ee", "deckhouse", "ee"},
		{"deckhouse/ce", "deckhouse", "ce"},
		{"deckhouse/se-plus", "deckhouse", "se-plus"},
		{"dkp/ee", "dkp", "ee"},
		{"mirror/deckhouse/fe", "mirror/deckhouse", "fe"},
		// No edition segment: the target is the root already.
		{"deckhouse", "deckhouse", ""},
		{"sys/deckhouse-oss", "sys/deckhouse-oss", ""},
		{"deckhouse/ee-mirror", "deckhouse/ee-mirror", ""},
		{"", "", ""},
		// CSE keeps its editionless artifacts under deckhouse/cse, so that
		// path is a root of its own.
		{"deckhouse/cse", "deckhouse/cse", ""},
		// The root keeps at least one path segment: "ee" alone names a
		// project, and cutting it would push plugins to the registry host.
		{"ee", "ee", ""},
		{"/se/", "/se/", ""},
	}

	for _, tc := range cases {
		t.Run(tc.target, func(t *testing.T) {
			t.Parallel()

			root, edition := pluginsRootPath(tc.target)
			assert.Equal(t, tc.wantRoot, root, "root")
			assert.Equal(t, tc.wantEdition, edition, "edition")
		})
	}
}
