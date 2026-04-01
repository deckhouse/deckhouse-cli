/*
Copyright 2024 Flant JSC

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
	"fmt"
	"testing"

	"github.com/google/go-containerregistry/pkg/crane"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestGenerateDeckhouseRelease(t *testing.T) {
	tests := []struct {
		name        string
		versionTag  string
		releaseInfo *releaseInfo
		wantErr     bool
		assertYAML  func(t *testing.T, got string)
	}{
		{
			name:       "without disruptions",
			versionTag: "v1.57.3",
			releaseInfo: &releaseInfo{
				Requirements: map[string]string{
					"k8s":          "1.23.0",
					"ingressNginx": "1.1",
				},
				Changelog: map[string]any{
					"candi": map[string]any{
						"fixes": []any{
							map[string]any{"summary": "Some fix"},
						},
					},
				},
			},
			assertYAML: func(t *testing.T, got string) {
				assert.Contains(t, got, "kind: DeckhouseRelease")
				assert.Contains(t, got, "name: v1.57.3")
				assert.Contains(t, got, "version: v1.57.3")
				assert.Contains(t, got, "changelogLink: https://github.com/deckhouse/deckhouse/releases/tag/v1.57.3")
				assert.NotContains(t, got, "disruptions")
			},
		},
		{
			name:       "with disruptions matching version minor",
			versionTag: "v1.56.12",
			releaseInfo: &releaseInfo{
				Requirements: map[string]string{"k8s": "1.23.0"},
				Disruptions: map[string][]string{
					"1.56": {"ingressNginx"},
				},
			},
			assertYAML: func(t *testing.T, got string) {
				assert.Contains(t, got, "name: v1.56.12")
				assert.Contains(t, got, "disruptions:")
				assert.Contains(t, got, "- ingressNginx")
			},
		},
		{
			name:       "disruptions for different minor version are ignored",
			versionTag: "v1.57.3",
			releaseInfo: &releaseInfo{
				Requirements: map[string]string{"k8s": "1.23.0"},
				Disruptions: map[string][]string{
					"1.56": {"ingressNginx"},
				},
			},
			assertYAML: func(t *testing.T, got string) {
				assert.NotContains(t, got, "disruptions")
			},
		},
		{
			name:       "invalid version tag",
			versionTag: "not-a-version",
			releaseInfo: &releaseInfo{
				Requirements: map[string]string{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := generateDeckhouseRelease(tt.versionTag, tt.releaseInfo)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			tt.assertYAML(t, string(got))
		})
	}
}

func TestExtractReleaseInfoForDeckhouseRelease(t *testing.T) {
	t.Run("extracts version info and changelog", func(t *testing.T) {
		img := createReleaseImage(t, "1.56.12")

		info, err := extractReleaseInfoForDeckhouseRelease(img)
		require.NoError(t, err)

		assert.Equal(t, "v1.56.12", info.Requirements["version"])
		assert.Equal(t, "1.23.0", info.Requirements["k8s"])
		assert.Equal(t, []string{"ingressNginx"}, info.Disruptions["1.56"])
		assert.Contains(t, info.Changelog, "candi")
	})
}

func createReleaseImage(t *testing.T, version string) v1.Image {
	t.Helper()

	changelog, err := yaml.JSONToYAML([]byte(
		`{"candi":{"fixes":[{"summary":"Fix containerd start.","pull_request":"https://github.com/deckhouse/deckhouse/pull/6329"}]}}`,
	))
	require.NoError(t, err)

	versionInfo := fmt.Sprintf(
		`{"disruptions":{"1.56":["ingressNginx"]},"requirements":{"k8s":"1.23.0","version":%q}}`,
		"v"+version,
	)

	l, err := crane.Layer(map[string][]byte{
		"version.json":   []byte(versionInfo),
		"changelog.yaml": changelog,
	})
	require.NoError(t, err)

	img, err := mutate.AppendLayers(empty.Image, l)
	require.NoError(t, err)
	return img
}
