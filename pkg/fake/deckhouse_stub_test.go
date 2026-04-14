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

package fake_test

import (
	"archive/tar"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/pkg/fake"
)

// TestNewRegistryClientStub_ReleaseChannelTags verifies that the Deckhouse
// stub exposes all five release channels and their version tags under the
// "release-channel" repo.
func TestNewRegistryClientStub_ReleaseChannelTags(t *testing.T) {
	client := fake.NewRegistryClientStub()
	rcClient := client.WithSegment("release-channel")

	tags, err := rcClient.ListTags(context.Background())
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{
		"alpha", "beta", "early-access", "stable", "rock-solid",
		"v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0",
	}, tags)
}

// TestNewRegistryClientStub_ReleaseChannelVersionJSON verifies that the
// version.json file inside each release-channel image contains a valid version.
func TestNewRegistryClientStub_ReleaseChannelVersionJSON(t *testing.T) {
	wantVersions := map[string]string{
		"alpha":        "v1.72.10",
		"beta":         "v1.71.0",
		"early-access": "v1.70.0",
		"stable":       "v1.69.0",
		"rock-solid":   "v1.68.0",
	}

	client := fake.NewRegistryClientStub()

	for channel, wantVersion := range wantVersions {
		t.Run(channel, func(t *testing.T) {
			rcClient := client.WithSegment("release-channel")
			img, err := rcClient.GetImage(context.Background(), channel)
			require.NoError(t, err)

			// Extract() returns the flattened tar stream of the image filesystem.
			rc := img.Extract()
			defer rc.Close()

			tr := tar.NewReader(rc)
			var versionJSON string

			for {
				hdr, err := tr.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				if hdr.Name != "version.json" {
					continue
				}
				data, err := io.ReadAll(tr)
				require.NoError(t, err)
				versionJSON = string(data)
				break
			}

			require.NotEmpty(t, versionJSON, "version.json not found in image for channel %q", channel)

			type vj struct {
				Version string `json:"version"`
			}
			var parsed vj
			require.NoError(t, json.Unmarshal([]byte(versionJSON), &parsed))
			assert.Equal(t, wantVersion, parsed.Version, "channel %q", channel)
		})
	}
}

// TestNewRegistryClientStub_RootTags verifies that root-level tags are present.
func TestNewRegistryClientStub_RootTags(t *testing.T) {
	client := fake.NewRegistryClientStub()

	tags, err := client.ListTags(context.Background())
	require.NoError(t, err)

	// All root-level tags must be present.
	wantTags := []string{
		"alpha", "beta", "early-access", "stable", "rock-solid",
		"v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0",
		"pr12345",
	}
	assert.ElementsMatch(t, wantTags, tags)
}

// TestNewRegistryClientStub_InstallTags verifies the "install" repository.
func TestNewRegistryClientStub_InstallTags(t *testing.T) {
	client := fake.NewRegistryClientStub()
	installClient := client.WithSegment("install")

	tags, err := installClient.ListTags(context.Background())
	require.NoError(t, err)

	assert.Contains(t, tags, "v1.72.10")
	assert.Contains(t, tags, "stable")
}

// TestNewRegistryClientStub_InstallStandaloneTags verifies the
// "install-standalone" repository.
func TestNewRegistryClientStub_InstallStandaloneTags(t *testing.T) {
	client := fake.NewRegistryClientStub()
	saClient := client.WithSegment("install-standalone")

	tags, err := saClient.ListTags(context.Background())
	require.NoError(t, err)

	assert.Contains(t, tags, "v1.71.0")
}

// TestNewRegistryClientStub_PlatformImageHasVersionJSON verifies that root
// images carry a version.json file.
func TestNewRegistryClientStub_PlatformImageHasVersionJSON(t *testing.T) {
	client := fake.NewRegistryClientStub()

	cfg, err := client.GetImageConfig(context.Background(), "v1.72.10")
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "v1.72.10", cfg.Config.Labels["org.opencontainers.image.version"])
}

// TestNewRegistryClientStub_CustomTag verifies that the non-semver "pr12345" tag exists.
func TestNewRegistryClientStub_CustomTag(t *testing.T) {
	client := fake.NewRegistryClientStub()

	err := client.CheckImageExists(context.Background(), "pr12345")
	assert.NoError(t, err)
}

// TestNewRegistryClientStub_GetRegistry verifies the default registry path.
func TestNewRegistryClientStub_GetRegistry(t *testing.T) {
	client := fake.NewRegistryClientStub()
	// Default host set in deckhouse_stub.go:
	// registry.deckhouse.ru/deckhouse/fe
	assert.Equal(t, "registry.deckhouse.ru/deckhouse/fe", client.GetRegistry())
}
