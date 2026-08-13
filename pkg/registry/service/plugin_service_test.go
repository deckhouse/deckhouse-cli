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

package service_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse/pkg/log"
	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/pkg"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// TestPluginsService_Scoping pins the registry paths of the plugins catalog:
// it lives at <root>/deckhouse-cli/plugins OUTSIDE the edition segment (same
// asymmetry as the installer), and each plugin repository hangs directly under
// it. A regression in either direction silently routes plugin pulls to a path
// the registry-packages-proxy and registry-bundle never serve.
func TestPluginsService_Scoping(t *testing.T) {
	logger := log.NewNop()

	const host = "registry.deckhouse.ru/deckhouse"

	t.Run("plugins catalog is NOT edition-scoped", func(t *testing.T) {
		svc := registryservice.NewService(pkgclient.NewFromOptions(host), pkg.FEEdition, logger)

		assert.Equal(t, host+"/deckhouse-cli/plugins", svc.PluginService().GetRoot(),
			"plugins catalog must live at the bare root, never under the edition segment")
		assert.Equal(t, host+"/deckhouse-cli/plugins/stronghold", svc.PluginService().Plugin("stronghold").GetRoot(),
			"a plugin repository must hang directly under the plugins catalog")
	})

	t.Run("no edition yields the same paths", func(t *testing.T) {
		svc := registryservice.NewService(pkgclient.NewFromOptions(host), pkg.NoEdition, logger)

		assert.Equal(t, host+"/deckhouse-cli/plugins", svc.PluginService().GetRoot())
	})

	t.Run("plugin sub-services are cached", func(t *testing.T) {
		svc := registryservice.NewService(pkgclient.NewFromOptions(host), pkg.NoEdition, logger)

		first := svc.PluginService().Plugin("stronghold")
		second := svc.PluginService().Plugin("stronghold")
		assert.Same(t, first, second, "Plugin must return the same instance for the same name")
	})
}

// TestPluginService_ContractAnnotation covers the contract resolution order:
// single manifest -> its annotation; index -> index annotation first, first
// child only as a fallback; no contract anywhere -> empty string, no error.
func TestPluginService_ContractAnnotation(t *testing.T) {
	logger := log.NewNop()

	t.Run("reads the annotation from the index without fetching a child", func(t *testing.T) {
		client := &fakeManifestClient{byTag: map[string]dkpreg.ManifestResult{
			"v1.0.0": fakeManifestResult{
				mediaType: types.OCIImageIndex,
				index: fakeIndexManifest{
					annotations: map[string]string{"contract": "index-contract"},
					manifests:   []dkpreg.Descriptor{fakeDescriptor{digest: v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("a", 64)}}},
				},
			},
		}}

		svc := registryservice.NewPluginService(client, logger)

		got, err := svc.ContractAnnotation(context.Background(), "v1.0.0")
		require.NoError(t, err)
		assert.Equal(t, "index-contract", got)
		assert.Equal(t, []string{"v1.0.0"}, client.gotTags,
			"the child manifest must not be fetched when the index carries the contract")
	})

	t.Run("falls back to the first child when the index has no contract", func(t *testing.T) {
		childDigest := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("b", 64)}
		client := &fakeManifestClient{byTag: map[string]dkpreg.ManifestResult{
			"v1.0.0": fakeManifestResult{
				mediaType: types.OCIImageIndex,
				index: fakeIndexManifest{
					manifests: []dkpreg.Descriptor{fakeDescriptor{digest: childDigest}},
				},
			},
			"@" + childDigest.String(): fakeManifestResult{
				mediaType: types.OCIManifestSchema1,
				manifest:  fakeManifest{annotations: map[string]string{"contract": "child-contract"}},
			},
		}}

		svc := registryservice.NewPluginService(client, logger)

		got, err := svc.ContractAnnotation(context.Background(), "v1.0.0")
		require.NoError(t, err)
		assert.Equal(t, "child-contract", got)
		assert.Equal(t, []string{"v1.0.0", "@" + childDigest.String()}, client.gotTags)
	})

	t.Run("reads the annotation from a single (non-index) manifest", func(t *testing.T) {
		client := &fakeManifestClient{byTag: map[string]dkpreg.ManifestResult{
			"v1.0.0": fakeManifestResult{
				mediaType: types.OCIManifestSchema1,
				manifest:  fakeManifest{annotations: map[string]string{"contract": "single-contract"}},
			},
		}}

		svc := registryservice.NewPluginService(client, logger)

		got, err := svc.ContractAnnotation(context.Background(), "v1.0.0")
		require.NoError(t, err)
		assert.Equal(t, "single-contract", got)
	})

	t.Run("contract-less image yields empty string without error", func(t *testing.T) {
		childDigest := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("c", 64)}
		client := &fakeManifestClient{byTag: map[string]dkpreg.ManifestResult{
			"v1.0.0": fakeManifestResult{
				mediaType: types.OCIImageIndex,
				index: fakeIndexManifest{
					manifests: []dkpreg.Descriptor{fakeDescriptor{digest: childDigest}},
				},
			},
			"@" + childDigest.String(): fakeManifestResult{
				mediaType: types.OCIManifestSchema1,
				manifest:  fakeManifest{},
			},
		}}

		svc := registryservice.NewPluginService(client, logger)

		got, err := svc.ContractAnnotation(context.Background(), "v1.0.0")
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("index without children yields empty string without error", func(t *testing.T) {
		client := &fakeManifestClient{byTag: map[string]dkpreg.ManifestResult{
			"v1.0.0": fakeManifestResult{
				mediaType: types.OCIImageIndex,
				index:     fakeIndexManifest{},
			},
		}}

		svc := registryservice.NewPluginService(client, logger)

		got, err := svc.ContractAnnotation(context.Background(), "v1.0.0")
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("manifest fetch error is returned", func(t *testing.T) {
		client := &fakeManifestClient{}

		svc := registryservice.NewPluginService(client, logger)

		_, err := svc.ContractAnnotation(context.Background(), "v9.9.9")
		require.Error(t, err)
	})
}

// fakeManifestClient serves a preset ManifestResult per reference and records
// the references it was asked for, so tests can assert whether a child
// manifest was fetched. Only GetManifest is implemented; the embedded
// interface panics on any other call.
type fakeManifestClient struct {
	dkpreg.Client

	byTag   map[string]dkpreg.ManifestResult
	gotTags []string
}

func (c *fakeManifestClient) GetManifest(_ context.Context, tag string) (dkpreg.ManifestResult, error) {
	c.gotTags = append(c.gotTags, tag)

	res, ok := c.byTag[tag]
	if !ok {
		return nil, fmt.Errorf("no manifest for %q", tag)
	}

	return res, nil
}

type fakeManifestResult struct {
	dkpreg.ManifestResult

	mediaType types.MediaType
	manifest  dkpreg.Manifest
	index     dkpreg.IndexManifest
}

func (r fakeManifestResult) GetMediaType() types.MediaType         { return r.mediaType }
func (r fakeManifestResult) GetManifest() (dkpreg.Manifest, error) { return r.manifest, nil }
func (r fakeManifestResult) GetIndexManifest() (dkpreg.IndexManifest, error) {
	return r.index, nil
}

type fakeManifest struct {
	dkpreg.Manifest

	annotations map[string]string
}

func (m fakeManifest) GetAnnotations() map[string]string { return m.annotations }

type fakeIndexManifest struct {
	dkpreg.IndexManifest

	annotations map[string]string
	manifests   []dkpreg.Descriptor
}

func (i fakeIndexManifest) GetAnnotations() map[string]string { return i.annotations }
func (i fakeIndexManifest) GetManifests() []dkpreg.Descriptor { return i.manifests }

type fakeDescriptor struct {
	dkpreg.Descriptor

	digest v1.Hash
}

func (d fakeDescriptor) GetDigest() v1.Hash { return d.digest }
