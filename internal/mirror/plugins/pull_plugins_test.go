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

package plugins

import (
	"archive/tar"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// newPhaseService wires the plugins phase over a fake registry with quiet
// loggers, a fresh working dir, and the given options.
func newPhaseService(t *testing.T, reg *upfake.Registry, options *Options) *Service {
	t.Helper()

	stub := pkgclient.Adapt(upfake.NewClient(reg))
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	regSvc := registryservice.NewService(stub, pkg.NoEdition, logger)

	return NewService(regSvc, t.TempDir(), options, logger, log.NewSLogger(slog.LevelWarn))
}

// readBundleTar returns all entry names of a bundle tar and the content of
// its single OCI index.json.
func readBundleTar(t *testing.T, path string) ([]string, []byte) {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var names []string

	var indexJSON []byte

	tr := tar.NewReader(f)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)
		names = append(names, header.Name)

		if strings.HasSuffix(header.Name, "/index.json") {
			indexJSON, err = io.ReadAll(tr)
			require.NoError(t, err)
		}
	}

	return names, indexJSON
}

// untarTo extracts a bundle tar into destDir.
func untarTo(t *testing.T, tarPath, destDir string) {
	t.Helper()

	f, err := os.Open(tarPath)
	require.NoError(t, err)
	defer f.Close()

	tr := tar.NewReader(f)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)

		target := filepath.Join(destDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			require.NoError(t, os.MkdirAll(target, 0o755))
		case tar.TypeReg:
			require.NoError(t, os.MkdirAll(filepath.Dir(target), 0o755))

			out, err := os.Create(target)
			require.NoError(t, err)
			_, err = io.Copy(out, tr) //nolint:gosec // test fixture tars only
			require.NoError(t, err)
			require.NoError(t, out.Close())
		}
	}
}

const mgrContractV110 = `{
	"name": "postgresql-mgr", "version": "v1.1.0",
	"requirements": {"modules": {"mandatory": [{"name": "postgresql", "constraint": ">=1.0.0 <1.5.0"}]}}
}`

const mgrContractV120 = `{
	"name": "postgresql-mgr", "version": "v1.2.0",
	"requirements": {"modules": {"mandatory": [{"name": "postgresql", "constraint": ">=1.5.0"}]}}
}`

// TestPullPlugins_EndToEnd runs the whole phase against a fake registry: the
// per-module-version selection picks two plugin versions, both land in one
// plugin-<name>.tar under the deckhouse-cli/plugins/<name> prefix, and the
// stats carry versions with provenance.
func TestPullPlugins_EndToEnd(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addPluginVersion(t, reg, "postgresql-mgr", "v1.1.0", mgrContractV110)
	addPluginVersion(t, reg, "postgresql-mgr", "v1.2.0", mgrContractV120)

	bundleDir := t.TempDir()
	svc := newPhaseService(t, reg, &Options{BundleDir: bundleDir})

	err := svc.PullPlugins(context.Background(), PullInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.0.0", "v1.5.0", "v1.10.0")},
	})
	require.NoError(t, err)

	tarPath := filepath.Join(bundleDir, "plugin-postgresql-mgr.tar")
	require.FileExists(t, tarPath)

	entries, indexJSON := readBundleTar(t, tarPath)
	require.NotEmpty(t, entries)

	for _, entry := range entries {
		assert.Truef(t, strings.HasPrefix(entry, "deckhouse-cli/plugins/postgresql-mgr/"),
			"every tar entry must carry the registry prefix, got %q", entry)
	}

	var index struct {
		Manifests []struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"manifests"`
	}
	require.NoError(t, json.Unmarshal(indexJSON, &index))

	tags := make([]string, 0, len(index.Manifests))
	for _, m := range index.Manifests {
		tags = append(tags, m.Annotations["io.deckhouse.image.short_tag"])
	}

	assert.ElementsMatch(t, []string{"v1.1.0", "v1.2.0"}, tags,
		"the bundle must hold exactly the versions the resolver picked")

	stats := svc.Stats()
	assert.True(t, stats.Attempted)
	require.Len(t, stats.Plugins, 1)
	assert.Equal(t, "postgresql-mgr", stats.Plugins[0].Name)
	assert.Equal(t, 2, stats.Plugins[0].Images)
	assert.Equal(t, 2, stats.TotalImages)
	require.Len(t, stats.Plugins[0].Versions, 2)
	assert.NotEmpty(t, stats.Plugins[0].Versions[0].Reasons, "provenance must reach the stats")
}

// TestPullPlugins_NothingToMirror: a registry with no relevant plugins yields
// no error, no tars, and attempted-but-empty stats.
func TestPullPlugins_NothingToMirror(t *testing.T) {
	reg := upfake.NewRegistry(testHost)

	bundleDir := t.TempDir()
	svc := newPhaseService(t, reg, &Options{BundleDir: bundleDir})

	err := svc.PullPlugins(context.Background(), PullInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
	})
	require.NoError(t, err)

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "no bundle files must be written when nothing is mirrored")

	stats := svc.Stats()
	assert.True(t, stats.Attempted)
	assert.Empty(t, stats.Plugins)
}

// TestPullPlugins_DryRun: the resolution runs in full (versions in stats) but
// neither layouts nor bundle tars are written.
func TestPullPlugins_DryRun(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addPluginVersion(t, reg, "postgresql-mgr", "v1.2.0", mgrContractV120)

	bundleDir := t.TempDir()
	svc := newPhaseService(t, reg, &Options{BundleDir: bundleDir, DryRun: true})

	err := svc.PullPlugins(context.Background(), PullInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
	})
	require.NoError(t, err)

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run must not write bundle files")

	stats := svc.Stats()
	require.Len(t, stats.Plugins, 1)
	assert.Equal(t, []PluginVersionStat{{
		Version: "v1.2.0",
		Reasons: []Reason{{Kind: ReasonModule, Subject: "postgresql", Constraint: ">=1.5.0"}},
	}}, stats.Plugins[0].Versions)
	assert.Zero(t, stats.TotalImages, "dry-run pulls no images")
}

// TestPullPlugins_MultiPlatformIndexPreserved is the end-to-end proof of the
// multi-platform keystone: a plugin published as a two-platform index with a
// contract annotation, a subject and inline child data goes through
// resolve -> pull -> pack and comes out of the bundle tar as the same index -
// same digest, both platforms, contract annotation, subject and data intact.
// Runs against ggcr's in-memory registry with the real client (the fake
// cannot store indexes).
func TestPullPlugins_MultiPlatformIndexPreserved(t *testing.T) {
	srv := httptest.NewServer(ggcrregistry.New())
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")

	contractJSON := `{
		"name": "pg-tool", "version": "v1.0.0",
		"requirements": {"modules": {"mandatory": [{"name": "postgresql", "constraint": ">=1.0.0"}]}}
	}`
	encodedContract := base64.StdEncoding.EncodeToString([]byte(contractJSON))

	linuxImg := upfake.NewImageBuilder().WithFile("plugin", "linux-bin").MustBuild()
	darwinImg := upfake.NewImageBuilder().WithFile("plugin", "darwin-bin").MustBuild()

	// Inline data on one child and a subject on the index: optional OCI
	// fields the rebuilt index must not lose.
	linuxRaw, err := linuxImg.RawManifest()
	require.NoError(t, err)

	idx := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: linuxImg, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}, Data: linuxRaw}},
		mutate.IndexAddendum{Add: darwinImg, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "darwin", Architecture: "arm64"}}},
	)
	annotated, ok := mutate.Annotations(idx, map[string]string{"contract": encodedContract}).(v1.ImageIndex)
	require.True(t, ok)

	subjectDesc, err := partial.Descriptor(darwinImg)
	require.NoError(t, err)
	source, ok := mutate.Subject(annotated, *subjectDesc).(v1.ImageIndex)
	require.True(t, ok)

	sourceDigest, err := source.Digest()
	require.NoError(t, err)

	pluginRef, err := name.ParseReference(host+"/deckhouse-cli/plugins/pg-tool:v1.0.0", name.Insecure)
	require.NoError(t, err)
	require.NoError(t, remote.WriteIndex(pluginRef, source))

	// The catalog's directory-as-tags name index.
	nameRef, err := name.ParseReference(host+"/deckhouse-cli/plugins:pg-tool", name.Insecure)
	require.NoError(t, err)
	require.NoError(t, remote.Write(nameRef, upfake.NewImageBuilder().WithFile("name", "pg-tool").MustBuild()))

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	regSvc := registryservice.NewService(pkgclient.NewFromOptions(host, regclient.WithInsecure(true)), pkg.NoEdition, logger)

	bundleDir := t.TempDir()
	svc := NewService(regSvc, t.TempDir(), &Options{BundleDir: bundleDir}, logger, log.NewSLogger(slog.LevelWarn))

	err = svc.PullPlugins(context.Background(), PullInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
	})
	require.NoError(t, err)

	tarPath := filepath.Join(bundleDir, "plugin-pg-tool.tar")
	require.FileExists(t, tarPath)

	extracted := t.TempDir()
	untarTo(t, tarPath, extracted)

	layoutPath := layout.Path(filepath.Join(extracted, "deckhouse-cli", "plugins", "pg-tool"))
	topIndex, err := layoutPath.ImageIndex()
	require.NoError(t, err)
	topManifest, err := topIndex.IndexManifest()
	require.NoError(t, err)

	require.Len(t, topManifest.Manifests, 1)
	desc := topManifest.Manifests[0]
	assert.True(t, desc.MediaType.IsIndex(), "the plugin version must stay an index in the bundle")
	assert.Equal(t, "v1.0.0", desc.Annotations["io.deckhouse.image.short_tag"])

	nested, err := topIndex.ImageIndex(desc.Digest)
	require.NoError(t, err)
	nestedManifest, err := nested.IndexManifest()
	require.NoError(t, err)

	require.Len(t, nestedManifest.Manifests, 2, "both platform children must survive the round-trip")
	platforms := []string{
		nestedManifest.Manifests[0].Platform.String(),
		nestedManifest.Manifests[1].Platform.String(),
	}
	assert.ElementsMatch(t, []string{"linux/amd64", "darwin/arm64"}, platforms)
	assert.Equal(t, encodedContract, nestedManifest.Annotations["contract"],
		"the contract annotation must survive into the bundle")

	require.NotNil(t, nestedManifest.Subject, "the index subject must survive into the bundle")
	assert.Equal(t, subjectDesc.Digest, nestedManifest.Subject.Digest)
	assert.Equal(t, linuxRaw, nestedManifest.Manifests[0].Data, "inline child data must survive into the bundle")

	assert.Equal(t, sourceDigest, desc.Digest, "the bundled index must keep the published digest")
}

// TestLayoutFor_RejectsMalformedName: the layout directory is built from the
// plugin name, so a name that is not a single path component must not reach
// the filesystem join even if it bypassed the resolver.
func TestLayoutFor_RejectsMalformedName(t *testing.T) {
	svc := newPhaseService(t, upfake.NewRegistry(testHost), &Options{})

	_, err := svc.layoutFor("../../outside")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid plugin name")
	assert.NoDirExists(t, filepath.Join(filepath.Dir(svc.workingDir), "outside"))
}
