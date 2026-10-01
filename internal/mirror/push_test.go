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

package mirror

import (
	"context"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// TestPackageNameFromPath covers the .tar-only contract packageNameFromPath
// relies on: cmd/push/validation.go always canonicalizes chunked archives to
// their <name>.tar path (see canonicalPackagePath) before they reach
// PushService, so this function only ever needs to strip ".tar".
func TestPackageNameFromPath(t *testing.T) {
	tests := []struct {
		name    string
		pkgPath string
		want    string
	}{
		{
			name:    "absolute tar path",
			pkgPath: "/bundle/platform.tar",
			want:    "platform",
		},
		{
			name:    "relative tar path",
			pkgPath: "platform.tar",
			want:    "platform",
		},
		{
			name:    "module tar path with dashes in the name",
			pkgPath: filepath.Join("/bundle", "module-foo.tar"),
			want:    "module-foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, packageNameFromPath(tt.pkgPath))
		})
	}
}

// buildLayoutBundle writes an OCI layout with one image annotated by
// short_tag, packs it into <dir>/<tarName> under the given tar prefix, and
// returns the archive path. Prefix "modules/<name>" mimics how pull packs a
// module; a bare prefix like "install" mimics a non-module layout.
func buildLayoutBundle(t *testing.T, dir, tarName, prefix, shortTag string) string {
	t.Helper()

	layoutDir := t.TempDir()
	imgLayout, err := regimage.NewImageLayout(layoutDir)
	require.NoError(t, err, "create OCI layout")

	img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+shortTag+`"}`).
		MustBuild()
	require.NoError(t, imgLayout.Path().AppendImage(img, layout.WithAnnotations(map[string]string{
		regimage.AnnotationImageShortTag: shortTag,
	})), "append annotated image")

	tarPath := filepath.Join(dir, tarName)
	f, err := os.Create(tarPath)
	require.NoError(t, err, "create bundle tar")
	defer f.Close()

	require.NoError(t, bundle.PackWithPrefix(context.Background(), layoutDir, prefix, f), "pack bundle tar")

	return tarPath
}

// TestPushService_PluginsLayout verifies the plugin leg of push: a bundle tar
// prefixed deckhouse-cli/plugins/<name> lands verbatim at that registry path,
// the discovery tag appears on the plugins index, the summary counts it, and
// --modules-path-suffix never moves plugin paths.
func TestPushService_PluginsLayout(t *testing.T) {
	const (
		repoHost   = "registry.example.com/deckhouse/ee"
		pluginName = "postgresql-mgr"
		pluginTag  = "v1.2.0"
	)

	bundleDir := t.TempDir()
	pluginPkg := buildLayoutBundle(t, bundleDir, "plugin-"+pluginName+".tar",
		path.Join("deckhouse-cli", "plugins", pluginName), pluginTag)

	reg := upfake.NewRegistry(repoHost)
	destClient := pkgclient.Adapt(upfake.NewClient(reg))

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := NewPushService(destClient, &PushServiceOptions{
		Packages:   []string{pluginPkg},
		WorkingDir: t.TempDir(),
		// A moved modules path must not touch plugins.
		ModulesPathSuffix: "/my/mods",
	}, logger, userLogger)

	summary, err := svc.Push(context.Background())
	require.NoError(t, err, "push must succeed")

	assert.Equal(t, 1, summary.Plugins, "one plugin repository pushed")
	assert.False(t, summary.PlatformPushed, "a plugin layout must not be classified as platform")

	ctx := context.Background()

	pluginClient := destClient.WithSegment("deckhouse-cli", "plugins", pluginName)
	assert.NoErrorf(t, pluginClient.CheckImageExists(ctx, pluginTag),
		"plugin image must exist at %s:%s", pluginClient.GetRegistry(), pluginTag)

	indexClient := destClient.WithSegment("deckhouse-cli", "plugins")
	tags, err := indexClient.ListTags(ctx)
	require.NoError(t, err)
	assert.Contains(t, tags, pluginName, "discovery tag must exist on the plugins index path")
}

// TestPushService_ModulesPathSuffix verifies that --modules-path-suffix moves
// both module images and their discovery index tag, while non-module layouts
// stay put. The default (empty / "/modules") keeps the historical layout.
func TestPushService_ModulesPathSuffix(t *testing.T) {
	const (
		repoHost   = "registry.example.com/deckhouse/ee"
		moduleName = "test-module"
		moduleTag  = "v0.0.1"
		installTag = "v1.76.2"
	)

	tests := []struct {
		name       string
		suffix     string
		wantModule string // repo (relative to target) holding module images
		wantIndex  string // repo (relative to target) holding the discovery tag
	}{
		{name: "empty keeps default", suffix: "", wantModule: "modules/" + moduleName, wantIndex: "modules"},
		{name: "explicit default", suffix: "/modules", wantModule: "modules/" + moduleName, wantIndex: "modules"},
		{name: "repo root", suffix: "/", wantModule: moduleName, wantIndex: ""},
		{name: "multi segment", suffix: "/my/mods", wantModule: "my/mods/" + moduleName, wantIndex: "my/mods"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bundleDir := t.TempDir()
			modulePkg := buildLayoutBundle(t, bundleDir, "module-"+moduleName+".tar", path.Join("modules", moduleName), moduleTag)
			// A non-module layout: it must never be affected by the suffix.
			installPkg := buildLayoutBundle(t, bundleDir, "platform.tar", "install", installTag)

			reg := upfake.NewRegistry(repoHost)
			destClient := pkgclient.Adapt(upfake.NewClient(reg))

			logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
			userLogger := log.NewSLogger(slog.LevelWarn)

			svc := NewPushService(destClient, &PushServiceOptions{
				Packages:          []string{modulePkg, installPkg},
				WorkingDir:        t.TempDir(),
				ModulesPathSuffix: tt.suffix,
			}, logger, userLogger)
			summary, err := svc.Push(context.Background())
			require.NoError(t, err, "push must succeed")

			// Summary reflects what was pushed: one module and the install
			// layout (counted as platform). Moved tracks a non-default modules path.
			assert.Equal(t, 1, summary.Modules, "one module pushed")
			assert.True(t, summary.PlatformPushed, "install layout counts as platform")
			wantMoved := tt.wantModule != "modules/"+moduleName
			assert.Equal(t, wantMoved, summary.ModulesPath.Moved,
				"modules path report reflects a moved modules path")

			ctx := context.Background()

			// Module images land at <repo>/<wantModule>:<moduleTag>.
			moduleClient := destClient.WithSegment(pkgclient.PathToSegments(tt.wantModule)...)
			assert.NoErrorf(t, moduleClient.CheckImageExists(ctx, moduleTag),
				"module image must exist at %s:%s", moduleClient.GetRegistry(), moduleTag)

			// Discovery tag lands at <repo>/<wantIndex>:<moduleName>.
			indexClient := destClient.WithSegment(pkgclient.PathToSegments(tt.wantIndex)...)
			tags, err := indexClient.ListTags(ctx)
			require.NoError(t, err)
			assert.Containsf(t, tags, moduleName,
				"discovery tag %q must exist at %s", moduleName, indexClient.GetRegistry())

			// A non-default suffix must MOVE modules, not copy them: the default
			// modules/ path must hold nothing.
			if tt.wantModule != "modules/"+moduleName {
				defaultRepo := destClient.WithSegment(pkgclient.PathToSegments("modules/" + moduleName)...)
				assert.Errorf(t, defaultRepo.CheckImageExists(ctx, moduleTag),
					"module must not remain at default modules/%s", moduleName)
			}

			// The non-module layout is unaffected by the suffix.
			installRepo := destClient.WithSegment(pkgclient.PathToSegments("install")...)
			assert.NoErrorf(t, installRepo.CheckImageExists(ctx, installTag),
				"install layout must stay at <repo>/install regardless of suffix")
		})
	}
}

func TestSplitTargetEdition(t *testing.T) {
	tests := []struct {
		repoPath    string
		wantRoot    string
		wantEdition pkg.Edition
	}{
		// An edition repo splits into the root above it and the edition.
		{repoPath: "/deckhouse/ee", wantRoot: "/deckhouse", wantEdition: pkg.EEEdition},
		{repoPath: "/deckhouse/fe", wantRoot: "/deckhouse", wantEdition: pkg.FEEdition},
		{repoPath: "/deckhouse/se", wantRoot: "/deckhouse", wantEdition: pkg.SEEdition},
		{repoPath: "/deckhouse/se-plus", wantRoot: "/deckhouse", wantEdition: pkg.SEPlusEdition},
		{repoPath: "/deckhouse/be", wantRoot: "/deckhouse", wantEdition: pkg.BEEdition},
		{repoPath: "/deckhouse/ce", wantRoot: "/deckhouse", wantEdition: pkg.CEEdition},
		{repoPath: "/deckhouse/cse", wantRoot: "/deckhouse", wantEdition: pkg.CSEEdition},
		{repoPath: "/deckhouse/ce/", wantRoot: "/deckhouse", wantEdition: pkg.CEEdition},
		{repoPath: "/mirror/deckhouse/ee", wantRoot: "/mirror/deckhouse", wantEdition: pkg.EEEdition},
		// No edition at the end of the path.
		{repoPath: "/deckhouse", wantRoot: "/deckhouse", wantEdition: pkg.NoEdition},
		{repoPath: "/deckhouse/ee-mirror", wantRoot: "/deckhouse/ee-mirror", wantEdition: pkg.NoEdition},
		{repoPath: "/ee/deckhouse", wantRoot: "/ee/deckhouse", wantEdition: pkg.NoEdition},
		// An edition as the only segment would leave the bare host as root.
		{repoPath: "/ee", wantRoot: "/ee", wantEdition: pkg.NoEdition},
		{repoPath: "/cse", wantRoot: "/cse", wantEdition: pkg.NoEdition},
		{repoPath: "//ee", wantRoot: "//ee", wantEdition: pkg.NoEdition},
	}

	for _, tt := range tests {
		t.Run(tt.repoPath, func(t *testing.T) {
			root, edition := SplitTargetEdition(tt.repoPath)
			assert.Equal(t, tt.wantRoot, root)
			assert.Equal(t, tt.wantEdition, edition)
		})
	}
}

// buildEditionBundle writes a bundle with one layout per push route: the
// edition-scoped platform root, install and a module, and the
// edition-independent installer, d8 binary and plugin.
func buildEditionBundle(t *testing.T) []string {
	t.Helper()

	bundleDir := t.TempDir()

	return []string{
		buildLayoutBundle(t, bundleDir, "platform.tar", "", "v1.76.2"),
		buildLayoutBundle(t, bundleDir, "install.tar", "install", "v1.76.2"),
		buildLayoutBundle(t, bundleDir, "module-foo.tar", path.Join("modules", "foo"), "v0.0.1"),
		buildLayoutBundle(t, bundleDir, "installer.tar", "installer", "latest"),
		buildLayoutBundle(t, bundleDir, "deckhouse-cli.tar", "deckhouse-cli", "v0.13.1"),
		buildLayoutBundle(t, bundleDir, "plugin-system.tar", path.Join("deckhouse-cli", "plugins", "system"), "v1.0.0"),
	}
}

// TestPushService_EditionTargets pushes CE, CSE and EE bundles to the edition
// repos of one registry, side by side. Edition-scoped layouts land under their
// own edition; the installer and deckhouse-cli land once, in the root above
// them all, as pull reads them from the Deckhouse registry.
func TestPushService_EditionTargets(t *testing.T) {
	reg := upfake.NewRegistry("registry.io")
	rootClient := pkgclient.Adapt(upfake.NewClient(reg)).WithSegment("deckhouse")

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	ctx := context.Background()
	exists := func(repo, tag string) error {
		return rootClient.WithSegment(pkgclient.PathToSegments(repo)...).CheckImageExists(ctx, tag)
	}

	editions := []pkg.Edition{pkg.CEEdition, pkg.CSEEdition, pkg.EEEdition}

	for _, edition := range editions {
		svc := NewPushService(rootClient, &PushServiceOptions{
			Packages:   buildEditionBundle(t),
			WorkingDir: t.TempDir(),
			Edition:    edition,
		}, logger, userLogger)

		summary, err := svc.Push(ctx)
		require.NoErrorf(t, err, "push to the %s edition repo", edition)

		assert.True(t, summary.PlatformPushed)
		assert.True(t, summary.InstallerPushed)
		assert.True(t, summary.DeckhouseCLIPushed)
		assert.Equal(t, 1, summary.Modules)
		assert.Equal(t, 1, summary.Plugins)
	}

	for _, edition := range editions {
		t.Run(edition.String(), func(t *testing.T) {
			edition := edition.String()

			assert.NoError(t, exists(edition, "v1.76.2"), "platform root stays in the edition")
			assert.NoError(t, exists(edition+"/install", "v1.76.2"), "install stays in the edition")
			assert.NoError(t, exists(edition+"/modules/foo", "v0.0.1"), "modules stay in the edition")

			tags, err := rootClient.WithSegment(edition, "modules").ListTags(ctx)
			require.NoError(t, err)
			assert.Contains(t, tags, "foo", "the modules index stays in the edition")

			assert.Error(t, exists(edition+"/installer", "latest"), "installer must not land in the edition")
			assert.Error(t, exists(edition+"/deckhouse-cli", "v0.13.1"), "d8 binary must not land in the edition")
			assert.Error(t, exists(edition+"/deckhouse-cli/plugins/system", "v1.0.0"), "plugin must not land in the edition")
		})
	}

	assert.NoError(t, exists("installer", "latest"), "installer lands in the root above the editions")
	assert.NoError(t, exists("deckhouse-cli", "v0.13.1"), "d8 binary lands in the root above the editions")
	assert.NoError(t, exists("deckhouse-cli/plugins/system", "v1.0.0"), "plugin lands in the root above the editions")

	tags, err := rootClient.WithSegment("deckhouse-cli", "plugins").ListTags(ctx)
	require.NoError(t, err)
	assert.Contains(t, tags, "system", "the plugins index lands in the root above the editions")
}

// TestPushService_NoEditionKeepsLayoutVerbatim: a target without an edition
// holds the whole bundle, the installer and deckhouse-cli included.
func TestPushService_NoEditionKeepsLayoutVerbatim(t *testing.T) {
	reg := upfake.NewRegistry("registry.io")
	target := pkgclient.Adapt(upfake.NewClient(reg)).WithSegment("mirror", "deckhouse")

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := NewPushService(target, &PushServiceOptions{
		Packages:   buildEditionBundle(t),
		WorkingDir: t.TempDir(),
	}, logger, userLogger)

	_, err := svc.Push(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	for repo, tag := range map[string]string{
		"":                             "v1.76.2",
		"install":                      "v1.76.2",
		"modules/foo":                  "v0.0.1",
		"installer":                    "latest",
		"deckhouse-cli":                "v0.13.1",
		"deckhouse-cli/plugins/system": "v1.0.0",
	} {
		assert.NoErrorf(t, target.WithSegment(pkgclient.PathToSegments(repo)...).CheckImageExists(ctx, tag),
			"%q:%s must land under the target", repo, tag)
	}
}
