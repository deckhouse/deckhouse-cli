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
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

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

// TestPushService_PluginsGoToRootAboveEdition verifies the plugin leg of push:
// a bundle tar prefixed deckhouse-cli/plugins/<name> lands under the registry
// root above the target's edition segment (the path pull reads plugins from
// and registry-packages-proxy looks them up by), the discovery tag appears on
// that plugins catalog, the summary counts it, the service's path report
// names it, and nothing else moves: a platform layout stays under the target
// and --modules-path-suffix never touches plugin paths.
func TestPushService_PluginsGoToRootAboveEdition(t *testing.T) {
	const (
		host       = "registry.example.com"
		pluginName = "postgresql-mgr"
		pluginTag  = "v1.2.0"
		installTag = "v1.76.2"
	)

	tests := []struct {
		name string
		// target is the push target path under host, as the CLI parses it
		// (leading slash from the URL path).
		target string
		// wantRoot is the path under host plugins are rooted at.
		wantRoot    string
		wantEdition string
	}{
		{name: "edition target", target: "/deckhouse/ee", wantRoot: "deckhouse", wantEdition: "ee"},
		{name: "hyphenated edition", target: "/deckhouse/se-plus", wantRoot: "deckhouse", wantEdition: "se-plus"},
		{name: "DKP docs layout", target: "/dkp/ee", wantRoot: "dkp", wantEdition: "ee"},
		{name: "trailing slash", target: "/deckhouse/ee/", wantRoot: "deckhouse", wantEdition: "ee"},
		{name: "no edition", target: "/deckhouse", wantRoot: "deckhouse"},
		{name: "no edition, nested path", target: "/sys/deckhouse-oss", wantRoot: "sys/deckhouse-oss"},
		{name: "edition name not at the end is not an edition", target: "/ee/deckhouse", wantRoot: "ee/deckhouse"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bundleDir := t.TempDir()
			pluginPkg := buildLayoutBundle(t, bundleDir, "plugin-"+pluginName+".tar",
				path.Join("deckhouse-cli", "plugins", pluginName), pluginTag)
			// A platform layout: it must stay under the target.
			installPkg := buildLayoutBundle(t, bundleDir, "platform.tar", "install", installTag)

			reg := upfake.NewRegistry(host)
			hostClient := pkgclient.Adapt(upfake.NewClient(reg))

			logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
			userLogger := log.NewSLogger(slog.LevelWarn)

			svc := NewPushService(hostClient, &PushServiceOptions{
				TargetPath: tt.target,
				Packages:   []string{pluginPkg, installPkg},
				WorkingDir: t.TempDir(),
				// A moved modules path must not touch plugins.
				ModulesPathSuffix: "/my/mods",
			}, logger, userLogger)

			summary, err := svc.Push(context.Background())
			require.NoError(t, err, "push must succeed")

			assert.Equal(t, 1, summary.Plugins, "one plugin repository pushed")
			assert.True(t, summary.PlatformPushed, "install layout counts as platform")

			ctx := context.Background()

			targetClient := hostClient.WithSegment(pkgclient.PathToSegments(tt.target)...)
			rootClient := hostClient.WithSegment(pkgclient.PathToSegments(tt.wantRoot)...)

			// The service's report (what the log notice and the denied-write
			// error are built from) names the real paths. Expected values are
			// spelled out as strings: the fake client's GetRegistry reports the
			// host only, so it cannot serve as an oracle.
			wantTarget := host + "/" + strings.Trim(tt.target, "/")
			wantRoot := host + "/" + tt.wantRoot
			assert.Equal(t, tt.wantEdition, svc.pluginsPath.Edition)
			assert.Equal(t, wantTarget, svc.pluginsPath.Target)
			assert.Equal(t, wantRoot, svc.pluginsPath.Root)
			assert.Equal(t, wantRoot+"/deckhouse-cli/plugins", svc.pluginsPath.Path)
			assert.Equal(t, tt.wantEdition != "", svc.pluginsPath.Moved())

			// Plugin image and discovery tag live at the root.
			pluginClient := rootClient.WithSegment("deckhouse-cli", "plugins", pluginName)
			assert.NoErrorf(t, pluginClient.CheckImageExists(ctx, pluginTag),
				"plugin image must exist at %s/deckhouse-cli/plugins/%s:%s", wantRoot, pluginName, pluginTag)

			catalogClient := rootClient.WithSegment("deckhouse-cli", "plugins")
			tags, err := catalogClient.ListTags(ctx)
			require.NoError(t, err)
			assert.Containsf(t, tags, pluginName, "discovery tag must exist at %s/deckhouse-cli/plugins", wantRoot)

			// The platform layout is unaffected.
			installClient := targetClient.WithSegment("install")
			assert.NoErrorf(t, installClient.CheckImageExists(ctx, installTag),
				"install layout must stay at %s/install", wantTarget)

			if tt.wantEdition == "" {
				return
			}

			// An edition was cut off: plugins must MOVE above it, not be copied,
			// so nothing is left under <target>/deckhouse-cli.
			underEdition := targetClient.WithSegment("deckhouse-cli", "plugins", pluginName)
			assert.Errorf(t, underEdition.CheckImageExists(ctx, pluginTag),
				"plugin must not remain under the edition at %s/deckhouse-cli/plugins/%s", wantTarget, pluginName)

			editionTags, err := targetClient.WithSegment("deckhouse-cli", "plugins").ListTags(ctx)
			require.NoError(t, err)
			assert.Empty(t, editionTags, "no discovery tag may remain under the edition")
		})
	}
}

// TestPushService_PluginLayoutIsNotPlatform pins the summary accounting for a
// plugin-only bundle: the plugin layout is counted by the plugins index step,
// never as a platform layout (which is what an unclassified segment would be).
func TestPushService_PluginLayoutIsNotPlatform(t *testing.T) {
	const (
		host       = "registry.example.com"
		pluginName = "postgresql-mgr"
		pluginTag  = "v1.2.0"
	)

	bundleDir := t.TempDir()
	pluginPkg := buildLayoutBundle(t, bundleDir, "plugin-"+pluginName+".tar",
		path.Join("deckhouse-cli", "plugins", pluginName), pluginTag)

	hostClient := pkgclient.Adapt(upfake.NewClient(upfake.NewRegistry(host)))

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := NewPushService(hostClient, &PushServiceOptions{
		TargetPath: "/deckhouse/ee",
		Packages:   []string{pluginPkg},
		WorkingDir: t.TempDir(),
	}, logger, userLogger)

	summary, err := svc.Push(context.Background())
	require.NoError(t, err, "push must succeed")

	assert.Equal(t, 1, summary.Plugins, "one plugin repository pushed")
	assert.False(t, summary.PlatformPushed, "a plugin layout must not be classified as platform")
	assert.False(t, summary.InstallerPushed)
	assert.Zero(t, summary.Modules)
	assert.Zero(t, summary.Packages)
	assert.Zero(t, summary.SecurityDatabases)
}

// TestPushService_ModulesPathSuffixIntoPluginsNamespace pins what the routing
// keys on: layouts go to the plugins root by their bundle segment, not by the
// path they end up at. A --modules-path-suffix that names the CLI plugins
// segment therefore moves modules inside the target, never above its edition,
// and their images and discovery tag stay together.
func TestPushService_ModulesPathSuffixIntoPluginsNamespace(t *testing.T) {
	const (
		host       = "registry.example.com"
		moduleName = "postgresql"
		moduleTag  = "v1.5.0"
	)

	bundleDir := t.TempDir()
	modulePkg := buildLayoutBundle(t, bundleDir, "module-"+moduleName+".tar",
		path.Join("modules", moduleName), moduleTag)

	hostClient := pkgclient.Adapt(upfake.NewClient(upfake.NewRegistry(host)))

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := NewPushService(hostClient, &PushServiceOptions{
		TargetPath:        "/deckhouse/ee",
		Packages:          []string{modulePkg},
		WorkingDir:        t.TempDir(),
		ModulesPathSuffix: "deckhouse-cli",
	}, logger, userLogger)

	summary, err := svc.Push(context.Background())
	require.NoError(t, err, "push must succeed")
	assert.Equal(t, 1, summary.Modules, "one module pushed")

	ctx := context.Background()

	underEdition := hostClient.WithSegment("deckhouse", "ee", "deckhouse-cli")
	assert.NoError(t, underEdition.WithSegment(moduleName).CheckImageExists(ctx, moduleTag),
		"module images must stay under the target")

	tags, err := underEdition.ListTags(ctx)
	require.NoError(t, err)
	assert.Contains(t, tags, moduleName, "the discovery tag must sit next to the images")

	aboveEdition := hostClient.WithSegment("deckhouse", "deckhouse-cli", moduleName)
	assert.Error(t, aboveEdition.CheckImageExists(ctx, moduleTag),
		"a module must never be written above the edition")
}

// denyWritesClient refuses writes to the repositories denies() selects with
// HTTP 403, the way a registry scopes credentials to one path. Reads and
// other writes go to the wrapped fake.
type denyWritesClient struct {
	dkpreg.Client
	// repo is the full repository reference of this scope. Tracked here
	// because the fake client's GetRegistry reports the host only.
	repo   string
	denies func(repo string) bool
}

func newDenyWritesClient(host string, denies func(repo string) bool) *denyWritesClient {
	return &denyWritesClient{
		Client: pkgclient.Adapt(upfake.NewClient(upfake.NewRegistry(host))),
		repo:   host,
		denies: denies,
	}
}

func (c *denyWritesClient) WithSegment(segments ...string) dkpreg.Client {
	return &denyWritesClient{
		Client: c.Client.WithSegment(segments...),
		repo:   path.Join(append([]string{c.repo}, segments...)...),
		denies: c.denies,
	}
}

func (c *denyWritesClient) denied() error {
	if c.denies(c.repo) {
		return &transport.Error{StatusCode: http.StatusForbidden}
	}

	return nil
}

func (c *denyWritesClient) PushImage(ctx context.Context, tag string, img v1.Image, opts ...dkpreg.ImagePushOption) error {
	if err := c.denied(); err != nil {
		return err
	}

	return c.Client.PushImage(ctx, tag, img, opts...)
}

func (c *denyWritesClient) PushIndex(ctx context.Context, tag string, idx v1.ImageIndex, opts ...dkpreg.ImagePushOption) error {
	if err := c.denied(); err != nil {
		return err
	}

	return c.Client.PushIndex(ctx, tag, idx, opts...)
}

// TestPushService_PluginsRootDenied verifies that a registry refusing the
// write above the edition (credentials scoped to the target path) surfaces as
// a PluginsRootError naming the refused repository and the target it sits
// above, with the HTTP status still reachable through the chain. Without an
// edition to cut off, the plugins path is under the target like everything
// else and the plain error passes through.
//
// The denial is placed on the plugins catalog (the discovery-tag step), which
// fails at once; the plugin layout push goes through the pusher's retries and
// is covered by TestPushService_PluginsRootDenied_LayoutPush.
func TestPushService_PluginsRootDenied(t *testing.T) {
	const (
		host       = "registry.example.com"
		pluginName = "postgresql-mgr"
		pluginTag  = "v1.2.0"
	)

	tests := []struct {
		name        string
		target      string
		root        string
		wantWrapped bool
	}{
		{name: "edition target: error names the root and the edition", target: "/deckhouse/ee", root: "deckhouse", wantWrapped: true},
		{name: "no edition: plain error", target: "/deckhouse", root: "deckhouse", wantWrapped: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bundleDir := t.TempDir()
			pluginPkg := buildLayoutBundle(t, bundleDir, "plugin-"+pluginName+".tar",
				path.Join("deckhouse-cli", "plugins", pluginName), pluginTag)

			catalog := path.Join(host, tt.root, "deckhouse-cli", "plugins")
			// Deny the catalog repo itself, not its sub-repositories.
			hostClient := newDenyWritesClient(host, func(repo string) bool { return repo == catalog })

			logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
			userLogger := log.NewSLogger(slog.LevelWarn)

			svc := NewPushService(hostClient, &PushServiceOptions{
				TargetPath: tt.target,
				Packages:   []string{pluginPkg},
				WorkingDir: t.TempDir(),
			}, logger, userLogger)

			_, err := svc.Push(context.Background())
			require.Error(t, err)

			var transportErr *transport.Error
			require.ErrorAs(t, err, &transportErr, "the registry status must stay reachable")
			assert.Equal(t, http.StatusForbidden, transportErr.StatusCode)

			var rootErr *PluginsRootError
			if !tt.wantWrapped {
				assert.False(t, errors.As(err, &rootErr), "no edition cut off: nothing to explain")
				return
			}

			require.ErrorAs(t, err, &rootErr)
			assert.Equal(t, catalog, rootErr.Repo)
			assert.Equal(t, "ee", rootErr.Report.Edition)
			assert.Equal(t, path.Join(host, "deckhouse", "ee"), rootErr.Report.Target)
			assert.Equal(t, path.Join(host, "deckhouse"), rootErr.Report.Root)
		})
	}
}

// TestPushService_PluginsRootDenied_LayoutPush is the same denial on the
// plugin image repository, the first write above the edition a push makes.
// It goes through the pusher's retry schedule, so it is skipped in -short.
func TestPushService_PluginsRootDenied_LayoutPush(t *testing.T) {
	if testing.Short() {
		t.Skip("walks the pusher's retry schedule")
	}

	const (
		host       = "registry.example.com"
		pluginName = "postgresql-mgr"
		pluginTag  = "v1.2.0"
	)

	bundleDir := t.TempDir()
	pluginPkg := buildLayoutBundle(t, bundleDir, "plugin-"+pluginName+".tar",
		path.Join("deckhouse-cli", "plugins", pluginName), pluginTag)

	deniedPrefix := path.Join(host, "deckhouse", "deckhouse-cli")
	hostClient := newDenyWritesClient(host, func(repo string) bool { return strings.HasPrefix(repo, deniedPrefix) })

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := NewPushService(hostClient, &PushServiceOptions{
		TargetPath: "/deckhouse/ee",
		Packages:   []string{pluginPkg},
		WorkingDir: t.TempDir(),
	}, logger, userLogger)

	_, err := svc.Push(context.Background())
	require.Error(t, err)

	var rootErr *PluginsRootError
	require.ErrorAs(t, err, &rootErr)
	assert.Equal(t, path.Join(host, "deckhouse", "deckhouse-cli", "plugins", pluginName), rootErr.Repo)
	assert.Equal(t, "ee", rootErr.Report.Edition)

	var transportErr *transport.Error
	require.ErrorAs(t, err, &transportErr)
	assert.Equal(t, http.StatusForbidden, transportErr.StatusCode)
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
