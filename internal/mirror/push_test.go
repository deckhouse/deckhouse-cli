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
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	client "github.com/deckhouse/deckhouse/pkg/registry"
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

// scopeToRepo scopes the client to a slash-separated repo path, skipping empty
// components. An empty path leaves the client at the target repo root.
func scopeToRepo(c client.Client, repoPath string) client.Client {
	for _, seg := range strings.Split(repoPath, "/") {
		if seg == "" {
			continue
		}

		c = c.WithSegment(seg)
	}

	return c
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
			require.NoError(t, svc.Push(context.Background()), "push must succeed")

			ctx := context.Background()

			// Module images land at <repo>/<wantModule>:<moduleTag>.
			moduleClient := scopeToRepo(destClient, tt.wantModule)
			assert.NoErrorf(t, moduleClient.CheckImageExists(ctx, moduleTag),
				"module image must exist at %s:%s", moduleClient.GetRegistry(), moduleTag)

			// Discovery tag lands at <repo>/<wantIndex>:<moduleName>.
			indexClient := scopeToRepo(destClient, tt.wantIndex)
			tags, err := indexClient.ListTags(ctx)
			require.NoError(t, err)
			assert.Containsf(t, tags, moduleName,
				"discovery tag %q must exist at %s", moduleName, indexClient.GetRegistry())

			// A non-default suffix must MOVE modules, not copy them: the default
			// modules/ path must hold nothing.
			if tt.wantModule != "modules/"+moduleName {
				assert.Errorf(t, scopeToRepo(destClient, "modules/"+moduleName).CheckImageExists(ctx, moduleTag),
					"module must not remain at default modules/%s", moduleName)
			}

			// The non-module layout is unaffected by the suffix.
			assert.NoErrorf(t, scopeToRepo(destClient, "install").CheckImageExists(ctx, installTag),
				"install layout must stay at <repo>/install regardless of suffix")
		})
	}
}
