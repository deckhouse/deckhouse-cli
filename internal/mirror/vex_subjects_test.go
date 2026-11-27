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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/vex/vextest"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

func TestPullVEXInModuleAndPackageRepositories(t *testing.T) {
	const version = "v1.0.0"
	const extraTag = "v2.0.0"

	component := upfake.NewImageBuilder().WithFile("bin", "component").MustBuild()
	componentDigest := mustDigest(t, component)
	main := upfake.NewImageBuilder().
		WithFile("images_digests.json", fmt.Sprintf(`{"component":%q}`, componentDigest)).
		WithFile("extra_images.json", `{"scanner":"`+extraTag+`"}`).
		MustBuild()
	extra := upfake.NewImageBuilder().WithFile("bin", "scanner").MustBuild()
	reg := upfake.NewRegistry(pullStubRootURL)

	catalogs := []string{"modules", "packages"}
	for _, catalog := range catalogs {
		reg.MustAddImage(catalog, "console", main)
	}

	registerSubject := func(suffix, tag string, subject v1.Image) string {
		index, err := vextest.ReferrersIndex(subject, `{"vex":"test"}`)
		require.NoError(t, err)
		indexTag := "sha256-" + mustDigest(t, subject).Hex
		for _, catalog := range catalogs {
			repo := catalog + "/console" + suffix
			reg.MustAddImage(repo, tag, subject)
			reg.MustAddIndex(repo, indexTag, index)
		}
		return indexTag
	}

	mainVEX := registerSubject("", version, main)
	componentVEX := registerSubject("", componentDigest.Hex, component)
	extraVEX := registerSubject("/extra/scanner", extraTag, extra)
	legacy, err := vextest.LegacyAttestation(`{"vex":"legacy"}`)
	require.NoError(t, err)
	for _, catalog := range catalogs {
		reg.MustAddImage(catalog+"/console/extra/scanner", extraVEX+".att", legacy)
	}

	filter, err := modules.NewFilter([]string{"console@=" + version}, modules.FilterTypeWhitelist)
	require.NoError(t, err)

	for _, tc := range []struct {
		name      string
		onlyExtra bool
		skipVEX   bool
		mainTags  []string
		extraTags []string
	}{
		{name: "all subjects", mainTags: []string{version, mainVEX, componentDigest.Hex, componentVEX}, extraTags: []string{extraTag, extraVEX, extraVEX + ".att"}},
		{name: "only extra images", onlyExtra: true, extraTags: []string{extraTag, extraVEX, extraVEX + ".att"}},
		{name: "skip VEX", skipVEX: true, mainTags: []string{version, componentDigest.Hex}, extraTags: []string{extraTag}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundleDir := t.TempDir()
			svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
				BundleDir:       bundleDir,
				SkipPlatform:    true,
				SkipInstaller:   true,
				SkipSecurity:    true,
				ModuleFilter:    filter,
				PackageFilter:   filter,
				OnlyExtraImages: tc.onlyExtra,
				SkipVexImages:   tc.skipVEX,
			})

			ctx := t.Context()
			_, err := svc.Pull(ctx)
			require.NoError(t, err)

			for _, kind := range []string{"module", "package"} {
				name := kind + "-console"
				archive, err := os.Open(filepath.Join(bundleDir, name+".tar"))
				require.NoError(t, err)
				target := t.TempDir()
				err = bundle.Unpack(ctx, archive, target, name)
				require.NoError(t, archive.Close())
				require.NoError(t, err)

				repo := filepath.Join(target, kind+"s", "console")
				assert.ElementsMatch(t, tc.mainTags, vexSubjectLayoutTags(t, repo), repo)
				assert.ElementsMatch(t, tc.extraTags, vexSubjectLayoutTags(t, filepath.Join(repo, "extra", "scanner")), repo+"/extra/scanner")
			}
		})
	}
}

func vexSubjectLayoutTags(t *testing.T, dir string) []string {
	t.Helper()

	index, err := layout.Path(dir).ImageIndex()
	require.NoError(t, err)
	manifest, err := index.IndexManifest()
	require.NoError(t, err)
	var tags []string
	for _, desc := range manifest.Manifests {
		tags = append(tags, desc.Annotations[regimage.AnnotationImageShortTag])
	}
	return tags
}
