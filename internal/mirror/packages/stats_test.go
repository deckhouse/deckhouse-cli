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

package packages

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
)

// TestStats_DryRun verifies that, in dry-run mode, Stats reports the resolved
// package and its planned versions from the download list, without downloading
// any image blobs.
func TestStats_DryRun(t *testing.T) {
	reg := singlePackageRegistry(testPackageName, channelVersion, []string{channelVersion})

	svc := newServiceOpts(t, pkgclient.Adapt(upfake.NewClient(reg)), &Options{DryRun: true})

	require.NoError(t, svc.PullPackages(context.Background()))

	stats := svc.Stats()
	require.True(t, stats.Attempted)
	require.Len(t, stats.Packages, 1)
	require.Equal(t, testPackageName, stats.Packages[0].Name)
	require.Equal(t, []string{channelVersion}, stats.Packages[0].Versions)
}

// TestStats_RealPull_SurvivesPacking is the regression test for the bug where
// Stats reported 0 images after a successful real pull. The pack step deletes
// every OCI layout file as it tars it (see bundle.Pack), so counting manifests
// in Stats() - which runs after packing - read an emptied layout and returned
// zero. The fix captures the counts before packing; this test asserts the
// package's image count is non-zero after a full PullPackages (pull + pack).
func TestStats_RealPull_SurvivesPacking(t *testing.T) {
	reg := singlePackageRegistry(testPackageName, channelVersion, []string{channelVersion})

	svc := newServiceOpts(t, pkgclient.Adapt(upfake.NewClient(reg)), &Options{SkipVexImages: true})

	require.NoError(t, svc.PullPackages(context.Background()))

	stats := svc.Stats()
	require.True(t, stats.Attempted)
	require.Len(t, stats.Packages, 1)
	require.Equal(t, testPackageName, stats.Packages[0].Name)
	require.Greater(t, stats.Packages[0].Images, 0,
		"package image count must survive packing (captured before bundle.Pack deletes the layout)")
	require.Contains(t, stats.Packages[0].Versions, channelVersion)
}
