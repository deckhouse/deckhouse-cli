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

package platform

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal"
)

// TestAsList_ReturnsDefinedSubLayouts is a regression test for a bug where
// ImageLayouts.AsList used reflection to look for layout.Path-typed fields,
// which the struct does not have (its sub-layouts are *regimage.ImageLayout).
// The reflection loop therefore matched nothing and AsList returned an empty
// slice, which in turn made the end-of-pull summary report "0 images" for the
// platform even after a successful pull.
func TestAsList_ReturnsDefinedSubLayouts(t *testing.T) {
	tmp := t.TempDir()
	l := NewImageLayouts(tmp)

	require.Empty(t, l.AsList(), "no sub-layouts defined yet")

	mirrorTypes := []internal.MirrorType{
		internal.MirrorTypeDeckhouse,
		internal.MirrorTypeDeckhouseInstall,
		internal.MirrorTypeDeckhouseInstallStandalone,
		internal.MirrorTypeDeckhouseReleaseChannels,
	}
	for _, mt := range mirrorTypes {
		require.NoError(t, l.setLayoutByMirrorType(tmp, mt))
	}

	require.Len(t, l.AsList(), len(mirrorTypes),
		"every defined sub-layout must be returned by AsList")
}
