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

package rpp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCLIImage(t *testing.T) {
	ref := CLIImage()

	assert.Equal(t, "deckhouse-cli", ref.String())
	assert.Equal(t, "/v1/images/deckhouse-cli/tags", ref.tagsPath())
	assert.Equal(t, "/v1/images/deckhouse-cli/images/v1.2.3", ref.imagePath("v1.2.3"))
}

func TestPluginImage(t *testing.T) {
	t.Run("valid name", func(t *testing.T) {
		ref, err := PluginImage("stronghold")
		require.NoError(t, err)

		assert.Equal(t, "deckhouse-cli/plugins/stronghold", ref.String())
		assert.Equal(t, "/v1/images/deckhouse-cli/plugins/stronghold/tags", ref.tagsPath())
		assert.Equal(t, "/v1/images/deckhouse-cli/plugins/stronghold/images/v2.0.0", ref.imagePath("v2.0.0"))
	})

	t.Run("empty name is rejected", func(t *testing.T) {
		_, err := PluginImage("")
		require.ErrorIs(t, err, ErrInvalidImage)
	})

	t.Run("name with slash is rejected", func(t *testing.T) {
		_, err := PluginImage("plugins/extra")
		require.ErrorIs(t, err, ErrInvalidImage)
	})

	t.Run("names outside the OCI component grammar are rejected", func(t *testing.T) {
		for _, name := range []string{"..", "name?x=y", "name#f", "Name", "na me", "-lead", "trail-"} {
			_, err := PluginImage(name)
			assert.ErrorIs(t, err, ErrInvalidImage, "name %q must be rejected", name)
		}
	})

	t.Run("separator-joined names are accepted", func(t *testing.T) {
		for _, name := range []string{"delivery-kit", "my_plugin", "v1.plugin"} {
			_, err := PluginImage(name)
			assert.NoError(t, err, "name %q is a valid OCI component", name)
		}
	})
}

func TestValidateTag(t *testing.T) {
	t.Run("valid tag", func(t *testing.T) {
		require.NoError(t, validateTag("v1.2.3"))
	})

	t.Run("empty tag is rejected", func(t *testing.T) {
		require.ErrorIs(t, validateTag(""), ErrInvalidImage)
	})

	t.Run("tag with slash is rejected", func(t *testing.T) {
		require.ErrorIs(t, validateTag("v1/2"), ErrInvalidImage)
	})

	t.Run("tags outside the OCI grammar are rejected", func(t *testing.T) {
		// URL metacharacters and a leading separator would alter the request route.
		for _, tag := range []string{"v1?x=y", "v1#frag", "..", ".hidden", "v 1", "v1.2.3+meta"} {
			assert.ErrorIs(t, validateTag(tag), ErrInvalidImage, "tag %q must be rejected", tag)
		}
	})
}
