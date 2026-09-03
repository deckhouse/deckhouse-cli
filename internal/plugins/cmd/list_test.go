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

package pluginscmd

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/deckhouse-cli/internal/plugins"
)

// TestPrintAvailablePluginsRendersNotes: a plugin whose version could not be
// resolved is still named, with the reason in its own row.
func TestPrintAvailablePluginsRendersNotes(t *testing.T) {
	var out bytes.Buffer

	printAvailablePlugins(&out, []plugins.RemotePluginInfo{
		{Name: "package", Version: "v0.0.34", Installed: true},
		{Name: "stronghold", Version: "v1.3.0"},
		{Name: "broken", Note: "no versions found"},
	}, nil)

	rendered := out.String()

	assert.Contains(t, rendered, "Available in the registry:")
	assert.Contains(t, rendered, "NAME                 LATEST          STATUS")
	assert.Contains(t, rendered, "package              v0.0.34         installed")
	assert.Contains(t, rendered, "stronghold           v1.3.0")
	assert.Contains(t, rendered, "broken                               no versions found")
	assert.Contains(t, rendered, "Total: 3 plugin(s) published")
}

// TestPrintAvailablePluginsNoneFound distinguishes "the registry answered, and it
// holds nothing" from a failure to ask.
func TestPrintAvailablePluginsNoneFound(t *testing.T) {
	var out bytes.Buffer

	printAvailablePlugins(&out, nil, nil)

	assert.Contains(t, out.String(), "No plugins found in the registry")
	assert.NotContains(t, out.String(), "Could not list")
}

// TestPrintAvailablePluginsReportsFailure: an unreachable registry replaces the
// table with its reason and nothing else - the installed half printed earlier stands.
func TestPrintAvailablePluginsReportsFailure(t *testing.T) {
	var out bytes.Buffer

	printAvailablePlugins(&out, nil, errors.New("route not allowed"))

	rendered := out.String()

	assert.Contains(t, rendered, "Could not list published plugins: route not allowed")
	assert.NotContains(t, rendered, "No plugins found")
	assert.NotContains(t, rendered, "Total:")
}

// TestPrintInstalledPluginsEmpty keeps the installed half honest when nothing is
// on disk, so an empty local list never reads as a failure.
func TestPrintInstalledPluginsEmpty(t *testing.T) {
	var out bytes.Buffer

	printInstalledPlugins(&out, nil)

	rendered := out.String()

	assert.Contains(t, rendered, "No plugins installed")
	assert.Contains(t, rendered, "Total: 0 plugin(s) installed")
}

// TestPrintAvailablePluginsHintsOnUnsupportedTransport: a transport that cannot
// enumerate must not dead-end - naming a plugin still works on every transport, and
// direct registry access can enumerate.
func TestPrintAvailablePluginsHintsOnUnsupportedTransport(t *testing.T) {
	var out bytes.Buffer

	printAvailablePlugins(&out, nil,
		fmt.Errorf("%w (%s)", plugins.ErrCatalogUnsupported, plugins.TransportRPP))

	rendered := out.String()

	assert.Contains(t, rendered, "not supported by this transport")
	assert.Contains(t, rendered, string(plugins.TransportRPP))
	assert.Contains(t, rendered, "d8 dist plugins versions <name>")
	assert.Contains(t, rendered, "--source")
}

// TestPrintAvailablePluginsNoHintOnOtherFailures keeps the workaround tied to the
// one cause it addresses, so a transport failure is not mislabeled.
func TestPrintAvailablePluginsNoHintOnOtherFailures(t *testing.T) {
	var out bytes.Buffer

	printAvailablePlugins(&out, nil, errors.New("proxy unreachable"))

	assert.NotContains(t, out.String(), "d8 dist plugins versions <name>")
}
