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

package distcmd

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderSummaryFull(t *testing.T) {
	withoutColor(t)

	out := renderSummary(summaryData{
		current: "v0.33.5",
		latest:  "v0.34.0",
		newer:   true,
		plugins: []pluginRow{
			{name: "dk", installed: "0.5.1", latest: "0.5.1", status: statusCurrent},
			{name: "system", installed: "1.2.0", latest: "1.3.0", status: statusOutdated},
		},
	})

	assert.Equal(t, strings.Join([]string{
		"deckhouse-cli (d8)",
		"  Version:  v0.33.5",
		"  Latest:   v0.34.0  update available - run 'd8 dist update'",
		"",
		"Plugins (2 installed):",
		"  NAME    VERSION  LATEST  STATUS",
		"  dk      0.5.1    0.5.1   up to date",
		"  system  1.2.0    1.3.0   update available",
		"Update a plugin with 'd8 dist plugins install <name>' or 'd8 dist plugins install --all'.",
		"",
	}, "\n"), out)
}

func TestRenderSummaryUpToDate(t *testing.T) {
	withoutColor(t)

	out := renderSummary(summaryData{
		current: "v0.34.0",
		latest:  "v0.34.0",
		plugins: []pluginRow{
			{name: "dk", installed: "0.5.1", latest: "0.5.1", status: statusCurrent},
		},
	})

	assert.Contains(t, out, "Latest:   v0.34.0  up to date")
	assert.NotContains(t, out, "Update a plugin", "no update hint when nothing is outdated")
}

func TestRenderSummaryDegraded(t *testing.T) {
	withoutColor(t)

	out := renderSummary(summaryData{
		current:       "v0.33.5",
		offlineReason: "set up kubernetes client: no kubeconfig",
		plugins: []pluginRow{
			{name: "dk", installed: "0.5.1"},
		},
	})

	assert.Equal(t, strings.Join([]string{
		"deckhouse-cli (d8)",
		"  Version:  v0.33.5",
		"",
		"Plugins (1 installed):",
		"  NAME  VERSION",
		"  dk    0.5.1",
		"",
		"Warning: could not check for updates - cluster unreachable.",
		"  (set up kubernetes client: no kubeconfig)",
		"",
	}, "\n"), out)
}

func TestRenderSummaryNoPlugins(t *testing.T) {
	withoutColor(t)

	out := renderSummary(summaryData{current: "v0.33.5", latest: "v0.33.5"})

	assert.Contains(t, out, "Plugins: none installed")
	assert.Contains(t, out, "Install with 'd8 dist plugins install <name>'.")
}

func TestRenderSummaryUnknownPluginFreshness(t *testing.T) {
	withoutColor(t)

	// One plugin's tag listing failed: its row degrades to "?"/unknown, the
	// summary itself stays fresh.
	out := renderSummary(summaryData{
		current: "v0.33.5",
		latest:  "v0.33.5",
		plugins: []pluginRow{
			{name: "dk", installed: "0.5.1", status: statusUnknown},
		},
	})

	assert.Contains(t, out, "  dk    0.5.1    ?       unknown")
	assert.NotContains(t, out, "Warning:")
}
