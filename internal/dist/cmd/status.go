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
	"context"
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/plugins"
	pluginflags "github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
	"github.com/deckhouse/deckhouse-cli/internal/selfupdate"
)

// pluginStatus is the freshness verdict for one installed plugin.
type pluginStatus int

const (
	statusUnknown pluginStatus = iota // no freshness data (cluster unreachable or lookup failed)
	statusCurrent
	statusOutdated
)

type pluginRow struct {
	name      string
	installed string
	latest    string // "" when unknown
	status    pluginStatus
}

// summaryData is everything the summary renders. Freshness fields stay empty
// when the cluster was unreachable; offlineReason then carries the cause.
type summaryData struct {
	current       string
	latest        string // "" when unknown
	newer         bool
	plugins       []pluginRow
	offlineReason string
}

// newStatusCommand returns `d8 dist status` - a summary of the distribution:
// the running d8 version and installed plugins, with update status from the
// registry-packages-proxy. A cluster/proxy failure degrades the summary to
// local data instead of failing the command.
func newStatusCommand(logger *dkplog.Logger) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the distribution status: the d8 version, plugins, what is outdated",
		Long: "Show the state of the d8 distribution: the running deckhouse-cli version and the\n" +
			"installed plugins, with update status from the registry-packages-proxy.\n\n" +
			"When the cluster is unreachable, prints the local data and a warning instead of failing.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Print(renderSummary(collectSummary(cmd.Context(), cmd, logger)))

			return nil
		},
	}
}

// collectSummary gathers the local state (always) and the freshness state
// (best-effort): the first cluster/proxy failure switches the summary to the
// degraded, local-only form.
func collectSummary(ctx context.Context, cmd *cobra.Command, logger *dkplog.Logger) summaryData {
	store, err := selfupdate.NewStore()
	if err != nil {
		// A nil store is fine here: the summary then reports the compiled-in version.
		logger.Debug("version store unavailable", dkplog.Err(err))
	}

	data := summaryData{current: activeVersionTag(store)}

	manager := plugins.NewManager(logger.Named("plugins"))

	// The summary is read-only: resolve the root that actually holds an
	// install instead of creating one (EnsureInstallRoot is for the mutating
	// plugins commands). No install anywhere = no plugins, silently.
	if root, ok := installedPluginsRoot(); ok {
		manager.SetDirectory(root)

		for _, p := range manager.List() {
			data.plugins = append(data.plugins, pluginRow{name: p.Name, installed: p.Version})
		}
	}

	updater, err := newUpdater(ctx, cmd, logger)
	if err != nil {
		data.offlineReason = err.Error()

		return data
	}

	latest, newer, err := updater.LatestVersion(ctx, data.current)
	if err != nil {
		data.offlineReason = err.Error()

		return data
	}

	data.latest, data.newer = latest, newer

	if len(data.plugins) == 0 {
		return data
	}

	if err := manager.InitPluginServices(ctx); err != nil {
		data.offlineReason = err.Error()

		return data
	}

	// Per-plugin freshness uses the machinery's own notion of "latest" (the
	// highest stable version, the one a default install would pick). One
	// failed lookup leaves that row unknown, not the whole summary degraded.
	for i := range data.plugins {
		row := &data.plugins[i]

		latest, err := manager.LatestVersion(ctx, row.name)
		if err != nil {
			logger.Debug("plugin freshness unavailable", dkplog.Err(err))

			continue
		}

		row.latest = latest.Original()
		row.status = statusCurrent

		if installed, err := semver.NewVersion(row.installed); err == nil && latest.GreaterThan(installed) {
			row.status = statusOutdated
		}
	}

	return data
}

// renderSummary renders the summary. Pure: all data is in summaryData, so the
// layout is unit-testable. Verdicts are worded in plain text, not by colour
// alone (colour is lost in piped output and to colour-blind readers).
func renderSummary(d summaryData) string {
	var b strings.Builder

	fmt.Fprintf(&b, "%s\n", sumTitle("deckhouse-cli (d8)"))
	fmt.Fprintf(&b, "  %s %s\n", sumLabel(fmt.Sprintf("%-9s", "Version:")), verCur.Sprint(d.current))

	if d.latest != "" {
		if d.newer {
			fmt.Fprintf(&b, "  %s %s  %s\n",
				sumLabel(fmt.Sprintf("%-9s", "Latest:")), verNew.Sprint(d.latest),
				sumWarn("update available - run 'd8 dist update'"))
		} else {
			fmt.Fprintf(&b, "  %s %s  %s\n",
				sumLabel(fmt.Sprintf("%-9s", "Latest:")), sumDim(d.latest), sumGood("up to date"))
		}
	}

	b.WriteString("\n")
	writePluginsSection(&b, d)

	if d.offlineReason != "" {
		fmt.Fprintf(&b, "\n%s\n", sumWarn("Warning: could not check for updates - cluster unreachable."))
		fmt.Fprintf(&b, "%s\n", sumDim("  ("+d.offlineReason+")"))
	}

	return b.String()
}

// writePluginsSection renders the installed-plugins table. The LATEST/STATUS
// columns appear only when freshness was checked (the summary is not degraded).
func writePluginsSection(b *strings.Builder, d summaryData) {
	if len(d.plugins) == 0 {
		fmt.Fprintf(b, "%s none installed\n", sumTitle("Plugins:"))
		fmt.Fprintf(b, "%s\n", sumDim("Install with 'd8 dist plugins install <name>'."))

		return
	}

	fmt.Fprintf(b, "%s\n", sumTitle(fmt.Sprintf("Plugins (%d installed):", len(d.plugins))))

	withFreshness := d.offlineReason == ""

	nameW, verW, latestW := len("NAME"), len("VERSION"), len("LATEST")
	for _, p := range d.plugins {
		nameW = max(nameW, len(p.name))
		verW = max(verW, len(p.installed))
		latestW = max(latestW, len(latestCell(p)))
	}

	if withFreshness {
		fmt.Fprintf(b, "  %s\n", sumDim(fmt.Sprintf("%-*s  %-*s  %-*s  %s", nameW, "NAME", verW, "VERSION", latestW, "LATEST", "STATUS")))
	} else {
		fmt.Fprintf(b, "  %s\n", sumDim(fmt.Sprintf("%-*s  %s", nameW, "NAME", "VERSION")))
	}

	outdated := false

	for _, p := range d.plugins {
		if !withFreshness {
			fmt.Fprintf(b, "  %-*s  %s\n", nameW, p.name, p.installed)

			continue
		}

		latest := fmt.Sprintf("%-*s", latestW, latestCell(p))

		var status string

		switch p.status {
		case statusOutdated:
			outdated = true
			latest = verNew.Sprint(latest)
			status = sumWarn("update available")
		case statusCurrent:
			status = sumGood("up to date")
		default:
			status = sumDim("unknown")
		}

		fmt.Fprintf(b, "  %-*s  %-*s  %s  %s\n", nameW, p.name, verW, p.installed, latest, status)
	}

	if outdated {
		fmt.Fprintf(b, "%s\n", sumDim("Update a plugin with 'd8 dist plugins install <name>' or 'd8 dist plugins install --all'."))
	}
}

// latestCell is the LATEST column value: "?" when the lookup failed.
func latestCell(p pluginRow) string {
	if p.latest == "" {
		return "?"
	}

	return p.latest
}

// installedPluginsRoot reports the plugins root that actually holds an
// install: the configured root, or the home fallback (~/.deckhouse-cli) -
// the same resolution `plugins install --all` and the root command's plugin
// override use. ok=false means no plugins are installed anywhere.
func installedPluginsRoot() (string, bool) {
	return layout.ResolveInstallRoot(pluginflags.DeckhousePluginsDir)
}
