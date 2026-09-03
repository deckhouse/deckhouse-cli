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
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/plugins"
)

// newVersionsCommand returns `d8 dist plugins versions <name>` - list all
// published versions of one plugin, the same verb `d8 dist versions` uses for
// the CLI itself.
func newVersionsCommand(manager *plugins.Manager) *cobra.Command {
	return &cobra.Command{
		Use:   "versions <plugin-name>",
		Short: "List all versions of a plugin",
		Long: "List all published versions of a plugin, newest first. The installed version is\n" +
			"marked, versions newer than it are highlighted.\n\n" +
			"A plugin is published one image per platform, so a release reaches the registry as\n" +
			"several tags. They are collapsed into one line per version, listing the platforms\n" +
			"that version was built for.\n\n" +
			"Versions are fetched by the plugin's name through the registry-packages-proxy, so no\n" +
			"catalog access is needed. Install a specific version with\n" +
			"'d8 dist plugins install <name> --version X' - a version already on disk is switched to\n" +
			"instantly, without a download.",
		Args: cobra.ExactArgs(1),
		ValidArgsFunction: func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}

			// Completion must stay instant and offline, so it offers the installed
			// plugins (read from disk) rather than reaching the registry for the
			// published set.
			names, err := manager.InstalledPluginNames()
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}

			completions := make([]string, 0, len(names))

			for _, name := range names {
				if strings.HasPrefix(name, toComplete) {
					completions = append(completions, name)
				}
			}

			return completions, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			if err := plugins.ValidatePluginName(pluginName); err != nil {
				return err
			}

			versions, err := manager.PublishedVersions(cmd.Context(), pluginName)
			if err != nil {
				return err
			}

			if len(versions) == 0 {
				return fmt.Errorf("no versions found for plugin %q", pluginName)
			}

			current := manager.InstalledVersionOrNil(pluginName)

			lines, currentListed := formatPluginVersionList(versions, current)
			for _, line := range lines {
				fmt.Println(line)
			}

			if current != nil && !currentListed {
				fmt.Printf("\nInstalled version %s is not published in the registry.\n", current.Original())
			}

			return nil
		},
	}
}

// groupColumnWidth is the width of the trailing group word ("current"/"newer"),
// held fixed so the platform lists that follow it line up into a column.
const groupColumnWidth = len("current")

// formatPluginVersionList renders the version list newest-first: versions newer
// than the installed one are green, the installed one is starred and cyan,
// older ones are dimmed - the same grouping `d8 dist versions` uses. A nil
// current (plugin not installed, version unknown) produces a plain uncolored
// list. Each line carries the platforms that version was published for.
// Reports whether current appeared in the list.
func formatPluginVersionList(versions []plugins.PluginVersion, current *semver.Version) ([]string, bool) {
	var (
		newer  = color.New(color.FgGreen)
		actual = color.New(color.FgCyan, color.Bold)
		older  = color.New(color.Faint)
		listed bool
		widest int
	)

	for _, version := range versions {
		if len(version.Version.Original()) > widest {
			widest = len(version.Version.Original())
		}
	}

	lines := make([]string, 0, len(versions))

	for _, version := range versions {
		var (
			tint   *color.Color
			group  string
			marker = " "
		)

		switch {
		case current == nil:
			// Left uncolored and ungrouped: with no installed version to compare
			// against, no entry is newer, older or current.
		case version.Version.Equal(current):
			listed, tint, group, marker = true, actual, "current", "*"
		case version.Version.GreaterThan(current):
			tint, group = newer, "newer"
		default:
			tint = older
		}

		entry := fmt.Sprintf("%s %-*s  %-*s  %s",
			marker, widest, version.Version.Original(),
			groupColumnWidth, group, strings.Join(version.Platforms, ", "))

		// Both columns are padded to a fixed width so the platform lists line up;
		// a row missing the group word or the platforms would otherwise carry
		// invisible trailing spaces. A plugin published without platform suffixes
		// has every platform list empty, and the output collapses back to the bare
		// version plus its group word.
		entry = strings.TrimRight(entry, " ")

		if tint != nil {
			entry = tint.Sprint(entry)
		}

		lines = append(lines, entry)
	}

	return lines, listed
}
