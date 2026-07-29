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

// newVersionsCommand returns `d8 plugins versions <name>` - list all
// published versions of one plugin, the same verb `d8 cli versions` uses for
// the CLI itself.
func newVersionsCommand(manager *plugins.Manager) *cobra.Command {
	return &cobra.Command{
		Use:   "versions <plugin-name>",
		Short: "List all versions of a plugin",
		Long: "List all published versions of a plugin, newest first. The installed version is\n" +
			"marked, versions newer than it are highlighted.\n\n" +
			"Versions are fetched by the plugin's name through the registry-packages-proxy, so no\n" +
			"catalog access is needed. Install a specific version with\n" +
			"'d8 plugins install <name> --version X' - a version already on disk is switched to\n" +
			"instantly, without a download.",
		Args: cobra.ExactArgs(1),
		ValidArgsFunction: func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}

			// Completion must stay instant and offline, so it offers the installed
			// plugins (read from disk); the remote catalog is not available through
			// the rpp source anyway.
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

// formatPluginVersionList renders the version list newest-first: versions newer
// than the installed one are green, the installed one is starred and cyan,
// older ones are dimmed - the same grouping `d8 cli versions` uses. A nil
// current (plugin not installed, version unknown) produces a plain uncolored
// list. Reports whether current appeared in the list.
func formatPluginVersionList(versions []*semver.Version, current *semver.Version) ([]string, bool) {
	var (
		newer  = color.New(color.FgGreen)
		actual = color.New(color.FgCyan, color.Bold)
		older  = color.New(color.Faint)
		listed bool
		widest int
	)

	for _, v := range versions {
		if len(v.Original()) > widest {
			widest = len(v.Original())
		}
	}

	lines := make([]string, 0, len(versions))

	for _, v := range versions {
		var entry string

		switch {
		case current == nil:
			entry = fmt.Sprintf("  %-*s", widest, v.Original())
		case v.Equal(current):
			listed = true
			entry = actual.Sprintf("* %-*s  current", widest, v.Original())
		case v.GreaterThan(current):
			entry = newer.Sprintf("  %-*s  newer", widest, v.Original())
		default:
			entry = older.Sprintf("  %-*s", widest, v.Original())
		}

		// The padding is for the trailing group word; entries without one would
		// otherwise carry invisible trailing spaces.
		lines = append(lines, strings.TrimRight(entry, " "))
	}

	return lines, listed
}
