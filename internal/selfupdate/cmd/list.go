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

package selfupdatecmd

import (
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/fatih/color"
	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/selfupdate"
	"github.com/deckhouse/deckhouse-cli/internal/version"
)

func newVersionsCommand(logger *dkplog.Logger) *cobra.Command {
	return &cobra.Command{
		Use:     "versions",
		Aliases: []string{"list"},
		Short:   "List deckhouse-cli versions available in the cluster registry",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			updater, err := newUpdater(cmd.Context(), cmd, logger)
			if err != nil {
				return err
			}

			versions, err := updater.Versions(cmd.Context())
			if err != nil {
				return err
			}

			if len(versions) == 0 {
				return fmt.Errorf("no deckhouse-cli versions found in the registry")
			}

			store, storeErr := selfupdate.NewStore()
			if storeErr != nil {
				// Listing still works; entries just lose their "installed" marker.
				logger.Debug("version store unavailable", dkplog.Err(storeErr))
			}

			installed := store.List()

			// For a store-managed install the `current` symlink names the active
			// version reliably even when the binary was built without version
			// ldflags; trust it only when this invocation runs through the store.
			current := version.Version

			if exePath, err := selfupdate.CurrentExecutable(); err == nil && store.Contains(exePath) {
				if tag := store.CurrentTag(); tag != "" {
					current = tag
				}
			}

			lines, currentListed := formatVersionList(versions, current, installed)
			for _, line := range lines {
				fmt.Println(line)
			}

			if extra := storedOnly(installed, versions); len(extra) > 0 {
				fmt.Println("\nInstalled locally (switch with 'd8 cli use'), not published in the registry:")

				for _, v := range extra {
					fmt.Printf("  %s\n", v.Original())
				}
			}

			if !currentListed {
				fmt.Printf("\nCurrent version %s is not published in the registry.\n", current)
			}

			return nil
		},
	}
}

// formatVersionList renders the version list newest-first: versions newer than
// current are green, the current one is starred and cyan, older ones are dimmed.
// Versions present in the local store carry an "installed" marker - `d8 cli use`
// switches to them without a download. A non-semver current (dev build) produces
// a plain uncolored list. Reports whether the current version appeared in the list.
func formatVersionList(versions []*semver.Version, current string, installed []*semver.Version) ([]string, bool) {
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

	inStore := func(v *semver.Version) bool {
		for _, s := range installed {
			if s.Equal(v) {
				return true
			}
		}

		return false
	}

	currentVersion, err := semver.NewVersion(current)

	lines := make([]string, 0, len(versions))

	for _, v := range versions {
		marker := ""
		if inStore(v) {
			marker = "  installed"
		}

		var entry string

		switch {
		case err != nil:
			// Dev build - no reference point, no grouping.
			entry = fmt.Sprintf("  %-*s%s", widest, v.Original(), marker)
		case v.Equal(currentVersion):
			listed = true
			entry = actual.Sprintf("* %-*s  current%s", widest, v.Original(), marker)
		case v.GreaterThan(currentVersion):
			entry = newer.Sprintf("  %-*s  newer%s", widest, v.Original(), marker)
		default:
			entry = older.Sprintf("  %-*s%s", widest, v.Original(), marker)
		}

		// The padding is for the trailing group word; entries without one would
		// otherwise carry invisible trailing spaces.
		lines = append(lines, strings.TrimRight(entry, " "))
	}

	return lines, listed
}

// storedOnly returns stored versions absent from the published list (the registry
// was re-pointed or pruned); they remain switchable via `d8 cli use`.
func storedOnly(installed, published []*semver.Version) []*semver.Version {
	extra := make([]*semver.Version, 0, len(installed))

	for _, s := range installed {
		found := false

		for _, p := range published {
			if p.Equal(s) {
				found = true

				break
			}
		}

		if !found {
			extra = append(extra, s)
		}
	}

	return extra
}
