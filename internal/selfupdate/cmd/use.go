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
	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/selfupdate"
	"github.com/deckhouse/deckhouse-cli/internal/version"
)

// newUseCommand returns `d8 cli use <version>` - switch the d8 binary to a
// specific version by repointing the version store's `current` symlink,
// preferring locally installed versions over a download.
func newUseCommand(logger *dkplog.Logger) *cobra.Command {
	return &cobra.Command{
		Use:   "use <version>",
		Short: "Switch to a specific deckhouse-cli version (no download when it is installed locally)",
		Long: "Switch the d8 binary to the given version.\n\n" +
			"Versions live in the local store (~/.deckhouse-cli/cli/versions) and the active one is\n" +
			"selected by the store's 'current' symlink - the same layout plugins use. Switching to an\n" +
			"installed version repoints that symlink: instant, offline, no elevated privileges.\n" +
			"A version missing from the store is downloaded through the registry-packages-proxy first\n" +
			"(kubeconfig required) and stays installed afterwards.\n\n" +
			"The first switch migrates a plain-file install to the symlink layout; the original binary\n" +
			"is kept with a \".old\" suffix.",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeStoredVersions,
		RunE: func(cmd *cobra.Command, args []string) error {
			requested, err := semver.NewVersion(args[0])
			if err != nil {
				return fmt.Errorf("invalid version %q: %w", args[0], err)
			}

			exePath, err := selfupdate.CurrentExecutable()
			if err != nil {
				return err
			}

			store, err := selfupdate.NewStore()
			if err != nil {
				return fmt.Errorf("version store unavailable: %w", err)
			}

			// The `current` link is authoritative only when this very invocation runs
			// through the store; a foreign binary (e.g. a copied-off d8) must not
			// trust a link it is not part of.
			if store.Contains(exePath) {
				if tag := store.CurrentTag(); tag != "" {
					if cur, err := semver.NewVersion(tag); err == nil && cur.Equal(requested) {
						fmt.Printf("deckhouse-cli is already at %s.\n", verCur.Sprint(tag))

						return nil
					}
				}
			}

			// Installed locally - pure symlink switch, no network, no kubeconfig.
			if stored := store.Resolve(requested); stored != "" {
				res, err := selfupdate.SwitchTo(cmd.Context(), exePath, stored, store, logger, nil)
				if err != nil {
					return err
				}

				fmt.Printf("%s Switched deckhouse-cli to %s (installed locally).\n", okMark.Sprint("✓"), verNew.Sprint(stored))
				printSwitchNotes(res)

				return nil
			}

			// The requested version may be the running binary itself (a plain-file
			// install not seeded into the store yet): migration alone satisfies it -
			// SwitchTo archives the running binary under its version, no download.
			if cur, err := semver.NewVersion(version.Version); err == nil && cur.Equal(requested) {
				res, err := selfupdate.SwitchTo(cmd.Context(), exePath, version.Version, store, logger, nil)
				if err != nil {
					return err
				}

				fmt.Printf("%s Switched deckhouse-cli to %s (taken from the running binary).\n", okMark.Sprint("✓"), verNew.Sprint(version.Version))
				printSwitchNotes(res)

				return nil
			}

			updater, err := newUpdater(cmd.Context(), cmd, logger)
			if err != nil {
				return err
			}

			fmt.Printf("Version %s is not installed locally, downloading...\n", verNew.Sprint(requested.Original()))

			res, err := updater.Apply(cmd.Context(), requested.Original())
			if err != nil {
				return err
			}

			fmt.Printf("%s Switched deckhouse-cli to %s.\n", okMark.Sprint("✓"), verNew.Sprint(requested.Original()))
			printSwitchNotes(res)

			return nil
		},
	}
}

// printSwitchNotes tells the user what the switch left behind and how to undo it.
func printSwitchNotes(res selfupdate.SwitchResult) {
	if res.Migrated {
		fmt.Printf("The d8 binary in PATH is now a symlink into the version store; the previous binary is kept with a %q suffix.\n", selfupdate.OldSuffix)
	}

	if res.PrevTag != "" {
		fmt.Printf("Previous version %s remains installed - switch back with 'd8 cli use %s'.\n", verOld.Sprint(res.PrevTag), res.PrevTag)
	}
}

// completeStoredVersions offers the locally installed versions for `d8 cli use
// <TAB>`. Completion must stay instant and side-effect-free (the same contract
// root.go enforces for __complete), so it reads only the store - the versions
// that switch offline are exactly the ones worth suggesting.
func completeStoredVersions(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) != 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	store, err := selfupdate.NewStore()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	return storedVersionCompletions(store, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// storedVersionCompletions renders the store content as shell completions,
// newest-first, filtered by the typed prefix.
func storedVersionCompletions(store *selfupdate.Store, toComplete string) []string {
	versions := store.List()
	completions := make([]string, 0, len(versions))

	for _, v := range versions {
		if !strings.HasPrefix(v.Original(), toComplete) {
			continue
		}

		completions = append(completions, v.Original()+"\tinstalled locally, switches offline")
	}

	return completions
}
