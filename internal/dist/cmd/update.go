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
	"fmt"

	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/version"
)

func newUpdateCommand(logger *dkplog.Logger) *cobra.Command {
	var targetVersion string

	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update deckhouse-cli to the latest version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			updater, err := newUpdater(cmd.Context(), cmd, logger)
			if err != nil {
				return err
			}

			tag := targetVersion
			if tag == "" {
				latest, newer, err := updater.LatestVersion(cmd.Context(), version.Version)
				if err != nil {
					return err
				}

				if !newer {
					fmt.Printf("deckhouse-cli is already up to date (%s).\n", verCur.Sprint(version.Version))

					return nil
				}

				tag = latest
			}

			fmt.Printf("Updating deckhouse-cli to %s...\n", verNew.Sprint(tag))

			res, err := updater.Apply(cmd.Context(), tag)
			if err != nil {
				return err
			}

			fmt.Printf("%s deckhouse-cli updated to %s.\n", okMark.Sprint("✓"), verNew.Sprint(tag))
			printSwitchNotes(res)

			return nil
		},
	}

	cmd.Flags().StringVar(&targetVersion, "version", "", "Exact version to install; downgrades are allowed (default: the latest).")

	return cmd
}
