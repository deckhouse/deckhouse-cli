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

func newCheckCommand(logger *dkplog.Logger) *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "Report whether a newer deckhouse-cli version is available",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			updater, err := newUpdater(cmd.Context(), cmd, logger)
			if err != nil {
				return err
			}

			latest, newer, err := updater.LatestVersion(cmd.Context(), version.Version)
			if err != nil {
				return err
			}

			if newer {
				fmt.Printf("A newer deckhouse-cli is available: %s (current: %s). Run 'd8 dist update' to upgrade.\n",
					verNew.Sprint(latest), verOld.Sprint(version.Version))
			} else {
				fmt.Printf("deckhouse-cli is up to date (%s).\n", verCur.Sprint(version.Version))
			}

			return nil
		},
	}
}
