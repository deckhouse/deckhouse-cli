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
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/autoupdate"
)

// newCronCommand returns `d8 cli cron` - a no-op helper that only PRINTS the
// crontab instructions for enabling automatic self-updates. d8 deliberately
// never installs its own updates unattended (the background check only shows a
// notice), so the automation is an explicit, user-owned cron line; this command
// makes that line copy-pasteable.
func newCronCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "cron",
		Short: "Show how to enable automatic self-updates via cron (prints instructions, changes nothing)",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			printCronInstructions(cmd.OutOrStdout())
		},
	}
}

func printCronInstructions(w io.Writer) {
	// A random minute so a fleet of machines does not hit the proxy in the same
	// second; the hour is fixed at 06 local time.
	schedule := fmt.Sprintf("%d 6 * * *", rand.IntN(60))

	// $HOME is single-quoted through echo on purpose: it must reach the crontab
	// verbatim and expand when cron runs the line, not when the user pastes it.
	enable := fmt.Sprintf(`( crontab -l 2>/dev/null; echo '%s %s cli update >>"$HOME/.cache/deckhouse-cli/auto-update.log" 2>&1' ) | crontab -`,
		schedule, d8PathForCron())

	fmt.Fprintf(w, `deckhouse-cli never edits your crontab itself - run the command below
to enable automatic daily self-updates.

Enable:

  %s

What it does:
  - runs 'd8 cli update' daily (a random minute is suggested to spread load);
  - idempotent: does nothing when d8 is already up to date;
  - uses your kubeconfig identity via the in-cluster registry-packages-proxy;
  - log: ~/.cache/deckhouse-cli/auto-update.log

Verify:   crontab -l | grep 'd8 cli update'
Disable:  crontab -l | grep -v 'd8 cli update' | crontab -

Plugins need no cron: installed plugins already auto-update in the
background (opt out with %s=1).
`, enable, autoupdate.EnvDisableAutoUpdate)
}

// d8PathForCron returns the stable path to embed in the crontab line: the
// binary as it was invoked, or as found on PATH - WITHOUT resolving symlinks.
// os.Executable (and selfupdate.CurrentExecutable) must not be used here: on
// Linux they resolve /proc/self/exe to the symlink target inside the version
// store, and a cron line with that path would keep running the version that
// was current when the line was written, forever.
func d8PathForCron() string {
	arg0 := os.Args[0]

	if strings.ContainsRune(arg0, os.PathSeparator) {
		if abs, err := filepath.Abs(arg0); err == nil {
			return abs
		}

		return arg0
	}

	if found, err := exec.LookPath(arg0); err == nil {
		if abs, err := filepath.Abs(found); err == nil {
			return abs
		}

		return found
	}

	return "d8"
}
