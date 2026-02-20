/*
Copyright 2025 Flant JSC

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

package commands

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/cni"
)

var (
	cniSwitchLong = `A group of commands to switch the CNI (Container Network Interface) provider in the DKP.

The migration process is handled automatically by an in-cluster controller.
This CLI tool is used to trigger the migration and monitor its status.

Workflow:

  1. 'd8 cni-migration switch --to-cni <CNI>' - Initiates the migration.
     This creates a CNIMigration resource, which triggers the deployment of the migration agent.
     The agent then performs all necessary steps (validation, node checks, CNI switching).

  2. 'd8 cni-migration watch' - (Optional) Monitors the progress of the migration.
     Since the process is automated, this command simply watches the status.

  3. 'd8 cni-migration cleanup' - Cleans up the migration resources after completion.`

	cniSwitchExample = templates.Examples(`
		# Start the migration to Cilium CNI
		d8 cni-migration switch --to-cni cilium`)

	cniWatchExample = templates.Examples(`
		# Monitor the ongoing migration
		d8 cni-migration watch`)

	cniCleanupExample = templates.Examples(`
		# Cleanup resources created by the 'switch' command
		d8 cni-migration cleanup`)

	supportedCNIs = []string{"cilium", "flannel", "simple-bridge"}
)

func NewCniSwitchCommand() *cobra.Command {
	log.SetFlags(0)
	ctrllog.SetLogger(logr.Discard())

	cmd := &cobra.Command{
		Use:   "cni-migration",
		Short: "A group of commands to switch CNI in the cluster",
		Long:  cniSwitchLong,
	}
	cmd.AddCommand(NewCmdCniSwitch())
	cmd.AddCommand(NewCmdCniWatch())
	cmd.AddCommand(NewCmdCniCleanup())
	return cmd
}

func NewCmdCniSwitch() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "switch",
		Short:   "Initiates the CNI switching",
		Example: cniSwitchExample,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			targetCNI, _ := cmd.Flags().GetString("to-cni")
			for _, supported := range supportedCNIs {
				if strings.ToLower(targetCNI) == supported {
					return nil
				}
			}
			return fmt.Errorf(
				"invalid --to-cni value %q. Supported values are: %s",
				targetCNI,
				strings.Join(supportedCNIs, ", "),
			)
		},

		Run: func(cmd *cobra.Command, _ []string) {
			targetCNI, _ := cmd.Flags().GetString("to-cni")

			if err := cni.RunSwitch(targetCNI); err != nil {
				if errors.Is(err, cni.ErrCancelled) {
					return
				}
				log.Fatalf("❌ Error running switch command: %v", err)
			}

			fmt.Println()
			if err := cni.RunWatch(); err != nil {
				if errors.Is(err, cni.ErrMigrationFailed) {
					os.Exit(1)
				}
				log.Fatalf("❌ Error monitoring switch progress: %v", err)
			}
		},
	}
	cmd.Flags().String("to-cni", "", fmt.Sprintf(
		"Target CNI provider to switch to. Supported values: %s",
		strings.Join(supportedCNIs, ", "),
	))
	_ = cmd.MarkFlagRequired("to-cni")

	return cmd
}

func NewCmdCniWatch() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "watch",
		Short:   "Monitors the CNI switching progress",
		Example: cniWatchExample,
		Run: func(_ *cobra.Command, _ []string) {
			if err := cni.RunWatch(); err != nil {
				if errors.Is(err, cni.ErrMigrationFailed) {
					os.Exit(1)
				}
				log.Fatalf("❌ Error running watch command: %v", err)
			}
		},
	}
	return cmd
}

func NewCmdCniCleanup() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "cleanup",
		Short:   "Cleans up resources created during CNI switching",
		Example: cniCleanupExample,
		Run: func(_ *cobra.Command, _ []string) {
			if err := cni.RunCleanup(); err != nil {
				log.Fatalf("❌ Error running cleanup command: %v", err)
			}
		},
	}
	return cmd
}
