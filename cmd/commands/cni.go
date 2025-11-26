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
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/cni"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var ( // TODO: STEP 3 ???
	cniSwitchLong = templates.LongDesc(`
A group of commands to switch the CNI (Container Network Interface) provider in the Deckhouse cluster.

This process is divided into several steps:

  - 'd8 cni-switch prepare'  - STEP 1. Prepares the cluster for CNI migration.
  - 'd8 cni-switch switch'   - STEP 2. Performs the actual CNI switch.
  - 'd8 cni-switch cleanup'  - (Optional) Cleans up resources if the switch is aborted.
  - 'd8 cni-switch rollback' - (Optional) Rollback CNI if the switch is aborted.`)

	cniPrepareExample = templates.Examples(`
		# Prepare to switch to Cilium CNI
		d8 cni-switch prepare --to-cni cilium`)

	cniSwitchExample = templates.Examples(`
		# Perform the CNI switch after the prepare step is complete
		d8 cni-switch switch`)

	cniCleanupExample = templates.Examples(`
		# Cleanup resources created by the 'prepare' command
		d8 cni-switch cleanup`)

	cniRollbackExample = templates.Examples(`
		# Rollback changes, restore previous CNI, cleanup resources created by the 'prepare' command
		d8 cni-switch rollback`)

	supportedCNIs = []string{"cilium", "flannel", "simple-bridge"}
)

func NewCniSwitchCommand() *cobra.Command {
	log.SetFlags(0)
	cmd := &cobra.Command{
		Use:   "cni-switch",
		Short: "A group of commands to switch CNI in the cluster",
		Long:  cniSwitchLong,
	}
	cmd.PersistentFlags().Duration("timeout", 30*time.Minute, "The timeout for the entire operation (e.g., 30m, 1h)")
	cmd.AddCommand(NewCmdCniPrepare())
	cmd.AddCommand(NewCmdCniSwitch())
	cmd.AddCommand(NewCmdCniCleanup())
	cmd.AddCommand(NewCmdCniRollback())
	return cmd
}

func NewCmdCniPrepare() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "prepare",
		Short:   "Prepares the cluster for CNI switching",
		Example: cniPrepareExample,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			targetCNI, _ := cmd.Flags().GetString("to-cni")
			for _, supported := range supportedCNIs {
				if strings.ToLower(targetCNI) == supported {
					return nil
				}
			}
			return fmt.Errorf(
				"invalid --to-cni value %q. supported values are: %s",
				targetCNI,
				strings.Join(supportedCNIs, ", "),
			)
		},

		Run: func(cmd *cobra.Command, args []string) {
			targetCNI, _ := cmd.Flags().GetString("to-cni")
			timeout, _ := cmd.Flags().GetDuration("timeout")
			if err := cni.RunPrepare(targetCNI, timeout); err != nil {
				log.Fatalf("❌ Error running prepare command: %v", err)
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

func NewCmdCniSwitch() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "switch",
		Short:   "Performs the CNI switching",
		Example: cniSwitchExample,
		Run: func(cmd *cobra.Command, args []string) {
			timeout, _ := cmd.Flags().GetDuration("timeout")
			if err := cni.RunSwitch(timeout); err != nil {
				log.Fatalf("❌ Error running switch command: %v", err)
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
		Run: func(cmd *cobra.Command, args []string) {
			timeout, _ := cmd.Flags().GetDuration("timeout")
			if err := cni.RunCleanup(timeout); err != nil {
				log.Fatalf("❌ Error running cleanup command: %v", err)
			}
		},
	}
	return cmd
}

func NewCmdCniRollback() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "rollback",
		Short:   "Rollback all changes and restore previous CNI",
		Example: cniCleanupExample,
		Run: func(cmd *cobra.Command, args []string) {
			timeout, _ := cmd.Flags().GetDuration("timeout")
			if err := cni.RunRollback(timeout); err != nil {
				log.Fatalf("❌ Error running rollback command: %v", err)
			}
		},
	}
	return cmd
}
