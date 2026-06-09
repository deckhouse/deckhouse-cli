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

package list

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/inventory"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdLong = `List Snapshot CRs in the cluster.

By default, snapshots from the current kubeconfig namespace are shown.
Use -A/--all-namespaces to list across all namespaces, or -n/--namespace
to target a specific namespace.

Output format defaults to human-readable table; use -o json or -o yaml for structured output.`

	cmdExample = `  # List snapshots in the current kubeconfig namespace
  d8 snapshot list

  # List snapshots across all namespaces
  d8 snapshot list -A

  # List snapshots in a specific namespace
  d8 snapshot list -n my-ns

  # Output as JSON
  d8 snapshot list -n my-ns -o json`
)

// NewCommand returns the cobra command for `d8 snapshot list`.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List Snapshot CRs in the cluster",
		Long:    cmdLong,
		Example: cmdExample,
		Args:    cobra.NoArgs,
		RunE:    run,
	}

	cmd.Flags().StringP("namespace", "n", "", "namespace to list snapshots in (default: current kubeconfig namespace)")
	cmd.Flags().BoolP("all-namespaces", "A", false, "list snapshots across all namespaces")
	cmd.Flags().StringP("output", "o", inventory.FormatHuman, "output format: human, json, yaml")

	return cmd
}

func run(cmd *cobra.Command, _ []string) error {
	namespace, _ := cmd.Flags().GetString("namespace")
	allNamespaces, _ := cmd.Flags().GetBool("all-namespaces")
	format, _ := cmd.Flags().GetString("output")

	if allNamespaces && namespace != "" {
		return fmt.Errorf("--all-namespaces and --namespace are mutually exclusive")
	}

	var showNamespace bool

	switch {
	case allNamespaces:
		namespace = ""
		showNamespace = true
	case namespace != "":
		showNamespace = false
	default:
		namespace = safeClient.DefaultNamespace()
		showNamespace = false
	}

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	safeClient.SupportNoAuth = false

	sc, err := safeClient.NewSafeClient(cmd.PersistentFlags())
	if err != nil {
		return fmt.Errorf("build kube client: %w", err)
	}

	rtClient, err := sc.NewRTClient()
	if err != nil {
		return fmt.Errorf("build runtime client: %w", err)
	}

	infos, err := inventory.List(ctx, rtClient, namespace)
	if err != nil {
		return err
	}

	return inventory.Render(cmd.OutOrStdout(), infos, format, showNamespace)
}
