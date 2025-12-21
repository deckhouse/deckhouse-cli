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

package approve

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/cli"
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/modulereleases"
)

var approveLong = templates.LongDesc(`
Approve a pending module release for installation.

This command adds the annotation 'modules.deckhouse.io/approved="true"'
to the specified ModuleRelease resource, allowing it to be deployed
according to the update policy.

This command is used for modules with Manual update policy that require
explicit approval before deployment.

© Flant JSC 2025`)

var approveExample = templates.Examples(`
  # Approve a specific module release
  d8 system module approve csi-hpe v0.3.10

  # Approve without 'v' prefix (will be added automatically)
  d8 system module approve csi-hpe 0.3.10
`)

func NewCommand() *cobra.Command {
	approveCmd := &cobra.Command{
		Use:               "approve <module-name> <version>",
		Short:             "Approve a pending module release for Manual update policy.",
		Long:              approveLong,
		Example:           approveExample,
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: cli.CompleteForApprove,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE:              approveRelease,
	}

	return approveCmd
}

func approveRelease(cmd *cobra.Command, args []string) error {
	moduleName := args[0]
	version := modulereleases.NormalizeVersion(args[1])

	dynamicClient, err := cli.GetDynamicClient(cmd)
	if err != nil {
		return err
	}

	// Try to get the release
	release, err := modulereleases.GetModuleRelease(dynamicClient, moduleName, version)
	if err != nil {
		if errors.IsNotFound(err) {
			return cli.SuggestSuitableReleasesOnNotFound(dynamicClient, moduleName, version, modulereleases.CanBeApproved)
		}
		return fmt.Errorf("failed to get module release: %w", err)
	}

	// Check if already approved
	if release.IsApproved {
		fmt.Fprintf(os.Stderr, "%s Module release '%s' is already approved.\n", cli.MsgWarn, release.Name)
		fmt.Fprintf(os.Stderr, "   Phase: %s\n", release.Phase)
		if release.Message != "" {
			fmt.Fprintf(os.Stderr, "   Message: %s\n", release.Message)
		}
		return nil
	}

	// Check if the release is in Pending phase
	if release.Phase != v1alpha1.ModuleReleasePhasePending {
		fmt.Fprintf(os.Stderr, "%s Module release '%s' is not in Pending phase.\n", cli.MsgWarn, release.Name)
		fmt.Fprintf(os.Stderr, "   Current phase: %s\n", release.Phase)
		if release.Message != "" {
			fmt.Fprintf(os.Stderr, "   Message: %s\n", release.Message)
		}
		fmt.Fprintln(os.Stderr, "\nOnly releases in 'Pending' phase can be approved.")
		return nil
	}

	// Apply the approved annotation
	err = modulereleases.ApproveModuleRelease(dynamicClient, release.Name)
	if err != nil {
		return fmt.Errorf("failed to approve module release: %w", err)
	}
	fmt.Printf("%s Module release '%s' approved.\n", cli.MsgOK, release.Name)
	fmt.Println("   The release will be deployed according to the update policy.")

	return nil
}
