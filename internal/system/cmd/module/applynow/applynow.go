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

package applynow

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/operatemodule"
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/v1alpha1"
)

var applyNowLong = templates.LongDesc(`
Apply a module release immediately, bypassing update windows.

This command adds the annotation 'modules.deckhouse.io/apply-now="true"'
to the specified ModuleRelease resource, forcing immediate deployment
regardless of configured update windows or applyAfter schedules.

This command is used for modules with Auto update policy that have
update windows configured or releases scheduled for future deployment.

© Flant JSC 2025`)

var applyNowExample = templates.Examples(`
  # Apply a module release immediately
  d8 system module apply-now csi-hpe v0.3.10

  # Apply without 'v' prefix (will be added automatically)
  d8 system module apply-now csi-hpe 0.3.10
`)

func NewCommand() *cobra.Command {
	applyNowCmd := &cobra.Command{
		Use:               "apply-now <module-name> <version>",
		Short:             "Apply a module release immediately, bypassing update windows.",
		Long:              applyNowLong,
		Example:           applyNowExample,
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: operatemodule.CompleteForApplyNow,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE:              applyNowRelease,
	}

	return applyNowCmd
}

func applyNowRelease(cmd *cobra.Command, args []string) error {
	moduleName := args[0]
	version := operatemodule.NormalizeVersion(args[1])

	dynamicClient, err := operatemodule.GetDynamicClient(cmd)
	if err != nil {
		return err
	}

	// Try to get the release
	release, err := operatemodule.GetModuleRelease(dynamicClient, moduleName, version)
	if err != nil {
		if errors.IsNotFound(err) {
			return operatemodule.SuggestSuitableReleasesOnNotFound(dynamicClient, moduleName, version, operatemodule.CanBeAppliedNow)
		}
		return fmt.Errorf("failed to get module release: %w", err)
	}

	// Check if already has apply-now annotation
	if release.IsApplyNow {
		fmt.Fprintf(os.Stderr, "%s Module release '%s' already has apply-now annotation.\n", operatemodule.MsgWarn, release.Name)
		fmt.Fprintf(os.Stderr, "   Phase: %s\n", release.Phase)
		if release.Message != "" {
			fmt.Fprintf(os.Stderr, "   Message: %s\n", release.Message)
		}
		return nil
	}

	// Check if the release is in Pending phase
	if release.Phase != v1alpha1.ModuleReleasePhasePending {
		fmt.Fprintf(os.Stderr, "%s Module release '%s' is not in Pending phase.\n", operatemodule.MsgWarn, release.Name)
		fmt.Fprintf(os.Stderr, "   Current phase: %s\n", release.Phase)
		if release.Message != "" {
			fmt.Fprintf(os.Stderr, "   Message: %s\n", release.Message)
		}
		fmt.Fprintln(os.Stderr, "\nOnly releases in 'Pending' phase can be applied.")
		return nil
	}

	// Apply the apply-now annotation
	err = operatemodule.ApplyNowModuleRelease(dynamicClient, release.Name)
	if err != nil {
		return fmt.Errorf("failed to set apply-now annotation: %w", err)
	}
	fmt.Printf("%s Module release '%s' marked for immediate deployment.\n", operatemodule.MsgOK, release.Name)
	fmt.Println("   The release will be applied immediately, bypassing update windows.")

	return nil
}
