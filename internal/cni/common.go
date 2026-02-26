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

package cni

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/cni/api/v1alpha1"
)

var ErrCancelled = errors.New("cancelled")

// AskForConfirmation displays a warning and prompts the user for confirmation.
func AskForConfirmation(commandName string) (bool, error) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("⚠️  IMPORTANT: PLEASE READ CAREFULLY")
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println()
	fmt.Printf("You are about to run the '%s' step of the CNI switch process. Please ensure that:\n\n", commandName)
	fmt.Println("1. External cluster management systems (CI/CD, GitOps like ArgoCD, Flux)")
	fmt.Println("   are temporarily disabled. They might interfere with the CNI switch process")
	fmt.Println("   by reverting changes made by this tool.")
	fmt.Println("2. This tool is NOT intended for switching to any (third-party) CNI.")
	fmt.Println("3. The utility does not configure CNI modules in the cluster; it only enables/disables")
	fmt.Println("   them via ModuleConfig during operation. The user must independently prepare the")
	fmt.Println("   ModuleConfig configuration for the target CNI.")
	fmt.Println("4. During the migration, all pods in the cluster using the network (PodNetwork)")
	fmt.Println("   created by the current CNI will be restarted. This will cause service interruption.")
	fmt.Println("   To minimize the risk of critical data loss, it is highly recommended to manually")
	fmt.Println("   stop the most critical services before performing the work.")
	fmt.Println("5. It is recommended to perform the work during an agreed maintenance window.")
	fmt.Println()
	fmt.Println("Once the process starts, no active intervention is required from you.")
	fmt.Println()
	fmt.Print("Do you want to continue? (y/n): ")

	for {
		response, err := reader.ReadString('\n')
		if err != nil {
			return false, err
		}

		response = strings.ToLower(strings.TrimSpace(response))

		switch response {
		case "y", "yes":
			fmt.Println()
			return true, nil
		case "n", "no":
			fmt.Println()
			return false, nil
		default:
			fmt.Print("Invalid input. Please enter 'y/yes' or 'n/no'): ")
		}
	}
}

// FindActiveMigration searches for an existing CNIMigration resource.
func FindActiveMigration(ctx context.Context, rtClient client.Client) (*v1alpha1.CNIMigration, error) {
	migrationList := &v1alpha1.CNIMigrationList{}
	if err := rtClient.List(ctx, migrationList); err != nil {
		return nil, fmt.Errorf("listing CNIMigration objects: %w", err)
	}

	if len(migrationList.Items) == 0 {
		return nil, nil // No migration found
	}

	if len(migrationList.Items) > 1 {
		return nil, fmt.Errorf(
			"found %d CNI migration objects, which is an inconsistent state. "+
				"Please run 'd8 cni-migration cleanup' to resolve this",
			len(migrationList.Items),
		)
	}

	return &migrationList.Items[0], nil
}
