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
	"fmt"
	"os"
	"strings"
)

var CNIModuleConfigs = []string{"cni-cilium", "cni-flannel", "cni-simple-bridge"}

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
	fmt.Println()
	fmt.Println("2. You have sufficient administrative privileges for this cluster to perform")
	fmt.Println("   the required actions (modifying ModuleConfigs, deleting pods, etc.).")
	fmt.Println()
	fmt.Println("3. The utility does not configure CNI modules in the cluster; it only enables/disables")
	fmt.Println("   them via ModuleConfig during operation. The user must independently prepare the")
	fmt.Println("   ModuleConfig configuration for the target CNI.")
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

		if response == "y" || response == "yes" {
			fmt.Println()
			return true, nil
		} else if response == "n" || response == "no" {
			fmt.Println()
			return false, nil
		} else {
			fmt.Print("Invalid input. Please enter 'y/yes' or 'n/no'): ")
		}
	}
}
