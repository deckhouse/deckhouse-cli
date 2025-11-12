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
	"fmt"
	"os"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/cni/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Constants used across the CNI switch process
const (
	// Namespace where the helper and webhook are deployed
	cniSwitchNamespace = "cni-switch"

	// Service Account Names
	switchHelperServiceAccountName = "cni-switch-helper-sa"
	webhookServiceAccountName      = "cni-switch-webhook-sa"

	// Webhook Resources
	webhookDeploymentName = "cni-switch-webhook"
	webhookServiceName    = "cni-switch-webhook-service"
	webhookSecretName     = "cni-switch-webhook-tls"
	// IMPORTANT: This name is used for both creating the configuration in prepare.go
	// and deleting it in switch.go. It must be consistent.
	webhookConfigName = "cni-switch-pod-annotator"
	webhookPort       = 9443

	// Annotations
	EffectiveCNIAnnotation = "effective-cni.network.deckhouse.io"
)

var (
	CNIModuleConfigs = []string{"cni-cilium", "cni-flannel", "cni-simple-bridge"}

	moduleConfigGVK = schema.GroupVersionKind{
		Group:   "deckhouse.io",
		Version: "v1alpha1",
		Kind:    "ModuleConfig",
	}
)

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

// FindActiveMigration searches for a CNIMigration resource that is not in a terminal state.
// It returns an error if more than one active migration is found.
func FindActiveMigration(ctx context.Context, rtClient client.Client) (*v1alpha1.CNIMigration, error) {
	migrationList := &v1alpha1.CNIMigrationList{}
	if err := rtClient.List(ctx, migrationList); err != nil {
		return nil, fmt.Errorf("listing CNIMigration objects: %w", err)
	}

	activeMigrations := make([]v1alpha1.CNIMigration, 0)

	for _, migration := range migrationList.Items {
		isTerminal := false
		for _, cond := range migration.Status.Conditions {
			if cond.Type == "Succeeded" && cond.Status == metav1.ConditionTrue {
				isTerminal = true
				break
			}
		}
		if migration.Status.ObservedPhase == "Failed" { // TODO: will it be like that?
			isTerminal = true
		}

		if !isTerminal {
			activeMigrations = append(activeMigrations, migration)
		}
	}

	if len(activeMigrations) == 0 {
		return nil, nil // No active migration found
	}

	if len(activeMigrations) > 1 {
		return nil, fmt.Errorf(
			"found %d active CNI migrations, which is an inconsistent state. "+
				"Please run 'd8 cni-switch cleanup' to resolve this",
			len(activeMigrations),
		)
	}

	return &activeMigrations[0], nil
}
