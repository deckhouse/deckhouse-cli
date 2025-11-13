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
	"context"
	"fmt"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/cni/api/v1alpha1"
	saferequest "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const switchTimeout = 60 * time.Minute

// RunSwitch executes the logic for the 'cni-switch switch' command.
func RunSwitch() error {
	ctx, cancel := context.WithTimeout(context.Background(), switchTimeout)
	defer cancel()

	// 1. Create a Kubernetes client
	safeClient, err := saferequest.NewSafeClient()
	if err != nil {
		return fmt.Errorf("creating safe client: %w", err)
	}

	rtClient, err := safeClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return fmt.Errorf("creating runtime client: %w", err)
	}

	// 2. Find the active migration
	activeMigration, err := FindActiveMigration(ctx, rtClient)
	if err != nil {
		return fmt.Errorf("failed to find active migration: %w", err)
	}

	if activeMigration == nil {
		return fmt.Errorf("no active CNI migration found. Please run 'd8 cni-switch prepare' first")
	}

	// 3. Check if the preparation step was completed successfully
	isPrepared := false
	for _, cond := range activeMigration.Status.Conditions {
		if cond.Type == "PreparationSucceeded" && cond.Status == metav1.ConditionTrue {
			isPrepared = true
			break
		}
	}

	if !isPrepared {
		return fmt.Errorf("cluster is not ready for switching. Please ensure the 'prepare' command completed successfully")
	}

	fmt.Printf("Found prepared migration '%s'. Starting the switch process...\n", activeMigration.Name)

	// TODO: Implement the switch logic as per ADR
	// - Disable current CNI via ModuleConfig
	// - Wait for cni-switch-helper to clean up nodes
	// - Enable target CNI via ModuleConfig
	// - Wait for new CNI to be ready
	// - Restart pods
	// - Update CNIMigration status

	fmt.Println("Logic for switch is not implemented yet.")
	return nil
}
