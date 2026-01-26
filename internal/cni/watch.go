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
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/deckhouse-cli/internal/cni/api/v1alpha1"
	saferequest "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// RunWatch executes the logic for the 'cni-switch watch' command.
func RunWatch() error {
	ctx := context.Background()

	fmt.Println("🚀 Monitoring CNI switch progress")

	// Create a Kubernetes client
	safeClient, err := saferequest.NewSafeClient()
	if err != nil {
		return fmt.Errorf("creating safe client: %w", err)
	}

	rtClient, err := safeClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return fmt.Errorf("creating runtime client: %w", err)
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var (
		migrationName   string
		lastPrintedTime time.Time
		footerLines     int // Number of lines in the dynamic footer (progress + errors)
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			activeMigration, err := FindActiveMigration(ctx, rtClient)
			if err != nil {
				// Clean previous footer before printing warning
				clearFooter(footerLines)
				fmt.Printf("⚠️  Error finding active migration: %v\n", err)
				footerLines = 1 // We printed one line
				continue
			}

			if activeMigration == nil {
				clearFooter(footerLines)
				// If we were watching a migration and it disappeared, it's done or deleted.
				if migrationName != "" {
					fmt.Println("ℹ️ Migration resource is gone.")
				} else {
					fmt.Println("ℹ️ No active migration found.")
				}
				return nil
			}

			// Clear previous footer to verify if we need to print new logs
			clearFooter(footerLines)
			footerLines = 0

			// Print migration name once
			if migrationName == "" {
				migrationName = activeMigration.Name
				fmt.Printf("ℹ️ Monitoring migration resource: %s\n", migrationName)
			}

			// Collect and sort valid conditions
			var validConditions []metav1.Condition
			for _, c := range activeMigration.Status.Conditions {
				if c.Status == metav1.ConditionTrue {
					validConditions = append(validConditions, c)
				}
			}

			sort.Slice(validConditions, func(i, j int) bool {
				return validConditions[i].LastTransitionTime.Before(&validConditions[j].LastTransitionTime)
			})

			// Print new conditions
			for _, c := range validConditions {
				if c.LastTransitionTime.Time.After(lastPrintedTime) {
					fmt.Printf("[%s] %s: %s\n",
						c.LastTransitionTime.Format("15:04:05"),
						c.Type,
						c.Message)
					lastPrintedTime = c.LastTransitionTime.Time
				}
			}

			// Draw Footer
			// 1. Node Progress
			if activeMigration.Status.NodesTotal > 0 {
				fmt.Printf("  Nodes: %d/%d succeeded",
					activeMigration.Status.NodesSucceeded,
					activeMigration.Status.NodesTotal)
				if activeMigration.Status.NodesFailed > 0 {
					fmt.Printf(", %d failed", activeMigration.Status.NodesFailed)
				}
				fmt.Println() // End of progress line
				footerLines++

				// 2. Failed Nodes Details
				if len(activeMigration.Status.FailedSummary) > 0 {
					fmt.Println("  ⚠️ Failed Nodes:")
					footerLines++
					for _, f := range activeMigration.Status.FailedSummary {
						fmt.Printf("    - %s: %s\n", f.Node, f.Reason)
						footerLines++
					}
				}
			} else {
				// Placeholder if no nodes stats yet
				fmt.Println("  Waiting for node statistics...")
				footerLines++
			}

			// Check for completion
			for _, cond := range activeMigration.Status.Conditions {
				if cond.Type == v1alpha1.ConditionSucceeded && cond.Status == metav1.ConditionTrue {
					fmt.Printf("\n🎉 CNI switch to '%s' completed successfully!\n",
						activeMigration.Spec.TargetCNI)
					return nil
				}
			}
		}
	}
}

func clearFooter(lines int) {
	for range lines {
		fmt.Print("\033[1A\033[K") // Move up and clear line
	}
}
