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
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mitchellh/go-wordwrap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/deckhouse-cli/internal/cni/api/v1alpha1"
	saferequest "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

var ErrMigrationFailed = errors.New("migration failed")

// RunWatch executes the logic for the 'cni-migration watch' command.
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

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var (
		migrationName string
		printedEvents = make(map[string]bool)
		footerLines   int // Number of lines in the dynamic footer (progress + errors)
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
					fmt.Println("ℹ️  Migration resource is gone")
				} else {
					fmt.Println("ℹ️  No active migration found")
				}
				return nil
			}

			// Clear previous footer to verify if we need to print new logs
			clearFooter(footerLines)
			footerLines = 0

			// Print migration name once
			if migrationName == "" {
				migrationName = activeMigration.Name
				fmt.Printf("[%s] ℹ️  Monitoring migration resource: %s\n",
					activeMigration.CreationTimestamp.Format("15:04:05"),
					migrationName)
			}

			// Sort conditions by transition time
			conditions := activeMigration.Status.Conditions
			sort.Slice(conditions, func(i, j int) bool {
				return conditions[i].LastTransitionTime.Before(&conditions[j].LastTransitionTime)
			})

			// Track time of the last completed step to calculate deltas
			lastStepTime := activeMigration.CreationTimestamp.Time

			// Print new conditions
			var currentProgressMsg string
			for _, c := range conditions {
				// Track InProgress message (latest wins due to sort)
				if c.Reason == "InProgress" {
					currentProgressMsg = c.Message
					continue // Don't print InProgress to log
				}

				// Create unique key for the event
				eventKey := fmt.Sprintf("%s|%s|%s|%s|%s",
					c.Type, c.Status, c.Reason, c.Message, c.LastTransitionTime.Time.String())

				var icon string
				shouldPrint := false

				switch {
				case c.Status == metav1.ConditionTrue:
					icon = "✅"
					shouldPrint = true
				case c.Status == metav1.ConditionFalse && c.Reason == "Error":
					icon = "❌"
					shouldPrint = true
				}

				if shouldPrint {
					// Calculate duration since the last completed step
					stepDuration := c.LastTransitionTime.Time.Sub(lastStepTime)

					// Only print if not already printed
					if !printedEvents[eventKey] {
						fmt.Printf("[%s] %s %s: %s (+%s)\n",
							c.LastTransitionTime.Format("15:04:05"),
							icon,
							c.Type,
							c.Message,
							stepDuration.Round(time.Second))
						printedEvents[eventKey] = true
					}

					// Update the reference time for the next step
					lastStepTime = c.LastTransitionTime.Time
				}

				if c.Status == metav1.ConditionFalse && c.Reason == "Error" {
					return ErrMigrationFailed
				}
			}

			// Draw Footer
			// 0. Phase
			if activeMigration.Status.Phase != "" {
				fmt.Printf("  Phase: %s\n", activeMigration.Status.Phase)
				footerLines++
			}

			// 0.5 Current Progress (Dynamic)
			if currentProgressMsg != "" {
				// Use wordwrap for long progress messages too
				wrapped := wordwrap.WrapString(currentProgressMsg, 100)
				lines := strings.Split(wrapped, "\n")
				fmt.Printf("  ⏳ %s\n", lines[0])
				footerLines++
				for _, line := range lines[1:] {
					fmt.Printf("     %s\n", line)
					footerLines++
				}
			}

			// 1. Failed Nodes Details (Critical info only)
			if len(activeMigration.Status.FailedSummary) > 0 {
				fmt.Printf("  ⚠️  Failed Nodes (%d):\n", len(activeMigration.Status.FailedSummary))
				footerLines++
				for _, f := range activeMigration.Status.FailedSummary {
					// Wrap error message at 100 chars
					wrapped := wordwrap.WrapString(f.Reason, 100)
					lines := strings.Split(wrapped, "\n")

					// Print first line with node name
					fmt.Printf("    - %s: %s\n", f.Node, lines[0])
					footerLines++

					// Print subsequent lines with indentation
					indent := strings.Repeat(" ", 6+len(f.Node)+2) // "    - " + node + ": "
					for _, line := range lines[1:] {
						fmt.Printf("%s%s\n", indent, line)
						footerLines++
					}
				}
			}

			// Check for completion
			for _, cond := range activeMigration.Status.Conditions {
				if cond.Type == v1alpha1.ConditionSucceeded && cond.Status == metav1.ConditionTrue {
					totalDuration := cond.LastTransitionTime.Time.Sub(activeMigration.CreationTimestamp.Time)
					fmt.Printf("🎉 CNI switch to '%s' completed successfully! (Total time: %s)\n",
						activeMigration.Spec.TargetCNI,
						totalDuration.Round(time.Second))
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
