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
	"os"
	"sort"
	"strings"
	"time"

	"github.com/mitchellh/go-wordwrap"
	"golang.org/x/term"
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

	// Switching process
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	var (
		migrationName string
		printedEvents = make(map[string]bool)
		footerLines   int // Number of visual lines in the dynamic footer
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			activeMigration, err := FindActiveMigration(ctx, rtClient)
			if err != nil {
				// Clear footer before warning
				clearFooter(footerLines)
				msg := fmt.Sprintf("⚠️ Error finding active migration: %v", err)

				termWidth := getTermWidth()
				if termWidth <= 0 {
					termWidth = 80
				}
				footerLines = printFooter(msg, termWidth)

				continue
			}

			if activeMigration == nil {
				clearFooter(footerLines)
				// Migration resource disappeared
				if migrationName != "" {
					fmt.Println("🔎 Migration resource is gone")
				} else {
					fmt.Println("🔎 No active migration found")
				}
				return nil
			}

			// Clear footer to print new logs
			clearFooter(footerLines)
			footerLines = 0

			// Print migration info once
			if migrationName == "" {
				migrationName = activeMigration.Name
				fmt.Printf("[%s] 🔎 Monitoring migration resource: %s\n",
					activeMigration.CreationTimestamp.Format("15:04:05"),
					migrationName)
			}

			// Sort conditions by time
			conditions := activeMigration.Status.Conditions
			sort.Slice(conditions, func(i, j int) bool {
				return conditions[i].LastTransitionTime.Before(&conditions[j].LastTransitionTime)
			})

			// Track last step completion time
			lastStepTime := activeMigration.CreationTimestamp.Time

			// Process conditions
			for _, c := range conditions {
				// Deduplicate events
				eventKey := fmt.Sprintf("%s|%s|%s|%s|%s",
					c.Type, c.Status, c.Reason, c.Message, c.LastTransitionTime.Time.String())

				var icon string
				shouldPrint := false
				isProgress := false

				switch {
				case c.Status == metav1.ConditionTrue:
					icon = "✅"
					shouldPrint = true
				case c.Status == metav1.ConditionFalse && c.Reason == "Error":
					icon = "❌"
					shouldPrint = true
				case c.Reason == "InProgress":
					icon = "  "
					shouldPrint = true
					isProgress = true
				}

				if shouldPrint {
					if !printedEvents[eventKey] {
						if isProgress {
							fmt.Printf("[%s] %s %s: %s\n",
								time.Now().Format("15:04:05"),
								icon,
								c.Type,
								c.Message)
						} else {
							stepDuration := c.LastTransitionTime.Time.Sub(lastStepTime)

							fmt.Printf("[%s] %s %s: %s (+%s)\n",
								time.Now().Format("15:04:05"),
								icon,
								c.Type,
								c.Message,
								stepDuration.Round(time.Second))
						}
						printedEvents[eventKey] = true
					}

					// Update reference time for completed steps
					if !isProgress {
						lastStepTime = c.LastTransitionTime.Time
					}
				}
			}

			// Print Failed Nodes
			for _, f := range activeMigration.Status.FailedSummary {
				failKey := fmt.Sprintf("fail|%s|%s", f.Node, f.Reason)
				if !printedEvents[failKey] {
					fmt.Printf("⚠️ Node %s failed: %s\n", f.Node, f.Reason)
					printedEvents[failKey] = true
				}
			}

			// Update status footer
			phaseMsg := ""
			if activeMigration.Status.Phase != "" {
				phaseMsg = fmt.Sprintf("  Phase: %s", activeMigration.Status.Phase)

				if count := len(activeMigration.Status.FailedSummary); count > 0 {
					phaseMsg += fmt.Sprintf(" (Failed Nodes: %d)", count)
				}
			}

			if phaseMsg != "" {
				termWidth := getTermWidth()
				if termWidth <= 0 {
					termWidth = 80
				}
				lines := printFooter(phaseMsg, termWidth)
				footerLines += lines
			}

			// Check completion
			for _, cond := range activeMigration.Status.Conditions {
				if cond.Type == v1alpha1.ConditionSucceeded && cond.Status == metav1.ConditionTrue {
					clearFooter(footerLines)
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

func getTermWidth() int {
	width, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 80
	}
	return width
}

// printFooter prints text wrapped to fit within width and returns the number of lines printed.
func printFooter(text string, width int) int {
	// If the text is short enough, just print it
	if len(text) <= width {
		fmt.Println(text)
		return 1
	}

	// Use wordwrap to split into lines
	wrapped := wordwrap.WrapString(text, uint(width))
	fmt.Println(wrapped)

	return strings.Count(wrapped, "\n") + 1
}
