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
	"strings"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/cni/api/v1alpha1"
	saferequest "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RunSwitch executes the logic for the 'cni-switch switch' command.
func RunSwitch(timeout time.Duration) error {
	// 0. Ask for user confirmation
	confirmed, err := AskForConfirmation("switch")
	if err != nil {
		return fmt.Errorf("asking for confirmation: %w", err)
	}
	if !confirmed {
		fmt.Println("Operation cancelled by user.")
		return nil
	}

	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fmt.Printf("🚀 Starting CNI switch (global timeout: %s)\n", timeout)

	// 1. Create a Kubernetes client
	safeClient, err := saferequest.NewSafeClient()
	if err != nil {
		return fmt.Errorf("creating safe client: %w", err)
	}

	rtClient, err := safeClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return fmt.Errorf("creating runtime client: %w", err)
	}
	fmt.Printf("✅ Kubernetes client created (total elapsed: %s)\n\n",
		time.Since(startTime).Round(time.Millisecond))

	// 2. Find the active migration
	activeMigration, err := FindActiveMigration(ctx, rtClient)
	if err != nil {
		return fmt.Errorf("failed to find active migration: %w", err)
	}

	if activeMigration == nil {
		return fmt.Errorf("no active CNI migration found. Please run 'd8 cni-switch prepare' first")
	}

	// Check if the switch is already completed successfully.
	for _, cond := range activeMigration.Status.Conditions {
		if cond.Type == "Succeeded" && cond.Status == metav1.ConditionTrue {
			fmt.Printf("🎉 CNI switch to '%s' is already completed successfully.\n", activeMigration.Spec.TargetCNI)
			fmt.Println("\nYou can run 'd8 cni-switch cleanup' to remove auxiliary resources.")
			return nil
		}
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
		return fmt.Errorf("cluster is not ready for switching. " +
			"Please ensure the 'prepare' command completed successfully")
	}

	// Verify all resources exist
	fmt.Println("Verifying all created resources...")
	if err = verifyResourcesExist(ctx, rtClient); err != nil {
		return fmt.Errorf("verifying resources: %w", err)
	}
	fmt.Println("- Verified. All necessary resources are present in the cluster")

	fmt.Printf(
		"✅ Working with prepared migration '%s' (total elapsed: %s)\n\n",
		activeMigration.Name,
		time.Since(startTime).Round(time.Millisecond),
	)

	// 4. Enable target CNI
	currentCNI := activeMigration.Status.CurrentCNI
	targetCNI := activeMigration.Spec.TargetCNI

	// Check if we already allowed the target CNI to start
	targetCNIAllowedToStart := false
	for _, cond := range activeMigration.Status.Conditions {
		if cond.Type == "NodeCleanupSucceeded" && cond.Status == metav1.ConditionTrue {
			targetCNIAllowedToStart = true
			break
		}
	}

	fmt.Printf("Enabling target CNI module 'cni-%s'...\n", targetCNI)
	if err = toggleModule(ctx, rtClient, "cni-"+strings.ToLower(targetCNI), true); err != nil {
		return fmt.Errorf("enabling module '%s': %w", targetCNI, err)
	}

	var dsName string
	if !targetCNIAllowedToStart {
		// Wait for target CNI pods to start initializing
		targetModuleName := "cni-" + strings.ToLower(targetCNI)
		dsName, err = getDaemonSetNameForCNI(targetModuleName)
		if err != nil {
			return fmt.Errorf("getting daemonset name for target CNI: %w", err)
		}
		if err = waitForModulePodsInitializing(ctx, rtClient, targetModuleName, dsName); err != nil {
			return fmt.Errorf("waiting for target CNI pods to initialize: %w", err)
		}
		fmt.Printf("✅ CNI module 'cni-%s' enabled and pods initialized (total elapsed: %s)\n\n",
			targetCNI,
			time.Since(startTime).Round(time.Millisecond))

		// 5. Disable current CNI
		fmt.Printf("Disabling current CNI module 'cni-%s'...\n", currentCNI)
		if err = toggleModule(ctx, rtClient, "cni-"+strings.ToLower(currentCNI), false); err != nil {
			return fmt.Errorf("disabling module '%s': %w", currentCNI, err)
		}
		if err = waitForModule(ctx, rtClient, "cni-"+strings.ToLower(currentCNI), false); err != nil {
			return fmt.Errorf("waiting for module '%s' to be disabled: %w", currentCNI, err)
		}

		var dsNameCurrent string
		dsNameCurrent, err = getDaemonSetNameForCNI("cni-" + strings.ToLower(currentCNI))
		if err != nil {
			return fmt.Errorf("getting daemonset name for current CNI: %w", err)
		}
		if err = waitForModulePodsTermination(
			ctx, rtClient, "cni-"+strings.ToLower(currentCNI), dsNameCurrent,
		); err != nil {
			return fmt.Errorf("waiting for current CNI pods to terminate: %w", err)
		}

		if err = updateCNIMigrationStatus(ctx, rtClient, activeMigration.Name, metav1.Condition{
			Type:    "OldCNIDisabled",
			Status:  metav1.ConditionTrue,
			Reason:  "ModuleDisabled",
			Message: fmt.Sprintf("Module 'cni-%s' was successfully disabled.", currentCNI),
		}); err != nil {
			return fmt.Errorf("updating CNIMigration status: %w", err)
		}
		fmt.Printf("✅ CNI module 'cni-%s' disabled (total elapsed: %s)\n\n",
			currentCNI,
			time.Since(startTime).Round(time.Millisecond))

		// 6. Update phase to Migrate (Triggers cleanup on nodes)
		fmt.Println("Updating CNIMigration phase to 'Migrate' to trigger node cleanup...")
		if err = updateCNIMigrationPhase(ctx, rtClient, activeMigration.Name, "Migrate"); err != nil {
			return fmt.Errorf("updating CNIMigration phase: %w", err)
		}
		fmt.Printf("✅ CNIMigration phase updated (total elapsed: %s)\n\n",
			time.Since(startTime).Round(time.Millisecond))

		// 7. Wait for nodes to be cleaned up
		fmt.Println("Waiting for nodes to be cleaned up by cni-switch-helper...")
		if err = waitForNodeConditions(ctx, rtClient, activeMigration, "CleanupSucceeded"); err != nil {
			return fmt.Errorf("waiting for node cleanup: %w", err)
		}
		fmt.Printf("✅ All nodes cleaned up (total elapsed: %s)\n\n",
			time.Since(startTime).Round(time.Millisecond))

		// 8. This status update is CRITICAL. It unblocks the target CNI's init-container.
		fmt.Println("Signaling target CNI pods to proceed by updating CNIMigration status...")
		if err = updateCNIMigrationStatus(ctx, rtClient, activeMigration.Name, metav1.Condition{
			Type:    "NodeCleanupSucceeded",
			Status:  metav1.ConditionTrue,
			Reason:  "AllNodesCleanedUp",
			Message: "All nodes have been successfully cleaned up from old CNI artifacts.",
		}); err != nil {
			return fmt.Errorf("updating CNIMigration status: %w", err)
		}
		fmt.Printf("✅ CNIMigration status updated (total elapsed: %s)\n\n",
			time.Since(startTime).Round(time.Millisecond))
	} else {
		fmt.Printf("ℹ️ Skipping wait for init, disable old CNI, cleanup as NodeCleanupSucceeded is already True.\n\n")
	}

	// 9. Wait for target CNI to be Ready
	// Now that NodeCleanupSucceeded is True, the target CNI pods should unblock and become Ready
	if err = waitForModule(ctx, rtClient, "cni-"+strings.ToLower(targetCNI), true); err != nil {
		return fmt.Errorf("waiting for module '%s' to be ready: %w", targetCNI, err)
	}
	fmt.Printf("✅ CNI module 'cni-%s' is now Ready (total elapsed: %s)\n\n",
		targetCNI,
		time.Since(startTime).Round(time.Millisecond))

	// 10. Delete Mutating Webhook
	fmt.Println("Deleting Mutating Webhook...")
	if err = deleteMutatingWebhook(ctx, rtClient); err != nil {
		return fmt.Errorf("deleting mutating webhook: %w", err)
	}
	fmt.Printf("✅ Mutating webhook deleted (total elapsed: %s)\n\n",
		time.Since(startTime).Round(time.Millisecond))

	// 11. Signal 'NewCNIEnabled'
	fmt.Println("Signaling 'NewCNIEnabled' to proceed with pod restart...")
	if err = updateCNIMigrationStatus(ctx, rtClient, activeMigration.Name, metav1.Condition{
		Type:    "NewCNIEnabled",
		Status:  metav1.ConditionTrue,
		Reason:  "ModuleEnabled",
		Message: fmt.Sprintf("Module 'cni-%s' was successfully enabled.", targetCNI),
	}); err != nil {
		return fmt.Errorf("updating CNIMigration status: %w", err)
	}
	fmt.Printf("✅ CNIMigration status updated (total elapsed: %s)\n\n",
		time.Since(startTime).Round(time.Millisecond))

	// 12. Wait for pods to be restarted
	fmt.Println("Waiting for pods to be restarted on all nodes...")
	// We do not hard fail here, as per ADR suggestions.
	if err = waitForNodeConditions(ctx, rtClient, activeMigration, "PodsRestarted"); err != nil {
		fmt.Printf("⚠️  Warning: Timed out waiting for pods to restart: %v\n", err)
		fmt.Println("Please check the cluster status manually. The CNI switch is otherwise complete.")
	} else {
		fmt.Printf("✅ All pods restarted (total elapsed: %s)\n\n",
			time.Since(startTime).Round(time.Millisecond))
	}

	// 13. Finalize migration
	fmt.Println("Finalizing migration...")

	// Update condition 'Succeeded' using the helper
	if err = updateCNIMigrationStatus(ctx, rtClient, activeMigration.Name, metav1.Condition{
		Type:               "Succeeded",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "MigrationComplete",
		Message:            "CNI migration completed successfully.",
	}); err != nil {
		fmt.Printf("\n  ⚠️  Warning: Failed to update status (Succeeded): %v. Proceeding...\n", err)
	}

	fmt.Printf(
		"🎉 CNI switch to '%s' completed successfully! (total time: %s)\n",
		targetCNI,
		time.Since(startTime).Round(time.Second),
	)
	fmt.Println("\nYou can now run 'd8 cni-switch cleanup' to remove auxiliary resources.")

	return nil
}

func toggleModule(ctx context.Context, cl client.Client, moduleName string, toggle bool) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			mc := &unstructured.Unstructured{}
			mc.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "deckhouse.io",
				Version: "v1alpha1",
				Kind:    "ModuleConfig",
			})

			err := cl.Get(ctx, types.NamespacedName{Name: moduleName}, mc)
			if err != nil {
				fmt.Printf("\r  ⚠️  Error getting module config '%s': %v. Retrying...", moduleName, err)
				continue
			}

			spec, found, err := unstructured.NestedMap(mc.Object, "spec")
			if err != nil {
				fmt.Printf("\r  ⚠️  Error getting spec from module config '%s': %v. Retrying...", moduleName, err)
				continue
			}
			if !found {
				spec = make(map[string]any)
			}

			// Skip update if value is already correct
			if currentToggle, ok := spec["enabled"].(bool); ok && currentToggle == toggle {
				return nil
			}

			spec["enabled"] = toggle

			if err := unstructured.SetNestedMap(mc.Object, spec, "spec"); err != nil {
				fmt.Printf("\r  ⚠️  Error setting spec for module config '%s': %v. Retrying...", moduleName, err)
				continue
			}

			if err := cl.Update(ctx, mc); err != nil {
				fmt.Printf("\r  ⚠️  Error updating module config '%s': %v. Retrying...", moduleName, err)
				continue
			}
			return nil
		}
	}
}

func waitForModule(ctx context.Context, cl client.Client, moduleName string, shouldBeReady bool) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			module := &unstructured.Unstructured{}
			module.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "deckhouse.io",
				Version: "v1alpha1",
				Kind:    "Module",
			})

			err := cl.Get(ctx, types.NamespacedName{Name: moduleName}, module)

			if shouldBeReady {
				if err != nil {
					if errors.IsNotFound(err) {
						fmt.Printf("\r  Waiting for module '%s': not found yet...", moduleName)
					} else {
						fmt.Printf("\r  ⚠️  Error getting module '%s': %v. Retrying...", moduleName, err)
					}
					continue
				}

				state, found, err := unstructured.NestedString(module.Object, "status", "phase")
				if err != nil || !found {
					fmt.Printf("\r  Waiting for module '%s': status.phase field not found. Retrying...",
						moduleName)
					continue
				}

				if state == "Ready" {
					fmt.Printf("Module '%s' is Ready.", moduleName)
					fmt.Println()
					return nil
				}
				fmt.Printf("\r  Waiting for module '%s' to be Ready, current state: %s", moduleName, state)

			} else { // should NOT be ready (disabled)
				err := cl.Get(ctx, types.NamespacedName{Name: moduleName}, module)
				if err != nil {
					if errors.IsNotFound(err) {
						fmt.Printf("\r  Module '%s' is not found, assuming disabled.", moduleName)
						fmt.Println()
						return nil
					}
					fmt.Printf("\r  ⚠️  Error getting module '%s': %v. Retrying...", moduleName, err)
					continue
				}

				// Check conditions to see if it's disabled
				conditions, found, err := unstructured.NestedSlice(module.Object, "status", "conditions")
				if err != nil || !found {
					fmt.Printf("\r  Waiting for module '%s' status conditions. Retrying...", moduleName)
					continue
				}

				isReadyFound := false
				for _, c := range conditions {
					condition, ok := c.(map[string]any)
					if !ok {
						continue
					}

					condType, found, err := unstructured.NestedString(condition, "type")
					if err != nil || !found {
						continue
					}

					if condType == "IsReady" {
						isReadyFound = true
						condStatus, _, _ := unstructured.NestedString(condition, "status")
						if condStatus == "False" {
							fmt.Printf("\r✅ Module '%s' is disabled (IsReady=False).\n", moduleName)
							fmt.Println()
							return nil
						}
					}
				}

				if !isReadyFound {
					fmt.Printf("\r  Waiting for module '%s' to be disabled, 'IsReady' condition not found...",
						moduleName)
				} else {
					fmt.Printf("\r  Waiting for module '%s' to be disabled (IsReady=False)...", moduleName)
				}
			}
		}
	}
}

func updateCNIMigrationStatus(
	ctx context.Context, cl client.Client, migrationName string, newCondition metav1.Condition,
) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			migration := &v1alpha1.CNIMigration{}
			err := cl.Get(ctx, types.NamespacedName{Name: migrationName}, migration)
			if err != nil {
				fmt.Printf("\r  ⚠️  Error getting CNIMigration '%s': %v. Retrying...", migrationName, err)
				continue
			}

			patchedMigration := migration.DeepCopy()
			newCondition.LastTransitionTime = metav1.Now()

			found := false
			for i, cond := range patchedMigration.Status.Conditions {
				if cond.Type == newCondition.Type {
					// Check if update is needed
					if cond.Status == newCondition.Status && cond.Reason == newCondition.Reason &&
						cond.Message == newCondition.Message {
						return nil // Already up to date
					}
					patchedMigration.Status.Conditions[i] = newCondition
					found = true
					break
				}
			}

			if !found {
				patchedMigration.Status.Conditions = append(patchedMigration.Status.Conditions, newCondition)
			}

			err = cl.Status().Patch(ctx, patchedMigration, client.MergeFrom(migration))
			if err == nil {
				return nil
			}

			if !errors.IsConflict(err) {
				fmt.Printf("\r  ⚠️  Error patching CNIMigration status: %v. Retrying...", err)
			}
		}
	}
}

func waitForNodeConditions(
	ctx context.Context, cl client.Client, cniMigration *v1alpha1.CNIMigration, conditionType string,
) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	reportedFailures := make(map[string]struct{})

	// Helper to update status stats
	updateStats := func(total, succeeded, failed int) {
		// Fetch fresh object to avoid conflicts
		freshMigration := &v1alpha1.CNIMigration{}
		if err := cl.Get(ctx, types.NamespacedName{Name: cniMigration.Name}, freshMigration); err != nil {
			return // Ignore error, will retry next tick
		}

		if freshMigration.Status.NodesTotal == total &&
			freshMigration.Status.NodesSucceeded == succeeded &&
			freshMigration.Status.NodesFailed == failed {
			return
		}

		patched := freshMigration.DeepCopy()
		patched.Status.NodesTotal = total
		patched.Status.NodesSucceeded = succeeded
		patched.Status.NodesFailed = failed

		_ = cl.Status().Patch(ctx, patched, client.MergeFrom(freshMigration))
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Get current total nodes
			nodeList := &corev1.NodeList{}
			if err := cl.List(ctx, nodeList); err != nil {
				fmt.Printf("\r  ⚠️  Error listing nodes: %v. Retrying...", err)
				continue
			}
			totalNodes := len(nodeList.Items)

			// Get current node migrations
			nodeMigrations := &v1alpha1.CNINodeMigrationList{}
			if err := cl.List(ctx, nodeMigrations); err != nil {
				fmt.Printf("\r  ⚠️  Error listing CNINodeMigration resources: %v. Retrying...", err)
				continue
			}

			succeededNodes := 0
			failedNodes := 0

			for _, nm := range nodeMigrations.Items {
				if !metav1.IsControlledBy(&nm, cniMigration) {
					continue
				}

				// Check specific condition success
				isSucceeded := false
				for _, cond := range nm.Status.Conditions {
					if cond.Type == conditionType && cond.Status == metav1.ConditionTrue {
						succeededNodes++
						isSucceeded = true

						// Check if it was previously failed
						if _, wasFailed := reportedFailures[nm.Name]; wasFailed {
							fmt.Printf("\r\033[K") // Clear line
							fmt.Printf("  ✅ Node '%s' has recovered.\n", nm.Name)
							delete(reportedFailures, nm.Name)
						}
						break
					}
				}

				if isSucceeded {
					continue
				}

				// Check failure
				for _, cond := range nm.Status.Conditions {
					if cond.Type == "Failed" && cond.Status == metav1.ConditionTrue {
						failedNodes++
						if _, reported := reportedFailures[nm.Name]; !reported {
							// Clear line, print error, then let the progress bar overwrite next
							fmt.Printf("\r\033[K") // Clear current line
							fmt.Printf("  ❌ Node '%s' failed: %s\n", nm.Name, cond.Message)
							reportedFailures[nm.Name] = struct{}{}
						}
						break
					}
				}
			}

			// Update status in CNIMigration
			go updateStats(totalNodes, succeededNodes, failedNodes)

			// Output progress
			fmt.Printf("\r  Progress: %d/%d nodes completed, %d failed.   ",
				succeededNodes, totalNodes, failedNodes)

			// 5. Check exit condition
			if succeededNodes >= totalNodes {
				fmt.Println()
				return nil
			}
		}
	}
}

func deleteMutatingWebhook(ctx context.Context, cl client.Client) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	fmt.Printf("  Deleting mutating webhook '%s'...", webhookConfigName)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			webhook := &admissionregistrationv1.MutatingWebhookConfiguration{
				ObjectMeta: metav1.ObjectMeta{
					Name: webhookConfigName,
				},
			}
			if err := cl.Delete(ctx, webhook); err != nil {
				if errors.IsNotFound(err) {
					fmt.Println("\r  Mutating webhook not found, assuming already deleted.")
					return nil
				}
				fmt.Printf("\r  ⚠️  Error deleting mutating webhook: %v. Retrying...", err)
				continue
			}
			fmt.Println("- Mutating webhook deleted")
			return nil
		}
	}
}

// getDaemonSetNameForCNI returns the name of the main DaemonSet for a given CNI module.
func getDaemonSetNameForCNI(cniModule string) (string, error) {
	switch cniModule {
	case "cni-cilium":
		return "agent", nil
	case "cni-flannel":
		return "flannel", nil
	case "cni-simple-bridge":
		return "simple-bridge", nil
	default:
		return "", fmt.Errorf("unknown CNI module: %s", cniModule)
	}
}

// waitForModulePodsInitializing waits for all pods of a module's daemonset to be in the 'Initializing' state,
// specifically waiting in the init container.
func waitForModulePodsInitializing(ctx context.Context, cl client.Client, moduleName string, dsName string) error {
	fmt.Printf("  Waiting for pods of module '%s' to enter 'Initializing' state...", moduleName)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Get the DaemonSet
			ds := &appsv1.DaemonSet{}
			err := cl.Get(ctx, types.NamespacedName{Name: dsName, Namespace: "d8-" + moduleName}, ds)
			if err != nil {
				if errors.IsNotFound(err) {
					fmt.Printf("\r  Waiting for DaemonSet '%s' in namespace 'd8-%s': not found...", dsName, moduleName)
					continue
				}
				fmt.Printf("\r  Error getting DaemonSet '%s' in namespace 'd8-%s': %v. Retrying...",
					dsName, moduleName, err)
				continue
			}

			// Check if pods are scheduled
			if ds.Status.DesiredNumberScheduled == 0 {
				fmt.Printf("\r  Waiting for DaemonSet '%s' to schedule pods...", dsName)
				continue
			}

			// List pods
			podList := &corev1.PodList{}
			opts := []client.ListOption{
				client.InNamespace("d8-" + moduleName),
				client.MatchingLabels(ds.Spec.Selector.MatchLabels),
			}
			if err := cl.List(ctx, podList, opts...); err != nil {
				fmt.Printf("\r  Error listing pods for module '%s': %v. Retrying...", moduleName, err)
				continue
			}

			if len(podList.Items) == 0 {
				fmt.Printf("\r  Waiting for pods of DaemonSet '%s' to be created...", dsName)
				continue
			}

			initializingPods := 0
			for _, pod := range podList.Items {
				// Check pod status
				if pod.Status.Phase == corev1.PodPending || pod.Status.Phase == corev1.PodRunning {
					for _, initStatus := range pod.Status.InitContainerStatuses {
						if initStatus.Name == "cni-switch-init-checker" &&
							(initStatus.State.Waiting != nil || initStatus.State.Running != nil) {
							// Pod is waiting in or running our init-container
							initializingPods++
							break
						}
					}
				}
			}

			fmt.Printf("\r  Progress: %d/%d pods are in 'Initializing' state.",
				initializingPods, ds.Status.DesiredNumberScheduled)

			if int32(initializingPods) >= ds.Status.DesiredNumberScheduled {
				fmt.Println("\n- All pods for target CNI are correctly waiting in the init-container.")
				return nil
			}
		}
	}
}

// waitForModulePodsTermination waits for all pods of a module's daemonset to be terminated.
func waitForModulePodsTermination(ctx context.Context, cl client.Client, moduleName string, dsName string) error {
	fmt.Printf("  Waiting for pods of module '%s' to terminate...", moduleName)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// List pods with label app=<dsName> in namespace d8-<moduleName>
			podList := &corev1.PodList{}
			opts := []client.ListOption{
				client.InNamespace("d8-" + moduleName),
				client.MatchingLabels(map[string]string{"app": dsName}),
			}
			if err := cl.List(ctx, podList, opts...); err != nil {
				fmt.Printf("\r  Error listing pods for module '%s': %v. Retrying...", moduleName, err)
				continue
			}

			if len(podList.Items) == 0 {
				fmt.Println("- All pods for disabled CNI module are terminated.")
				return nil
			}

			fmt.Printf("\r  Waiting for %d pods of module '%s' to terminate...", len(podList.Items), moduleName)
		}
	}
}

// verifyResourcesExist checks if all expected resources are present in the cluster.
func verifyResourcesExist(ctx context.Context, rtClient client.Client) error {
	resources := []struct {
		kind      string
		name      string
		namespace string
		obj       client.Object
	}{
		{"Namespace", cniSwitchNamespace, "", &corev1.Namespace{}},
		{"ServiceAccount", switchHelperServiceAccountName, cniSwitchNamespace, &corev1.ServiceAccount{}},
		{"ServiceAccount", webhookServiceAccountName, cniSwitchNamespace, &corev1.ServiceAccount{}},
		{"ClusterRole", switchHelperClusterRoleName, "", &rbacv1.ClusterRole{}},
		{"ClusterRole", webhookClusterRoleName, "", &rbacv1.ClusterRole{}},
		{"ClusterRoleBinding", switchHelperClusterRoleBindingName, "", &rbacv1.ClusterRoleBinding{}},
		{"ClusterRoleBinding", webhookClusterRoleBindingName, "", &rbacv1.ClusterRoleBinding{}},
		{"Secret", webhookSecretName, cniSwitchNamespace, &corev1.Secret{}},
		{"Service", webhookServiceName, cniSwitchNamespace, &corev1.Service{}},
		{"MutatingWebhookConfiguration", webhookConfigName, "", &admissionregistrationv1.MutatingWebhookConfiguration{}},
		{"DaemonSet", switchHelperDaemonSetName, cniSwitchNamespace, &appsv1.DaemonSet{}},
		{"Deployment", webhookDeploymentName, cniSwitchNamespace, &appsv1.Deployment{}},
	}

	for _, r := range resources {
		key := client.ObjectKey{Name: r.name, Namespace: r.namespace}
		if err := rtClient.Get(ctx, key, r.obj); err != nil {
			return fmt.Errorf("resource %s '%s' not found: %w", r.kind, r.name, err)
		}
	}
	return nil
}

func updateCNIMigrationPhase(ctx context.Context, cl client.Client, migrationName string, phase string) error {
	update := func() error { // TDEN переписать по типу других
		migration := &v1alpha1.CNIMigration{}
		if err := cl.Get(ctx, types.NamespacedName{Name: migrationName}, migration); err != nil {
			return fmt.Errorf("getting CNIMigration: %w", err)
		}

		if migration.Spec.Phase == phase {
			return nil
		}

		patchedMigration := migration.DeepCopy()
		patchedMigration.Spec.Phase = phase
		if err := cl.Patch(ctx, patchedMigration, client.MergeFrom(migration)); err != nil {
			return fmt.Errorf("patching CNIMigration: %w", err)
		}
		return nil
	}

	if err := update(); err == nil {
		return nil
	} else {
		fmt.Printf("\r  ⚠️  %v. Retrying...", err)
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := update(); err != nil {
				fmt.Printf("\r  ⚠️  %v. Retrying...", err)
				continue
			}
			return nil
		}
	}
}
