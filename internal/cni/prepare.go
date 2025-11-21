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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	cniSwitchNamespace = "cni-switch"
)

var (
	moduleConfigGVK = schema.GroupVersionKind{
		Group:   "deckhouse.io",
		Version: "v1alpha1",
		Kind:    "ModuleConfig",
	}
)

// RunPrepare executes the logic for the 'cni-switch prepare' command.
func RunPrepare(targetCNI string, timeout time.Duration) error {
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fmt.Printf("🚀 Starting CNI switch preparation for target '%s' (global timeout: %s)\n", targetCNI, timeout)

	// 1. Create a Kubernetes client
	safeClient, err := saferequest.NewSafeClient()
	if err != nil {
		return fmt.Errorf("creating safe client: %w", err)
	}

	rtClient, err := safeClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return fmt.Errorf("creating runtime client: %w", err)
	}
	fmt.Printf("✅ Kubernetes client created (total elapsed: %s)\n\n", time.Since(startTime).Round(time.Millisecond))

	// 2. Find an existing migration or create a new one
	activeMigration, err := getOrCreateMigrationForPrepare(ctx, rtClient, targetCNI)
	if err != nil {
		return err
	}
	if activeMigration == nil {
		// This means preparation is already complete, and the user has been notified.
		return nil
	}
	fmt.Printf(
		"✅ Working with migration: '%s' (total elapsed: %s)\n\n",
		activeMigration.Name,
		time.Since(startTime).Round(time.Millisecond),
	)

	// 3. Create the dedicated namespace
	fmt.Printf("Creating dedicated namespace '%s'...\n", cniSwitchNamespace)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cniSwitchNamespace}}
	if err = rtClient.Create(ctx, ns); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("creating namespace %s: %w", cniSwitchNamespace, err)
	}
	fmt.Print("✅ Namespace ensured\n\n")

	// 4. Detect current CNI and update migration status
	if activeMigration.Status.CurrentCNI == "" {
		var currentCNI string
		currentCNI, err = detectCurrentCNI(rtClient)
		if err != nil {
			return fmt.Errorf("detecting current CNI: %w", err)
		}
		fmt.Printf("Detected current CNI: '%s'\n", currentCNI)

		if currentCNI == targetCNI {
			return fmt.Errorf("target CNI '%s' is the same as the current CNI. Nothing to do", targetCNI)
		}

		activeMigration.Status.CurrentCNI = currentCNI
		err = rtClient.Status().Update(ctx, activeMigration)
		if err != nil {
			return fmt.Errorf("updating migration status with current CNI: %w", err)
		}
		fmt.Printf(
			"✅ Added current CNI to migration status (total elapsed: %s)\n\n",
			time.Since(startTime).Round(time.Millisecond),
		)
	}

	// 5. Create the cni-switch-helper daemonset and wait for it to be ready
	dsKey := client.ObjectKey{Name: "cni-switch-helper", Namespace: cniSwitchNamespace}
	ds := &appsv1.DaemonSet{}
	err = rtClient.Get(ctx, dsKey, ds)

	if err != nil {
		if errors.IsNotFound(err) {
			// DaemonSet does not exist, so we need to create it.
			fmt.Println("DaemonSet 'cni-switch-helper' not found. Creating it...")

			cm := &corev1.ConfigMap{}
			if getCMErr := rtClient.Get(
				ctx,
				client.ObjectKey{Name: "d8-cli-data", Namespace: "d8-system"},
				cm,
			); getCMErr != nil {
				return fmt.Errorf("getting d8-cli-data configmap: %w", getCMErr)
			}

			imageName, ok := cm.Data["cni-switch-helper-image"]
			if !ok || imageName == "" {
				return fmt.Errorf("cni-switch-helper-image not found or empty in d8-cli-data configmap")
			}

			// Create RBAC first
			fmt.Println("Applying RBAC for cni-switch-helper...")
			sa := getSwitchHelperServiceAccount(cniSwitchNamespace)
			role := getSwitchHelperClusterRole()
			binding := getSwitchHelperClusterRoleBinding(cniSwitchNamespace)

			if err = rtClient.Create(ctx, sa); err != nil && !errors.IsAlreadyExists(err) {
				return fmt.Errorf("creating service account: %w", err)
			}
			if err = rtClient.Create(ctx, role); err != nil && !errors.IsAlreadyExists(err) {
				return fmt.Errorf("creating cluster role: %w", err)
			}
			if err = rtClient.Create(ctx, binding); err != nil && !errors.IsAlreadyExists(err) {
				return fmt.Errorf("creating cluster role binding: %w", err)
			}
			fmt.Println("RBAC applied")

			dsToCreate := getSwitchHelperDaemonSet(cniSwitchNamespace, imageName)
			if createErr := rtClient.Create(ctx, dsToCreate); createErr != nil {
				return fmt.Errorf("creating helper daemonset: %w", createErr)
			}

			fmt.Printf("Successfully created DaemonSet '%s'\n", dsToCreate.Name)
			ds = dsToCreate // Use the newly created DS for the wait logic
		} else {
			// Some other error occurred while getting the DaemonSet.
			return fmt.Errorf("getting helper daemonset: %w", err)
		}
	} else {
		// DaemonSet already exists.
		fmt.Printf("DaemonSet '%s' already exists\n", ds.Name)
	}

	fmt.Println("Waiting for 'cni-switch-helper' DaemonSet to become ready")
	err = waitForDaemonSetReady(ctx, rtClient, ds)
	if err != nil {
		return fmt.Errorf("waiting for daemonset ready: %w", err)
	}
	fmt.Printf("✅ DaemonSet is ready (total elapsed: %s)\n\n", time.Since(startTime).Round(time.Millisecond))

	// // 5. Create the mutating webhook configuration
	// webhook := getMutatingWebhookConfiguration()
	// err = rtClient.Create(ctx, webhook)
	// if err != nil {
	// 	if errors.IsAlreadyExists(err) {
	// 		fmt.Printf("✅ MutatingWebhookConfiguration '%s' already exists\n\n", webhook.Name)
	// 	} else {
	// 		return fmt.Errorf("creating mutating webhook configuration: %w", err)
	// 	}
	// } else {
	// 	fmt.Printf(
	// 		"✅ Successfully created MutatingWebhook '%s' (total elapsed: %s)\n\n",
	// 		webhook.Name,
	// 		time.Since(startTime).Round(time.Millisecond),
	// 	)
	// }

	// 6. Wait for all nodes to be prepared
	fmt.Println("Waiting for all nodes to complete the preparation step...")
	err = waitForNodesPrepared(ctx, rtClient)
	if err != nil {
		return fmt.Errorf("waiting for nodes to be prepared: %w", err)
	}
	fmt.Printf("✅ All nodes are prepared (total elapsed: %s)\n\n", time.Since(startTime).Round(time.Millisecond))

	// 7. Update overall status
	activeMigration.Status.Conditions = append(activeMigration.Status.Conditions, metav1.Condition{
		Type:               "PreparationSucceeded",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "AllNodesPrepared",
		Message:            "All nodes successfully completed the preparation step.",
	})
	err = rtClient.Status().Update(ctx, activeMigration)
	if err != nil {
		return fmt.Errorf("updating CNIMigration status to prepared: %w", err)
	}

	fmt.Printf(
		"🎉 Cluster successfully prepared for CNI switch (total time: %s)\n",
		time.Since(startTime).Round(time.Second),
	)
	fmt.Println("You can now run 'd8 cni-switch switch' to proceed")

	return nil
}

func getOrCreateMigrationForPrepare(
	ctx context.Context,
	rtClient client.Client,
	targetCNI string,
) (*v1alpha1.CNIMigration, error) {
	activeMigration, err := FindActiveMigration(ctx, rtClient)
	if err != nil {
		return nil, fmt.Errorf("failed to find active migration: %w", err)
	}

	if activeMigration != nil {
		fmt.Printf("Found active migration '%s'\n", activeMigration.Name)

		// Check if preparation is already done
		for _, cond := range activeMigration.Status.Conditions {
			if cond.Type == "PreparationSucceeded" && cond.Status == metav1.ConditionTrue {
				fmt.Println("🎉 Cluster has already been prepared for CNI switch.")
				fmt.Println("You can now run 'd8 cni-switch switch' to proceed.")
				return nil, nil // Signal to the caller that we can exit gracefully
			}
		}

		// Check if migration is in an unexpected phase
		if activeMigration.Status.ObservedPhase != "" && activeMigration.Status.ObservedPhase != "Preparing" {
			return nil, fmt.Errorf(
				"an active migration is already in the '%s' phase. "+
					"Cannot run 'prepare' again. To proceed, run 'd8 cni-switch switch'. "+
					"To start over, run 'd8 cni-switch cleanup'",
				activeMigration.Status.ObservedPhase,
			)
		}

		return activeMigration, nil
	}

	migrationName := fmt.Sprintf("cni-migration-%s", time.Now().Format("20060102-150405"))
	fmt.Printf("No active migration found. Creating a new one...\n")

	newMigration := &v1alpha1.CNIMigration{
		ObjectMeta: metav1.ObjectMeta{
			Name: migrationName,
		},
		Spec: v1alpha1.CNIMigrationSpec{
			TargetCNI: targetCNI,
			Phase:     "Prepare",
		},
	}

	err = rtClient.Create(ctx, newMigration)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			fmt.Println("Migration object was created by another process. Getting it.")
			err = rtClient.Get(ctx, client.ObjectKey{Name: migrationName}, newMigration)
			if err != nil {
				return nil, fmt.Errorf("getting existing CNIMigration object: %w", err)
			}
			return newMigration, nil
		}
		return nil, fmt.Errorf("creating new CNIMigration object: %w", err)
	}

	newMigration.Status.ObservedPhase = "Preparing"
	err = rtClient.Status().Update(ctx, newMigration)
	if err != nil {
		return nil, fmt.Errorf("updating status for new CNIMigration object: %w", err)
	}

	fmt.Printf("Successfully created CNIMigration object '%s'\n", newMigration.Name)
	return newMigration, nil
}

func waitForDaemonSetReady(ctx context.Context, rtClient client.Client, ds *appsv1.DaemonSet) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			key := client.ObjectKey{Name: ds.Name, Namespace: ds.Namespace}
			err := rtClient.Get(ctx, key, ds)
			if err != nil {
				fmt.Printf("\n⚠️ Warning: could not get DaemonSet status: %v\n", err)
				continue
			}

			if ds.Status.DesiredNumberScheduled == ds.Status.NumberReady && ds.Status.NumberUnavailable == 0 {
				fmt.Printf(
					"\rWaiting for DaemonSet... %d/%d pods ready\n",
					ds.Status.NumberReady,
					ds.Status.DesiredNumberScheduled,
				)
				return nil
			}

			fmt.Printf(
				"\rWaiting for DaemonSet... %d/%d pods ready",
				ds.Status.NumberReady,
				ds.Status.DesiredNumberScheduled,
			)
		}
	}
}

func waitForNodesPrepared(ctx context.Context, rtClient client.Client) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			nodes := &corev1.NodeList{}
			if err := rtClient.List(ctx, nodes); err != nil {
				fmt.Printf("⚠️ Warning: could not list nodes: %v\n", err)
				continue
			}
			totalNodes := len(nodes.Items)

			migrations := &v1alpha1.CNINodeMigrationList{}
			if err := rtClient.List(ctx, migrations); err != nil {
				fmt.Printf("⚠️ Warning: could not list node migrations: %v\n", err)
				continue
			}

			readyNodes := 0
			for _, migration := range migrations.Items {
				for _, cond := range migration.Status.Conditions {
					if cond.Type == "PreparationSucceeded" && cond.Status == metav1.ConditionTrue {
						readyNodes++
						break
					}
				}
			}

			fmt.Printf("\rProgress: %d/%d nodes prepared...", readyNodes, totalNodes)

			if readyNodes >= totalNodes && totalNodes > 0 {
				fmt.Println() // Newline after progress bar
				return nil
			}
		}
	}
}

func detectCurrentCNI(rtClient client.Client) (string, error) {
	var enabledCNIs []string
	for _, cniModule := range cniModuleConfigs {
		mc := &unstructured.Unstructured{}
		mc.SetGroupVersionKind(moduleConfigGVK)

		err := rtClient.Get(context.Background(), client.ObjectKey{Name: cniModule}, mc)
		if err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return "", fmt.Errorf("getting module config %s: %w", cniModule, err)
		}

		enabled, found, err := unstructured.NestedBool(mc.Object, "spec", "enabled")
		if err != nil {
			return "", fmt.Errorf("parsing 'spec.enabled' for module config %s: %w", cniModule, err)
		}

		if found && enabled {
			cniName := strings.TrimPrefix(cniModule, "cni-")
			enabledCNIs = append(enabledCNIs, cniName)
		}
	}

	if len(enabledCNIs) == 0 {
		return "", fmt.Errorf("no enabled CNI module found. Looked for: %s", strings.Join(cniModuleConfigs, ", "))
	}

	if len(enabledCNIs) > 1 {
		return "", fmt.Errorf(
			"found multiple enabled CNI modules: %s. Please disable all but one",
			strings.Join(enabledCNIs, ", "),
		)
	}

	return enabledCNIs[0], nil
}
