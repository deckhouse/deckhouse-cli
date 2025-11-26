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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RunCleanup executes the logic for the 'cni-switch cleanup' command.
// It performs a robust cleanup by deleting a known list of resources
// and waiting for them to be fully terminated.
func RunCleanup(timeout time.Duration) error {
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fmt.Printf("🚀 Starting CNI switch cleanup (global timeout: %s)\n", timeout)

	// 1. Create Kubernetes client
	safeClient, err := saferequest.NewSafeClient()
	if err != nil {
		return fmt.Errorf("creating safe client: %w", err)
	}

	rtClient, err := safeClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return fmt.Errorf("creating runtime client: %w", err)
	}
	fmt.Printf("✅ Kubernetes client created\n\n")

	// 2. Delete cluster-scoped resources first, with waiting
	fmt.Println("Deleting cluster-scoped resources...")
	webhookConfig := &admissionregistrationv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: webhookConfigName}}
	if err = deleteAndWait(ctx, rtClient, webhookConfig); err != nil {
		return err
	}
	clusterRoleHelper := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "d8:cni-switch-helper"}}
	if err = deleteAndWait(ctx, rtClient, clusterRoleHelper); err != nil {
		return err
	}
	clusterRoleWebhook := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "d8:cni-switch-webhook"}}
	if err = deleteAndWait(ctx, rtClient, clusterRoleWebhook); err != nil {
		return err
	}
	clusterRoleBindingHelper := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "d8:cni-switch-helper"}}
	if err = deleteAndWait(ctx, rtClient, clusterRoleBindingHelper); err != nil {
		return err
	}
	clusterRoleBindingWebhook := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "d8:cni-switch-webhook"}}
	if err = deleteAndWait(ctx, rtClient, clusterRoleBindingWebhook); err != nil {
		return err
	}
	fmt.Println("✅ Cluster-scoped resources deleted")

	// 3. Delete namespaced resources, with waiting
	fmt.Printf("\nDeleting namespaced resources in '%s'...\n", cniSwitchNamespace)
	webhookService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: webhookServiceName, Namespace: cniSwitchNamespace}}
	if err = deleteAndWait(ctx, rtClient, webhookService); err != nil {
		return err
	}
	webhookDep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: webhookDeploymentName, Namespace: cniSwitchNamespace}}
	if err = deleteAndWait(ctx, rtClient, webhookDep); err != nil {
		return err
	}
	helperDs := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "cni-switch-helper", Namespace: cniSwitchNamespace}}
	if err = deleteAndWait(ctx, rtClient, helperDs); err != nil {
		return err
	}
	webhookSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: webhookSecretName, Namespace: cniSwitchNamespace}}
	if err = deleteAndWait(ctx, rtClient, webhookSecret); err != nil {
		return err
	}
	helperSA := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: switchHelperServiceAccountName, Namespace: cniSwitchNamespace}}
	if err = deleteAndWait(ctx, rtClient, helperSA); err != nil {
		return err
	}
	webhookSA := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: webhookServiceAccountName, Namespace: cniSwitchNamespace}}
	if err = deleteAndWait(ctx, rtClient, webhookSA); err != nil {
		return err
	}
	fmt.Println("✅ Namespaced resources deleted")

	// 4. Delete the namespace itself
	fmt.Printf("\nDeleting namespace '%s'...\n", cniSwitchNamespace)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cniSwitchNamespace}}
	if err = deleteAndWait(ctx, rtClient, ns); err != nil {
		return err
	}
	fmt.Println("✅ Namespace successfully deleted")

	// 5. Delete all CNINodeMigration resources
	fmt.Println("\nDeleting all CNINodeMigration resources...")
	nodeMigrations := &v1alpha1.CNINodeMigrationList{}
	if err = rtClient.List(ctx, nodeMigrations); err != nil && !isCRDNotFound(err) {
		return fmt.Errorf("listing CNINodeMigrations: %w", err)
	}
	for _, nm := range nodeMigrations.Items {
		if err = deleteAndWait(ctx, rtClient, &nm); err != nil {
			return err
		}
	}
	fmt.Println("✅ All CNINodeMigration resources deleted")

	// 6. Delete all CNIMigration resources
	fmt.Println("\nDeleting all CNIMigration resources...")
	migrations := &v1alpha1.CNIMigrationList{}
	if err = rtClient.List(ctx, migrations); err != nil && !isCRDNotFound(err) {
		return fmt.Errorf("listing CNIMigrations: %w", err)
	}
	for _, m := range migrations.Items {
		if err = deleteAndWait(ctx, rtClient, &m); err != nil {
			return err
		}
	}
	fmt.Println("✅ All CNIMigration resources deleted")

	fmt.Printf("\n🎉 CNI switch cleanup successfully completed (total time: %s)\n", time.Since(startTime).Round(time.Second))
	return nil
}

// deleteAndWait deletes a resource and waits for it to be fully terminated.
func deleteAndWait(ctx context.Context, rtClient client.Client, obj client.Object) error {
	if err := deleteResource(ctx, rtClient, obj); err != nil {
		// This handles the "already deleted" case, so we don't need to check again.
		return err
	}
	return waitForResourceDeletion(ctx, rtClient, obj)
}

// deleteResource just initiates deletion for a Kubernetes resource.
func deleteResource(ctx context.Context, rtClient client.Client, obj client.Object) error {
	kind := getKind(obj)
	name := obj.GetName()
	ns := obj.GetNamespace()

	fmt.Printf("- Deleting %s '%s%s'... ", kind, name, func() string {
		if ns != "" {
			return fmt.Sprintf("' in namespace '%s", ns)
		}
		return ""
	}())

	err := rtClient.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if err != nil {
		if errors.IsNotFound(err) {
			fmt.Println("already deleted.")
			return nil
		}
		return fmt.Errorf("failed to delete %s '%s': %w", kind, name, err)
	}

	fmt.Println("deleted.")
	return nil
}

// waitForResourceDeletion polls until a resource is confirmed to be gone.
func waitForResourceDeletion(ctx context.Context, rtClient client.Client, obj client.Object) error {
	// If the object was already gone, deleteResource would have returned nil,
	// but the object itself is empty. We need its name/namespace for the Get call.
	key := client.ObjectKey{Name: obj.GetName(), Namespace: obj.GetNamespace()}
	kind := getKind(obj)

	// Check if the resource is already gone before starting to wait.
	// This happens if deleteResource returned an IsNotFound error.
	err := rtClient.Get(ctx, key, obj)
	if err != nil {
		if errors.IsNotFound(err) {
			// It was already gone, no need to wait.
			return nil
		}
	}

	fmt.Printf("  Waiting for %s '%s' to terminate... ", kind, key.Name)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %s '%s' to be deleted", kind, key.Name)
		case <-ticker.C:
			err := rtClient.Get(ctx, key, obj)
			if err != nil {
				if errors.IsNotFound(err) {
					fmt.Println("terminated.")
					return nil // Success!
				}
				if isCRDNotFound(err) {
					fmt.Println("CRD not found, assuming terminated.")
					return nil // CRD itself is gone, so is the object.
				}
				return fmt.Errorf("getting %s '%s': %w", kind, key.Name, err)
			}
		}
	}
}

// getKind extracts a user-friendly kind from a runtime object.
func getKind(obj client.Object) string {
	kind := obj.GetObjectKind().GroupVersionKind().Kind
	if kind == "" {
		t := fmt.Sprintf("%T", obj)
		if t_ := t[strings.LastIndex(t, ".")+1:]; t_ != "" {
			return t_
		}
	}
	return kind
}

// isCRDNotFound checks if an error indicates that the CRD itself is not found.
func isCRDNotFound(err error) bool {
	// This is a heuristic. A proper check might involve more specific error types
	// if the client library provides them, but this is often what you get.
	return strings.Contains(err.Error(), "no matches for kind")
}
