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
	fmt.Printf("✅ Kubernetes client created\n")

	// 2. Delete cluster-scoped resources first, with waiting
	fmt.Println("\nDeleting cluster-scoped resources...")
	webhookConfig := &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: webhookConfigName},
	}
	if err = deleteAndWait(ctx, rtClient, webhookConfig); err != nil {
		return err
	}

	// 3. Stop active controllers first to prevent reconciliation loops
	fmt.Printf("  Stopping active controllers in '%s'...\n", cniSwitchNamespace)
	helperDs := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: switchHelperDaemonSetName, Namespace: cniSwitchNamespace},
	}
	if err = deleteAndWait(ctx, rtClient, helperDs); err != nil {
		return err
	}
	webhookDep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: webhookDeploymentName, Namespace: cniSwitchNamespace},
	}
	if err = deleteAndWait(ctx, rtClient, webhookDep); err != nil {
		return err
	}
	fmt.Println("✅ Active controllers stopped")

	// 4. Delete all CNINodeMigration resources
	fmt.Println("\nDeleting all CNINodeMigration resources...")
	nodeMigrations := &v1alpha1.CNINodeMigrationList{}
	if err = rtClient.List(ctx, nodeMigrations); err != nil && !strings.Contains(err.Error(), "no matches for kind") {
		return fmt.Errorf("listing CNINodeMigrations: %w", err)
	}
	for _, nm := range nodeMigrations.Items {
		// Remove finalizers to ensure deletion even if controller is down
		if err = removeFinalizers(ctx, rtClient, &nm); err != nil {
			fmt.Printf("⚠️  Warning: failed to remove finalizers from %s: %v\n", nm.Name, err)
		}
		if err = deleteAndWait(ctx, rtClient, &nm); err != nil {
			return err
		}
	}
	fmt.Println("✅ All CNINodeMigration resources deleted")

	// 5. Delete all CNIMigration resources
	fmt.Println("\nDeleting all CNIMigration resources...")
	migrations := &v1alpha1.CNIMigrationList{}
	if err = rtClient.List(ctx, migrations); err != nil && !strings.Contains(err.Error(), "no matches for kind") {
		return fmt.Errorf("listing CNIMigrations: %w", err)
	}
	for _, m := range migrations.Items {
		// Remove finalizers to ensure deletion even if controller is down
		if err = removeFinalizers(ctx, rtClient, &m); err != nil {
			fmt.Printf("⚠️  Warning: failed to remove finalizers from %s: %v\n", m.Name, err)
		}
		if err = deleteAndWait(ctx, rtClient, &m); err != nil {
			return err
		}
	}
	fmt.Println("✅ All CNIMigration resources deleted")

	// 6. Remove annotations from all pods
	if err = removePodAnnotations(ctx, rtClient); err != nil {
		// Non-fatal, print a warning
		fmt.Printf("⚠️  Warning: failed to remove all pod annotations: %v\n", err)
	}

	// 7. Delete RBAC resources
	fmt.Println("\nDeleting RBAC resources...")
	clusterRoleHelper := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: switchHelperClusterRoleName},
	}
	if err = deleteAndWait(ctx, rtClient, clusterRoleHelper); err != nil {
		return err
	}
	clusterRoleWebhook := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: webhookClusterRoleName},
	}
	if err = deleteAndWait(ctx, rtClient, clusterRoleWebhook); err != nil {
		return err
	}
	clusterRoleBindingHelper := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: switchHelperClusterRoleBindingName},
	}
	if err = deleteAndWait(ctx, rtClient, clusterRoleBindingHelper); err != nil {
		return err
	}
	clusterRoleBindingWebhook := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: webhookClusterRoleBindingName},
	}
	if err = deleteAndWait(ctx, rtClient, clusterRoleBindingWebhook); err != nil {
		return err
	}
	fmt.Println("✅ Cluster-scoped RBAC resources deleted")

	// 8. Delete remaining namespaced resources
	fmt.Printf("\nDeleting remaining namespaced resources in '%s'...\n", cniSwitchNamespace)
	webhookService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: webhookServiceName, Namespace: cniSwitchNamespace},
	}
	if err = deleteAndWait(ctx, rtClient, webhookService); err != nil {
		return err
	}
	webhookSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: webhookSecretName, Namespace: cniSwitchNamespace},
	}
	if err = deleteAndWait(ctx, rtClient, webhookSecret); err != nil {
		return err
	}
	helperSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: switchHelperServiceAccountName, Namespace: cniSwitchNamespace},
	}
	if err = deleteAndWait(ctx, rtClient, helperSA); err != nil {
		return err
	}
	webhookSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: webhookServiceAccountName, Namespace: cniSwitchNamespace},
	}
	if err = deleteAndWait(ctx, rtClient, webhookSA); err != nil {
		return err
	}
	fmt.Println("✅ Remaining namespaced resources deleted")

	// 9. Delete the namespace
	fmt.Printf("\nDeleting namespace '%s'...\n", cniSwitchNamespace)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cniSwitchNamespace}}
	if err = deleteAndWait(ctx, rtClient, ns); err != nil {
		return err
	}
	fmt.Println("✅ Namespace successfully deleted")

	fmt.Printf("\n🎉 CNI switch cleanup successfully completed (total time: %s)\n",
		time.Since(startTime).Round(time.Second),
	)
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
	key := client.ObjectKey{Name: obj.GetName(), Namespace: obj.GetNamespace()}
	kind := getKind(obj)

	// Check if the resource is already gone before starting to wait.
	if err := rtClient.Get(ctx, key, obj); err != nil {
		if errors.IsNotFound(err) {
			// It was already gone, no need to wait.
			return nil
		}
		return fmt.Errorf("getting %s '%s': %w", kind, key.Name, err)
	}

	fmt.Printf("  Waiting for %s '%s' to terminate... ", kind, key.Name)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("timeout.")
			return fmt.Errorf("timed out waiting for %s '%s' to be deleted", kind, key.Name)
		case <-ticker.C:
			err := rtClient.Get(ctx, key, obj)
			if err != nil {
				if errors.IsNotFound(err) {
					fmt.Println("terminated.")
					return nil // Success!
				}
				if strings.Contains(err.Error(), "no matches for kind") {
					fmt.Println("Kind not known, assuming terminated.")
					return nil
				}
				fmt.Printf("error: %v\n", err)
				return fmt.Errorf("getting %s '%s': %w", kind, key.Name, err)
			}
		}
	}
}

func removePodAnnotations(ctx context.Context, rtClient client.Client) error {
	fmt.Println("\nRemoving CNI switch annotations from all pods...")

	podList := &corev1.PodList{}
	if err := rtClient.List(ctx, podList); err != nil {
		return fmt.Errorf("listing all pods: %w", err)
	}

	podsPatched := 0
	for _, pod := range podList.Items {
		if pod.Spec.HostNetwork {
			continue
		}

		if _, ok := pod.Annotations[EffectiveCNIAnnotation]; ok {
			// Use a merge patch to remove just the one annotation
			patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{"%s":null}}}`, EffectiveCNIAnnotation))
			err := rtClient.Patch(ctx, &pod, client.RawPatch(client.Merge.Type(), patch))
			if err != nil {
				if errors.IsNotFound(err) {
					// No need to print warning, just continue
					continue
				}
				fmt.Printf("\n⚠️  Warning: failed to patch pod %s/%s: %v", pod.Namespace, pod.Name, err)
				continue
			}
			podsPatched++
			fmt.Printf("\r  Patched %d pods...", podsPatched)
		}
	}

	if podsPatched > 0 {
		fmt.Printf("\n✅ Removed annotations from %d pods.\n", podsPatched)
	} else {
		fmt.Print("✅ No pods with CNI switch annotations were found.\n")
	}

	return nil
}

// getKind extracts a user-friendly kind from a runtime object.
func getKind(obj client.Object) string {
	kind := obj.GetObjectKind().GroupVersionKind().Kind
	if kind == "" {
		t := fmt.Sprintf("%T", obj)
		parts := strings.Split(t, ".")
		if len(parts) > 0 {
			return parts[len(parts)-1]
		}
	}
	return kind
}

// removeFinalizers patches the object to remove all finalizers.
func removeFinalizers(ctx context.Context, rtClient client.Client, obj client.Object) error {
	if len(obj.GetFinalizers()) == 0 {
		return nil
	}

	patch := []byte(`{"metadata":{"finalizers":null}}`)
	return rtClient.Patch(ctx, obj, client.RawPatch(client.Merge.Type(), patch))
}
