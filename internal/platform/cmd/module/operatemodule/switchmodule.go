package operatemodule

import (
	"context"
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/utils/ptr"
	"log"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// updates spec.enabled flag or creates a new ModuleConfig with spec.enabled flag.
func OperateModule(cmd *cobra.Command, name string, enabled bool) error {

	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, _, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	// CustomResource represents the CRD you're trying to fetch.
	const customResourceGroup = "deckhouse.io"   // The group of your CRD
	const customResourceVersion = "v1alpha1"     // The version of your CRD
	const customResourcePlural = "moduleconfigs" // Plural name of your custom resource
	const customResourceNamespace = "default"    // Namespace (can be empty for cluster-wide)

	// Create a dynamic client
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create dynamic client: %v", err)
	}

	// Fetch the custom resource using the dynamic client
	resourceClient := dynamicClient.Resource(
		schema.GroupVersionResource{
			Group:    customResourceGroup,
			Version:  customResourceVersion,
			Resource: customResourcePlural,
		},
	)

	// Get the custom resource by name (or list them)
	//customResourceName := "example-resource" // Name of the custom resource you want to fetch

	customResource, err := resourceClient.Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		log.Fatalf("Failed to get custom resource: %v", err)
	}

	//unstructuredObj, err := kubeCl.dynamicClient().Resource(v1alpha1.ModuleConfigGVR).Get(context.TODO(), name, metav1.GetOptions{})
	//if client.IgnoreNotFound(err) != nil {
	//	return fmt.Errorf("failed to get the '%s' module config: %w", name, err)
	//}

	if customResource != nil {
		if err = unstructured.SetNestedField(customResource.Object, enabled, "spec", "enabled"); err != nil {
			return fmt.Errorf("failed to change spec.enabled to %v in the '%s' module config: %w", enabled, name, err)
		}
		if _, err = resourceClient.Update(context.TODO(), customResource, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("failed to update the '%s' module config: %w", name, err)
		}
		return nil
	}

	//if unstructuredObj != nil {
	//	if err = unstructured.SetNestedField(unstructuredObj.Object, enabled, "spec", "enabled"); err != nil {
	//		return fmt.Errorf("failed to change spec.enabled to %v in the '%s' module config: %w", enabled, name, err)
	//	}
	//	if _, err = kubeClient.Dynamic().Resource(v1alpha1.ModuleConfigGVR).Update(context.TODO(), unstructuredObj, metav1.UpdateOptions{}); err != nil {
	//		return fmt.Errorf("failed to update the '%s' module config: %w", name, err)
	//	}
	//	return nil
	//}

	// create new ModuleConfig if absent.
	newCfg := &v1alpha1.ModuleConfig{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.ModuleConfigGVK.Kind,
			APIVersion: v1alpha1.ModuleConfigGVK.GroupVersion().String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1alpha1.ModuleConfigSpec{
			Enabled: ptr.To(enabled),
		},
	}

	obj, err := ToUnstructured(newCfg)
	if err != nil {
		return fmt.Errorf("failed to convert the '%s' module config: %w", name, err)
	}

	if _, err = resourceClient.Create(context.TODO(), obj, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create the '%s' module config: %w", name, err)
	}
	return nil
}

func ToUnstructured(obj interface{}) (*unstructured.Unstructured, error) {
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	return &unstructured.Unstructured{Object: content}, err
}
