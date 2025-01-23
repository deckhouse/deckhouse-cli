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
	"sigs.k8s.io/controller-runtime/pkg/client"
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

	// Create a dynamic client
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create dynamic client: %v", err)
	}

	resourceClient := dynamicClient.Resource(
		schema.GroupVersionResource{
			Group:    "deckhouse.io",
			Version:  "v1alpha1",
			Resource: "moduleconfigs",
		},
	)

	customResource, err := resourceClient.Get(context.TODO(), name, metav1.GetOptions{})
	if client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("Failed to get moduleconfig %s: %v", name, err)
	}

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
			Kind:       "ModuleConfig",
			APIVersion: "deckhouse_io",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1alpha1.ModuleConfigSpec{
			Enabled: ptr.To(enabled),
		},
	}

	//// Create the custom resource
	//createdResource, err := resource.Create(context.TODO(), customResource, v1.CreateOptions{})
	//if err != nil {
	//	log.Fatalf("Failed to create custom resource '%s': %v", name, err)
	//}

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
