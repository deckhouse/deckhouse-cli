package operatemodule

import (
	"context"
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

func OperateModule(config *rest.Config, name string, enabled bool) error {
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("Failed to create dynamic client: %v", err)
	}

	resourceClient := dynamicClient.Resource(
		schema.GroupVersionResource{
			Group:    "deckhouse.io",
			Version:  "v1alpha1",
			Resource: "moduleconfigs",
		},
	)

	customResource, err := resourceClient.Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Error get options module '%s': %w", name, err)
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

	obj, err := createModuleConfig(name, enabled)
	if err != nil {
		return fmt.Errorf("failed to convert the '%s' module config: %w", name, err)
	}
	if _, err = resourceClient.Create(context.TODO(), obj, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create the '%s' module config: %w", name, err)
	}
	return err
}

func createModuleConfig(name string, enabled bool) (*unstructured.Unstructured, error) {
	newCfg := &v1alpha1.ModuleConfigMeta{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ModuleConfig",
			APIVersion: "deckhouse.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1alpha1.ModuleConfigSpec{
			Enabled: ptr.To(enabled),
		},
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(newCfg)
	return &unstructured.Unstructured{Object: content}, err
}
