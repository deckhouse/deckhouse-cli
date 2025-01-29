package operatemodule

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/utils/ptr"
)

type ModuleState string

const (
	ModuleEnabled  ModuleState = "enabled"
	ModuleDisabled ModuleState = "disabled"
)

func OperateModule(dynamicClient dynamic.Interface, name string, moduleState ModuleState) error {
	resourceClient := dynamicClient.Resource(
		schema.GroupVersionResource{
			Group:    "deckhouse.io",
			Version:  "v1alpha1",
			Resource: "moduleconfigs",
		},
	)

	customResource, err := resourceClient.Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	enabledSpec, err := patchSpec(moduleState)
	if customResource != nil {
		if _, err = resourceClient.Patch(context.TODO(), name, types.MergePatchType, enabledSpec, metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("failed to update the '%s' module config: %w", name, err)
		}
		return nil
	}

	obj, err := createModuleConfig(name, moduleState)
	if err != nil {
		return fmt.Errorf("failed to convert the '%s' module config: %w", name, err)
	}
	if _, err = resourceClient.Create(context.TODO(), obj, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create the '%s' module config: %w", name, err)
	}
	return err
}

func createModuleConfig(name string, moduleState ModuleState) (*unstructured.Unstructured, error) {
	newCfg := &v1alpha1.ModuleConfigMeta{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ModuleConfig",
			APIVersion: "deckhouse.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1alpha1.ModuleConfigSpec{
			Enabled: ptr.To(moduleState == ModuleEnabled),
		},
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(newCfg)
	return &unstructured.Unstructured{Object: content}, err
}

func patchSpec(moduleState ModuleState) ([]byte, error) {
	patchData := map[string]interface{}{
		"spec": map[string]interface{}{
			"enabled": moduleState == ModuleEnabled,
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return nil, fmt.Errorf("Error convert to json updated data: %w", err)
	}

	return patchBytes, nil
}
