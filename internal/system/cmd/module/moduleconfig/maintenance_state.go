package moduleconfig

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	constants "github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/const"
)

// MaintenanceState represents the desired maintenance state of a module.
type MaintenanceState string

const (
	// NoResourceReconciliation switches the module into maintenance mode.
	NoResourceReconciliation MaintenanceState = "NoResourceReconciliation"
	// DefaultReconciliation is the normal state; the spec.maintenance field is absent.
	DefaultReconciliation MaintenanceState = ""
)

// SetMaintenanceState sets the maintenance state of a module via its ModuleConfig resource.
// The ModuleConfig must already exist; if it does not, the underlying Kubernetes NotFound
// error is returned and can be detected by the caller with errors.IsNotFound.
func SetMaintenanceState(dynamicClient dynamic.Interface, name string, state MaintenanceState) (*Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultAPITimeout)
	defer cancel()

	resourceClient := dynamicClient.Resource(
		schema.GroupVersionResource{
			Group:    "deckhouse.io",
			Version:  "v1alpha1",
			Resource: "moduleconfigs",
		},
	)

	existing, err := resourceClient.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get module config '%s': %w", name, err)
	}

	// Check if module is already in the desired state
	if isMaintenanceState(existing, state) {
		return &Result{Status: AlreadyInState}, nil
	}

	patch, err := maintenancePatch(state)
	if err != nil {
		return nil, err
	}

	if _, err = resourceClient.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return nil, parseOperationError(name, err)
	}

	return &Result{Status: Changed}, nil
}

// maintenancePatch builds a JSON merge patch for spec.maintenance.
// DefaultReconciliation marshals the field as null, which removes it (back to normal).
func maintenancePatch(state MaintenanceState) ([]byte, error) {
	// DefaultReconciliation maps to JSON null, which removes the field;
	// any other state is written as its string value.
	var maintenance any
	if state != DefaultReconciliation {
		maintenance = string(state)
	}

	patchData := map[string]any{
		"spec": map[string]any{
			"maintenance": maintenance,
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal patch data: %w", err)
	}

	return patchBytes, nil
}

// isMaintenanceState reports whether the module is already in the desired
// maintenance state. An absent spec.maintenance field means module with DefaultReconciliation.
func isMaintenanceState(obj *unstructured.Unstructured, desiredState MaintenanceState) bool {
	maintenanceMode, found, err := unstructured.NestedString(obj.Object, "spec", "maintenance")
	if err != nil {
		return false
	}

	if !found {
		return desiredState == DefaultReconciliation
	}

	return MaintenanceState(maintenanceMode) == desiredState
}
