package module_config

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/utils/ptr"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/api/v1alpha1"
	constants "github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/const"
)

// EnabledState represents the desired enabled state of a module.
type EnabledState string

const (
	Enabled  EnabledState = "enabled"
	Disabled EnabledState = "disabled"
)

// ResultStatus represents the status of a SetEnabledState operation.
type ResultStatus int

const (
	// Changed indicates the module state was changed.
	Changed ResultStatus = iota
	// AlreadyInState indicates the module is already in the desired state.
	AlreadyInState
)

// Result contains the result of a SetEnabledState operation.
type Result struct {
	Status ResultStatus
}

// ExperimentalModuleError represents an error when trying to enable an experimental module.
type ExperimentalModuleError struct {
	ModuleName string
}

func (e *ExperimentalModuleError) Error() string {
	return fmt.Sprintf("module '%s' is experimental", e.ModuleName)
}

// experimentalModuleRegexp matches admission webhook errors for experimental modules.
var experimentalModuleRegexp = regexp.MustCompile(`the '([^']+)' module is experimental`)

// SetEnabledState sets the enabled state of a module via its ModuleConfig resource.
// If the ModuleConfig does not exist, it will be created.
func SetEnabledState(dynamicClient dynamic.Interface, name string, state EnabledState) (*Result, error) {
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
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get module config '%s': %w", name, err)
		}
		// Resource not found, create new one
		obj, err := createModuleConfig(name, state)
		if err != nil {
			return nil, fmt.Errorf("failed to convert the '%s' module config: %w", name, err)
		}
		if _, err = resourceClient.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
			return nil, parseOperationError(name, err)
		}
		return &Result{Status: Changed}, nil
	}

	// Check if module is already in the desired state
	if isInState(existing, state) {
		return &Result{Status: AlreadyInState}, nil
	}

	// Resource exists, patch it
	enabledSpec, err := patchSpec(state)
	if err != nil {
		return nil, err
	}
	if _, err = resourceClient.Patch(ctx, name, types.MergePatchType, enabledSpec, metav1.PatchOptions{}); err != nil {
		return nil, parseOperationError(name, err)
	}
	return &Result{Status: Changed}, nil
}

// isInState checks if the module is already in the desired state.
func isInState(obj *unstructured.Unstructured, desiredState EnabledState) bool {
	spec, found, err := unstructured.NestedMap(obj.Object, "spec")
	if err != nil || !found {
		return false
	}

	enabled, found, err := unstructured.NestedBool(spec, "enabled")
	if err != nil || !found {
		// If enabled is not set, module is not explicitly in any state
		return false
	}

	desiredEnabled := desiredState == Enabled
	return enabled == desiredEnabled
}

// parseOperationError parses the error from a module operation
// and returns a more user-friendly error if possible.
func parseOperationError(moduleName string, err error) error {
	errStr := err.Error()

	// Check for experimental module error
	if strings.Contains(errStr, "module is experimental") {
		matches := experimentalModuleRegexp.FindStringSubmatch(errStr)
		if len(matches) > 1 {
			return &ExperimentalModuleError{ModuleName: matches[1]}
		}
		return &ExperimentalModuleError{ModuleName: moduleName}
	}

	return err
}

func createModuleConfig(name string, state EnabledState) (*unstructured.Unstructured, error) {
	newCfg := &v1alpha1.ModuleConfigMeta{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ModuleConfig",
			APIVersion: "deckhouse.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1alpha1.ModuleConfigSpec{
			Enabled: ptr.To(state == Enabled),
		},
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(newCfg)
	return &unstructured.Unstructured{Object: content}, err
}

func patchSpec(state EnabledState) ([]byte, error) {
	patchData := map[string]interface{}{
		"spec": map[string]interface{}{
			"enabled": state == Enabled,
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal patch data: %w", err)
	}

	return patchBytes, nil
}
