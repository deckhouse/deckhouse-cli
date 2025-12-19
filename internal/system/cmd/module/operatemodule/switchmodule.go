package operatemodule

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/utils/ptr"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

type ModuleState string

const (
	ModuleEnabled  ModuleState = "enabled"
	ModuleDisabled ModuleState = "disabled"
)

// OperateResultStatus represents the result of a module operation.
type OperateResultStatus int

const (
	// ResultChanged indicates the module state was changed.
	ResultChanged OperateResultStatus = iota
	// ResultAlreadyInState indicates the module is already in the desired state.
	ResultAlreadyInState
)

// OperateResult contains the result of a module operation.
type OperateResult struct {
	Status OperateResultStatus
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

// GetDynamicClient creates a dynamic Kubernetes client from cobra command flags.
// It reads "kubeconfig" and "context" flags from the command.
// Dynamic client is required to work with Custom Resources like ModuleRelease
// and ModuleConfig, which don't have typed clients in client-go.
func GetDynamicClient(cmd *cobra.Command) (dynamic.Interface, error) {
	kubeconfigPath, _ := cmd.Flags().GetString("kubeconfig")
	contextName, _ := cmd.Flags().GetString("context")

	config, _, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return nil, fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return dynamicClient, nil
}

func OperateModule(dynamicClient dynamic.Interface, name string, moduleState ModuleState) (*OperateResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultAPITimeout)
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
		obj, err := createModuleConfig(name, moduleState)
		if err != nil {
			return nil, fmt.Errorf("failed to convert the '%s' module config: %w", name, err)
		}
		if _, err = resourceClient.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
			return nil, parseModuleOperationError(name, err)
		}
		return &OperateResult{Status: ResultChanged}, nil
	}

	// Check if module is already in the desired state
	if isModuleInState(existing, moduleState) {
		return &OperateResult{Status: ResultAlreadyInState}, nil
	}

	// Resource exists, patch it
	enabledSpec, err := patchSpec(moduleState)
	if err != nil {
		return nil, err
	}
	if _, err = resourceClient.Patch(ctx, name, types.MergePatchType, enabledSpec, metav1.PatchOptions{}); err != nil {
		return nil, parseModuleOperationError(name, err)
	}
	return &OperateResult{Status: ResultChanged}, nil
}

// isModuleInState checks if the module is already in the desired state.
func isModuleInState(obj *unstructured.Unstructured, desiredState ModuleState) bool {
	spec, found, err := unstructured.NestedMap(obj.Object, "spec")
	if err != nil || !found {
		return false
	}

	enabled, found, err := unstructured.NestedBool(spec, "enabled")
	if err != nil || !found {
		// If enabled is not set, module is not explicitly in any state
		return false
	}

	desiredEnabled := desiredState == ModuleEnabled
	return enabled == desiredEnabled
}

// parseModuleOperationError parses the error from a module operation
// and returns a more user-friendly error if possible.
func parseModuleOperationError(moduleName string, err error) error {
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
		return nil, fmt.Errorf("failed to marshal patch data: %w", err)
	}

	return patchBytes, nil
}
