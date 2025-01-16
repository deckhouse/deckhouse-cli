package operatemodule

import (
	"context"
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// updates spec.enabled flag or creates a new ModuleConfig with spec.enabled flag.
func ListModule(cmd *cobra.Command) error {

	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, _, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	// CustomResource represents the CRD you're trying to fetch.
	const customResourceGroup = "deckhouse.io" // The group of your CRD
	const customResourceVersion = "v1alpha1"   // The version of your CRD
	const customResourcePlural = "modules"     // Plural name of your custom resource

	// Create a dynamic client
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("Failed to create dynamic client: %v", err)
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

	customResources, err := resourceClient.List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("Failed to list custom resources: %v", err)
	}

	// Iterate through the list of custom resources and check their status.conditions
	for _, customResource := range customResources.Items {
		// Extract the 'status.conditions' field (assumes conditions is an array)
		conditions, _, err := unstructured.NestedSlice(customResource.Object, "status", "conditions")
		if err != nil {
			return fmt.Errorf("Failed to get status.conditions for custom resource '%s': %v", customResource.GetName(), err)
		}

		// Check if any condition is of type "Ready" and status "True"
		for _, condition := range conditions {
			conditionMap, valid := condition.(map[string]interface{})
			if !valid {
				return fmt.Errorf("Not valid condition format")
			}

			conditionStatus, _ := conditionMap["status"].(string)

			// If condition type is "Ready" and status is "True"
			if conditionStatus == "True" {
				// Print the custom resource's name if it is ready
				fmt.Printf("Custom Resource '%s' is Ready\n", customResource.GetName())
				break
			}
		}
	}

	return err
}
