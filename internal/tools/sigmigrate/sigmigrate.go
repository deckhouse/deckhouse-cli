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

package sigmigrate

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const (
	annotationKey         = "d8-migration"
	annotationKeyToRemove = "d8-migration-"
	defaultKubectlAs      = "system:serviceaccount:d8-system:deckhouse"
	switchAccount         = "system:serviceaccount:d8-multitenancy-manager:multitenancy-manager"
	failedAttemptsFile    = "/tmp/failed_annotations.txt"
	errorLogFile          = "/tmp/failed_errors.txt"
)

type ObjectRef struct {
	Namespace string
	Name      string
	Kind      string
	GVR       schema.GroupVersionResource
}

type SigMigrateConfig struct {
	RetryFailed bool
	KubectlAs   string
	LogLevel    string
	Kubeconfig  string
	Context     string
}

func SigMigrate(cmd *cobra.Command, _ []string) error {
	config := &SigMigrateConfig{}

	var err error
	config.RetryFailed, err = cmd.Flags().GetBool("retry")
	if err != nil {
		return fmt.Errorf("failed to get retry flag: %w", err)
	}

	config.KubectlAs, err = cmd.Flags().GetString("as")
	if err != nil {
		return fmt.Errorf("failed to get as flag: %w", err)
	}

	config.LogLevel, err = cmd.Flags().GetString("log-level")
	if err != nil {
		return fmt.Errorf("failed to get log-level flag: %w", err)
	}

	config.Kubeconfig, err = cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("failed to get kubeconfig flag: %w", err)
	}

	config.Context, err = cmd.Flags().GetString("context")
	if err != nil {
		return fmt.Errorf("failed to get context flag: %w", err)
	}

	restConfig, _, err := utilk8s.SetupK8sClientSet(config.Kubeconfig, config.Context)
	if err != nil {
		return fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	// Setup impersonation
	restConfig.Impersonate.UserName = config.KubectlAs

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create discovery client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	var objects map[string]ObjectRef
	if config.RetryFailed {
		color.Cyan("Retrying failed annotations from previous runs...\n")
		objects, err = loadFailedObjects()
		if err != nil {
			return fmt.Errorf("failed to load failed objects: %w", err)
		}
		if len(objects) == 0 {
			color.Red("No valid objects found in retry list. Exiting.")
			return nil
		}
		color.Cyan("Loaded %d objects for retry from %s.\n", len(objects), failedAttemptsFile)
	} else {
		objects, err = collectAllObjects(discoveryClient, dynamicClient, config.LogLevel)
		if err != nil {
			return fmt.Errorf("failed to collect objects: %w", err)
		}
		color.Cyan("\nTotal objects collected: %d\n", len(objects))
	}

	if len(objects) == 0 {
		color.Red("No objects available for annotation. Exiting.")
		return nil
	}

	// Clear failed attempts files
	_ = os.WriteFile(failedAttemptsFile, []byte{}, 0644)
	_ = os.WriteFile(errorLogFile, []byte{}, 0644)

	// Create switch account config for retry
	switchRestConfig := rest.CopyConfig(restConfig)
	switchRestConfig.Impersonate.UserName = switchAccount
	switchDynamicClient, err := dynamic.NewForConfig(switchRestConfig)
	if err != nil {
		return fmt.Errorf("failed to create switch dynamic client: %w", err)
	}

	timestamp := time.Now().Unix()
	unsupportedTypes := make(map[string]bool)

	err = annotateObjects(dynamicClient, switchDynamicClient, objects, timestamp, unsupportedTypes, config.LogLevel)
	if err != nil {
		return err
	}

	// Check if there were any failed annotations
	checkFailedAnnotations()

	return nil
}

func collectAllObjects(discoveryClient discovery.DiscoveryInterface, dynamicClient dynamic.Interface, logLevel string) (map[string]ObjectRef, error) {
	objects := make(map[string]ObjectRef)

	// Get all API resources
	apiResourceLists, err := discoveryClient.ServerPreferredResources()
	if err != nil {
		// Ignore group discovery errors
		if !discovery.IsGroupDiscoveryFailedError(err) {
			return nil, fmt.Errorf("failed to discover API resources: %w", err)
		}
	}

	namespacedResources := []schema.GroupVersionResource{}
	clusterResources := []schema.GroupVersionResource{}

	// Track resources by name to prefer core API versions over metrics/extensions
	type resourceInfo struct {
		gvr        schema.GroupVersionResource
		namespaced bool
	}
	resourceMap := make(map[string]resourceInfo)

	for _, apiResourceList := range apiResourceLists {
		gv, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
		if err != nil {
			continue
		}

		// Skip metrics and other API groups that don't support standard operations
		// These groups typically only support read operations
		if gv.Group == "metrics.k8s.io" || gv.Group == "custom.metrics.k8s.io" || gv.Group == "external.metrics.k8s.io" {
			continue
		}

		for _, apiResource := range apiResourceList.APIResources {
			// Skip subresources
			if strings.Contains(apiResource.Name, "/") {
				continue
			}

			// Skip resources that don't support list
			if !contains(apiResource.Verbs, "list") {
				continue
			}

			// Skip resources that don't support patch (needed for annotations)
			if !contains(apiResource.Verbs, "patch") {
				continue
			}

			gvr := schema.GroupVersionResource{
				Group:    gv.Group,
				Version:  gv.Version,
				Resource: apiResource.Name,
			}

			// Use resource name as key (e.g., "pods")
			resourceKey := apiResource.Name

			// Prefer core API versions (empty group or v1) over extensions
			// If we already have this resource, prefer the one with empty group or v1
			if existingInfo, exists := resourceMap[resourceKey]; exists {
				// Prefer core API (empty group) over extensions
				if gv.Group == "" && existingInfo.gvr.Group != "" {
					resourceMap[resourceKey] = resourceInfo{gvr: gvr, namespaced: apiResource.Namespaced}
				} else if gv.Group == "" && existingInfo.gvr.Group == "" {
					// Both are core, prefer v1 over other versions
					if gv.Version == "v1" && existingInfo.gvr.Version != "v1" {
						resourceMap[resourceKey] = resourceInfo{gvr: gvr, namespaced: apiResource.Namespaced}
					}
				}
				// Otherwise keep the existing one
			} else {
				resourceMap[resourceKey] = resourceInfo{gvr: gvr, namespaced: apiResource.Namespaced}
			}
		}
	}

	// Convert map to slices
	for _, info := range resourceMap {
		if info.namespaced {
			namespacedResources = append(namespacedResources, info.gvr)
		} else {
			clusterResources = append(clusterResources, info.gvr)
		}
	}

	totalResources := len(namespacedResources) + len(clusterResources)
	currentResource := 0

	// Process namespaced resources
	for _, gvr := range namespacedResources {
		currentResource++
		progress := (currentResource * 100) / totalResources
		if logLevel == "TRACE" {
			fmt.Printf("\nFetching resource: %s\n", gvr.String())
		} else {
			greenProgress := color.New(color.FgGreen).SprintFunc()
			fmt.Printf("\rCalculating: [%s] Processing Namespaced Resource: %s                                ", greenProgress(fmt.Sprintf("%d%%", progress)), gvr.Resource)
		}

		resourceClient := dynamicClient.Resource(gvr)
		list, err := resourceClient.List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			if logLevel == "TRACE" {
				fmt.Printf("Error listing %s: %v\n", gvr.String(), err)
			}
			continue
		}

		for _, item := range list.Items {
			namespace := item.GetNamespace()
			if namespace == "" {
				namespace = "clusterwide"
			}
			name := item.GetName()
			key := fmt.Sprintf("%s|%s|%s", namespace, name, gvr.Resource)
			objects[key] = ObjectRef{
				Namespace: namespace,
				Name:      name,
				Kind:      gvr.Resource,
				GVR:       gvr,
			}
		}
	}

	// Process cluster resources
	for _, gvr := range clusterResources {
		currentResource++
		progress := (currentResource * 100) / totalResources
		if logLevel == "TRACE" {
			fmt.Printf("\nFetching resource: %s\n", gvr.String())
		} else {
			greenProgress := color.New(color.FgGreen).SprintFunc()
			fmt.Printf("\rCalculating: [%s] Processing Cluster Resource: %s                                 ", greenProgress(fmt.Sprintf("%d%%", progress)), gvr.Resource)
		}

		resourceClient := dynamicClient.Resource(gvr)
		list, err := resourceClient.List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			if logLevel == "TRACE" {
				fmt.Printf("Error listing %s: %v\n", gvr.String(), err)
			}
			continue
		}

		for _, item := range list.Items {
			name := item.GetName()
			key := fmt.Sprintf("clusterwide|%s|%s", name, gvr.Resource)
			objects[key] = ObjectRef{
				Namespace: "clusterwide",
				Name:      name,
				Kind:      gvr.Resource,
				GVR:       gvr,
			}
		}
	}

	fmt.Println()
	return objects, nil
}

func annotateObjects(
	dynamicClient dynamic.Interface,
	switchDynamicClient dynamic.Interface,
	objects map[string]ObjectRef,
	timestamp int64,
	unsupportedTypes map[string]bool,
	logLevel string,
) error {
	currentObject := 0
	totalObjects := len(objects)

	for _, obj := range objects {
		var err error
		if unsupportedTypes[obj.Kind] {
			if logLevel == "DEBUG" || logLevel == "TRACE" {
				color.Yellow("\nSkipping type that does not support annotation: %s\n", obj.Kind)
			}
			continue
		}

		currentObject++
		progress := (currentObject * 100) / totalObjects
		greenProgress := color.New(color.FgGreen).SprintFunc()
		fmt.Printf("\rProgress: [%s] Annotating: Kind=%s, Namespace=%s, Name=%s                    ", greenProgress(fmt.Sprintf("%d%%", progress)), obj.Kind, obj.Namespace, obj.Name)

		if logLevel == "TRACE" {
			color.Cyan("\n[TRACE] Processing object: Kind=%s, Namespace=%s, Name=%s, GVR=%s\n", obj.Kind, obj.Namespace, obj.Name, obj.GVR.String())
		}

		resourceClient := dynamicClient.Resource(obj.GVR)
		var objClient dynamic.ResourceInterface
		if obj.Namespace == "clusterwide" {
			objClient = resourceClient
		} else {
			objClient = resourceClient.Namespace(obj.Namespace)
		}

		// Add annotation
		err = addAnnotation(objClient, obj.Name, annotationKey, fmt.Sprintf("%d", timestamp), logLevel)
		if err != nil {
			errStr := err.Error()

			// First, check for permission denied - try with different service account
			// This should be checked BEFORE MethodNotSupported, as permission errors
			// can sometimes be reported as "method not allowed"
			if strings.Contains(errStr, "denied request: failed expression: request.userInfo.username") {
				color.Yellow("\nRetrying with different service account: %s for %s/%s/%s\n", switchAccount, obj.Kind, obj.Namespace, obj.Name)
				switchResourceClient := switchDynamicClient.Resource(obj.GVR)
				var switchObjClient dynamic.ResourceInterface
				if obj.Namespace == "clusterwide" {
					switchObjClient = switchResourceClient
				} else {
					switchObjClient = switchResourceClient.Namespace(obj.Namespace)
				}

				err = addAnnotation(switchObjClient, obj.Name, annotationKey, fmt.Sprintf("%d", timestamp), logLevel)
				if err != nil {
					// If it still fails with MethodNotSupported after switching accounts, then it's truly unsupported
					if errors.IsMethodNotSupported(err) {
						if logLevel == "TRACE" {
							color.Cyan("\n[TRACE] MethodNotSupported error after switching account: %v\n", err)
						}
						unsupportedTypes[obj.Kind] = true
						color.Yellow("\nAdding %s to unsupported annotation types due to MethodNotSupported (after trying switch account).\n", obj.Kind)
						continue
					}
					color.Red("\nFailed to add annotation after switching accounts for %s/%s/%s\n", obj.Kind, obj.Namespace, obj.Name)
					color.Yellow("Retry Details: %v\n", err)
					recordFailure(obj, err.Error())
					continue
				}
				// Success with switch account, continue to next object
				continue
			}

			// Check for unsupported method (resource type doesn't support annotations)
			// Only mark as unsupported if it's truly MethodNotSupported AND not a permission issue
			if errors.IsMethodNotSupported(err) {
				if logLevel == "TRACE" {
					color.Cyan("\n[TRACE] MethodNotSupported error: %v\n", err)
					color.Cyan("[TRACE] Error details for %s/%s/%s: %s\n", obj.Kind, obj.Namespace, obj.Name, errStr)
					// Check if it's a StatusError to get more details
					if statusErr, ok := err.(*errors.StatusError); ok {
						color.Cyan("[TRACE] Status code: %d\n", statusErr.Status().Code)
						color.Cyan("[TRACE] Status reason: %s\n", statusErr.Status().Reason)
						color.Cyan("[TRACE] Status message: %s\n", statusErr.Status().Message)
					}
				}
				unsupportedTypes[obj.Kind] = true
				color.Yellow("\nAdding %s to unsupported annotation types due to MethodNotSupported.\n", obj.Kind)
				continue
			}

			// Record all other errors (excluding "Not found" - object was deleted, no need to retry)
			isNotFound := errors.IsNotFound(err) || strings.Contains(errStr, "Not found") || strings.Contains(errStr, "not found")

			if isNotFound {
				// Object not found - might have been deleted, skip recording as there's no point in retrying
				if logLevel == "DEBUG" || logLevel == "TRACE" {
					color.Yellow("\nObject not found (may have been deleted): %s/%s/%s - skipping\n", obj.Kind, obj.Namespace, obj.Name)
				}
				// Don't record to failed_annotations.txt - object doesn't exist anymore
			} else {
				// Other errors - definitely record them
				color.Red("\nFailed to add annotation to %s/%s/%s\n", obj.Kind, obj.Namespace, obj.Name)
				color.Yellow("Details: %v\n", err)
				recordFailure(obj, errStr)
			}
			continue
		}

		// Remove annotation
		err = removeAnnotation(objClient, obj.Name, annotationKeyToRemove, logLevel)
		if err != nil {
			// Skip MethodNotSupported and NotFound errors - no need to record them
			// MethodNotSupported: resource type doesn't support PATCH (already known from addAnnotation)
			// NotFound: object was deleted, no point in retrying
			if !errors.IsMethodNotSupported(err) && !errors.IsNotFound(err) {
				errStr := err.Error()
				if !strings.Contains(errStr, "Not found") && !strings.Contains(errStr, "not found") {
					color.Red("\nFailed to remove annotation from %s/%s/%s\n", obj.Kind, obj.Namespace, obj.Name)
					color.Yellow("Details: %v\n", err)
					recordFailure(obj, errStr)
				}
			}
		}
	}

	fmt.Println()
	return nil
}

func addAnnotation(client dynamic.ResourceInterface, name, key, value, logLevel string) error {
	obj, err := client.Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		if logLevel == "TRACE" {
			color.Cyan("\n[TRACE] Get failed for %s: %v\n", name, err)
		}
		return err
	}

	if logLevel == "TRACE" {
		color.Cyan("\n[TRACE] Running annotation command: add %s=%s to %s\n", key, value, name)
		color.Cyan("[TRACE] Object UID: %s, ResourceVersion: %s\n", obj.GetUID(), obj.GetResourceVersion())
	}

	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[key] = value
	obj.SetAnnotations(annotations)

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotations,
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	if logLevel == "TRACE" {
		color.Cyan("[TRACE] Patch payload: %s\n", string(patchBytes))
		color.Cyan("[TRACE] Calling Patch with MergePatchType for %s\n", name)
	}

	// Try MergePatchType first
	_, err = client.Patch(context.TODO(), name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		// If MergePatchType fails with MethodNotSupported, try StrategicMergePatchType
		// This is needed for some resources like pods
		if errors.IsMethodNotSupported(err) {
			if logLevel == "TRACE" {
				color.Cyan("[TRACE] MergePatchType not supported, trying StrategicMergePatchType for %s\n", name)
			}
			_, err = client.Patch(context.TODO(), name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
		}
		if err != nil && logLevel == "TRACE" {
			color.Cyan("[TRACE] Patch failed for %s: %v\n", name, err)
		}
	}
	return err
}

func removeAnnotation(client dynamic.ResourceInterface, name, keyPrefix, _ string) error {
	obj, err := client.Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	annotations := obj.GetAnnotations()
	if annotations == nil {
		return nil
	}

	// Remove all annotations that start with keyPrefix
	modified := false
	for key := range annotations {
		if strings.HasPrefix(key, keyPrefix) {
			delete(annotations, key)
			modified = true
		}
	}

	if !modified {
		return nil
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotations,
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	_, err = client.Patch(context.TODO(), name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}

func loadFailedObjects() (map[string]ObjectRef, error) {
	objects := make(map[string]ObjectRef)

	data, err := os.ReadFile(failedAttemptsFile)
	if err != nil {
		if os.IsNotExist(err) {
			return objects, nil
		}
		return nil, fmt.Errorf("failed to read failed attempts file: %w", err)
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 3 {
			continue
		}

		namespace := strings.TrimSpace(parts[0])
		name := strings.TrimSpace(parts[1])
		kind := strings.TrimSpace(parts[2])

		if namespace == "" || name == "" || kind == "" {
			continue
		}

		key := fmt.Sprintf("%s|%s|%s", namespace, name, kind)
		// For retry, we need to reconstruct GVR - use a simple approach
		// In production, you might want to store GVR in the file
		resource := strings.ToLower(kind)
		if !strings.HasSuffix(resource, "s") {
			resource += "s"
		}
		objects[key] = ObjectRef{
			Namespace: namespace,
			Name:      name,
			Kind:      kind,
			GVR: schema.GroupVersionResource{
				Resource: resource,
			},
		}
	}

	return objects, nil
}

func recordFailure(obj ObjectRef, errorMsg string) {
	// Append to failed attempts file
	f, err := os.OpenFile(failedAttemptsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// If we can't write to the file, log to stderr as fallback
		fmt.Fprintf(os.Stderr, "Warning: failed to write to %s: %v\n", failedAttemptsFile, err)
		return
	}
	_, _ = fmt.Fprintf(f, "%s|%s|%s\n", obj.Namespace, obj.Name, obj.Kind)
	_ = f.Sync()
	_ = f.Close()

	// Append to error log file
	f, err = os.OpenFile(errorLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// If we can't write to the file, log to stderr as fallback
		fmt.Fprintf(os.Stderr, "Warning: failed to write to %s: %v\n", errorLogFile, err)
		return
	}
	_, _ = fmt.Fprintf(f, "%s|%s|%s|%s\n", obj.Namespace, obj.Name, obj.Kind, errorMsg)
	_ = f.Sync()
	_ = f.Close()
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func checkFailedAnnotations() {
	data, err := os.ReadFile(failedAttemptsFile)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		// If we can't read the file, just return silently
		return
	}

	// Check if file is not empty (after trimming whitespace)
	content := strings.TrimSpace(string(data))
	if content == "" {
		return
	}

	// Count failed objects
	lines := strings.Split(content, "\n")
	failedCount := 0
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			failedCount++
		}
	}

	if failedCount > 0 {
		color.Red("\n⚠️  Migration completed with %d failed object(s).\n\n", failedCount)
		color.Red("Some objects could not be annotated. Please check the error details:\n")
		color.Yellow("  Error log file: %s\n", errorLogFile)
		color.Yellow("  Failed objects list: %s\n\n", failedAttemptsFile)
		color.Red("To investigate the issues:\n")
		color.Yellow("  1. Review the error log file to understand why objects failed\n")
		color.Yellow("  2. Check permissions and resource availability\n")
		color.Yellow("  3. Retry migration for failed objects only using:\n")
		color.Green("     d8 tools sig-migrate --retry\n\n")
	}
}
