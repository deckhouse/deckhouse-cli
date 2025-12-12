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
	_ = os.Truncate(failedAttemptsFile, 0)
	_ = os.Truncate(errorLogFile, 0)

	// Create switch account config for retry
	switchRestConfig := rest.CopyConfig(restConfig)
	switchRestConfig.Impersonate.UserName = switchAccount
	switchDynamicClient, err := dynamic.NewForConfig(switchRestConfig)
	if err != nil {
		return fmt.Errorf("failed to create switch dynamic client: %w", err)
	}

	timestamp := time.Now().Unix()
	unsupportedTypes := make(map[string]bool)

	return annotateObjects(dynamicClient, switchDynamicClient, objects, timestamp, unsupportedTypes, config.LogLevel)
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

	for _, apiResourceList := range apiResourceLists {
		gv, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
		if err != nil {
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

			gvr := schema.GroupVersionResource{
				Group:    gv.Group,
				Version:  gv.Version,
				Resource: apiResource.Name,
			}

			if apiResource.Namespaced {
				namespacedResources = append(namespacedResources, gvr)
			} else {
				clusterResources = append(clusterResources, gvr)
			}
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
			if strings.Contains(err.Error(), "the server does not allow this method") {
				unsupportedTypes[obj.Kind] = true
				color.Yellow("\nAdding %s to unsupported annotation types due to MethodNotAllowed.\n", obj.Kind)
				continue
			}

			if strings.Contains(err.Error(), "denied request: failed expression: request.userInfo.username") {
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
					color.Red("\nFailed to add annotation after switching accounts for %s/%s/%s\n", obj.Kind, obj.Namespace, obj.Name)
					color.Yellow("Retry Details: %v\n", err)
					recordFailure(obj, err.Error())
					continue
				}
			} else if !strings.Contains(err.Error(), "Not found") && !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "the server does not allow this method") {
				color.Red("\nFailed to add annotation to %s/%s/%s\n", obj.Kind, obj.Namespace, obj.Name)
				color.Yellow("Details: %v\n", err)
				recordFailure(obj, err.Error())
				continue
			}
		}

		// Remove annotation
		err = removeAnnotation(objClient, obj.Name, annotationKeyToRemove, logLevel)
		if err != nil {
			if !strings.Contains(err.Error(), "Not found") && !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "the server does not allow this method") {
				color.Red("\nFailed to remove annotation from %s/%s/%s\n", obj.Kind, obj.Namespace, obj.Name)
				color.Yellow("Details: %v\n", err)
				recordFailure(obj, err.Error())
			}
		}
	}

	fmt.Println()
	return nil
}

func addAnnotation(client dynamic.ResourceInterface, name, key, value, logLevel string) error {
	obj, err := client.Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if logLevel == "TRACE" {
		fmt.Printf("\nRunning annotation command: add %s=%s to %s\n", key, value, name)
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

	_, err = client.Patch(context.TODO(), name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
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
	if err == nil {
		fmt.Fprintf(f, "%s|%s|%s\n", obj.Namespace, obj.Name, obj.Kind)
		f.Close()
	}

	// Append to error log file
	f, err = os.OpenFile(errorLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		fmt.Fprintf(f, "%s|%s|%s|%s\n", obj.Namespace, obj.Name, obj.Kind, errorMsg)
		f.Close()
	}
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
