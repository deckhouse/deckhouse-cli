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
	"sync"
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
	annotationKey             = "d8-migration"
	annotationKeyToRemove     = "d8-migration-"
	defaultKubectlAs          = "system:serviceaccount:d8-system:deckhouse"
	switchAccount             = "system:serviceaccount:d8-multitenancy-manager:multitenancy-manager"
	legacyFailedAttemptsFile  = "/tmp/failed_annotations.txt"
	legacyErrorLogFile        = "/tmp/failed_errors.txt"
	legacySkippedObjectsFile  = "/tmp/skipped_objects.txt"
	runTimestampFormat        = "20060102T150405Z"
)

var runStateMu sync.RWMutex

var currentRunState *sigMigrateRunState

// sigMigrateRunState stores paths and resources for one command run.
type sigMigrateRunState struct {
	RunID                 string
	FailedAttemptsFile    string
	ErrorLogFile          string
	SkippedObjectsFile    string
	TraceLogFile          string
	LegacyFailedRetryFile string
	traceFile             *os.File
}

func newSigMigrateRunState(now time.Time) *sigMigrateRunState {
	runID := now.UTC().Format(runTimestampFormat)

	return &sigMigrateRunState{
		RunID:                 runID,
		FailedAttemptsFile:    fmt.Sprintf("/tmp/failed_annotations_%s.txt", runID),
		ErrorLogFile:          fmt.Sprintf("/tmp/failed_errors_%s.txt", runID),
		SkippedObjectsFile:    fmt.Sprintf("/tmp/skipped_objects_%s.txt", runID),
		TraceLogFile:          fmt.Sprintf("/tmp/sigmigrate_trace_%s.log", runID),
		LegacyFailedRetryFile: legacyFailedAttemptsFile,
	}
}

func setCurrentRunState(state *sigMigrateRunState) {
	runStateMu.Lock()
	defer runStateMu.Unlock()
	currentRunState = state
}

func getCurrentRunState() *sigMigrateRunState {
	runStateMu.RLock()
	defer runStateMu.RUnlock()

	if currentRunState == nil {
		return &sigMigrateRunState{
			FailedAttemptsFile:    legacyFailedAttemptsFile,
			ErrorLogFile:          legacyErrorLogFile,
			SkippedObjectsFile:    legacySkippedObjectsFile,
			LegacyFailedRetryFile: legacyFailedAttemptsFile,
		}
	}

	return currentRunState
}

func getFailedAttemptsFilePath() string {
	return getCurrentRunState().FailedAttemptsFile
}

func getErrorLogFilePath() string {
	return getCurrentRunState().ErrorLogFile
}

func getSkippedObjectsFilePath() string {
	return getCurrentRunState().SkippedObjectsFile
}

func getLegacyRetryFilePath() string {
	return getCurrentRunState().LegacyFailedRetryFile
}

func tracef(format string, args ...interface{}) {
	state := getCurrentRunState()
	if state.traceFile == nil {
		return
	}

	message := fmt.Sprintf(format, args...)
	if _, err := fmt.Fprintf(state.traceFile, "%s TRACE %s\n", time.Now().UTC().Format(time.RFC3339Nano), message); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write trace log file %s: %v\n", state.TraceLogFile, err)
	}
}

func syncLegacyRetryFile() error {
	state := getCurrentRunState()
	srcFile := state.FailedAttemptsFile
	dstFile := state.LegacyFailedRetryFile
	if srcFile == "" || dstFile == "" || srcFile == dstFile {
		return nil
	}

	data, err := os.ReadFile(srcFile)
	if err != nil {
		if os.IsNotExist(err) {
			data = []byte{}
		} else {
			return fmt.Errorf("failed to read source retry file %s: %w", srcFile, err)
		}
	}

	if err := os.WriteFile(dstFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write legacy retry file %s: %w", dstFile, err)
	}

	tracef("synced retry compatibility file: %s -> %s", srcFile, dstFile)
	return nil
}

func truncateFile(path string) {
	if err := os.WriteFile(path, []byte{}, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to truncate %s: %v\n", path, err)
		tracef("failed to truncate file %s: %v", path, err)
		return
	}
	tracef("truncated file %s", path)
}

var switchAccountRetryErrorPhrases = []string{
	"denied request: failed expression: request.userInfo.username",
	"is forbidden: ValidatingAdmissionPolicy 'd8-multitenancy-manager'",
}

const requestedResourceNotFoundPhrase = "the server could not find the requested resource"

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
	Object      string
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

	config.Object, err = cmd.Flags().GetString("object")
	if err != nil {
		return fmt.Errorf("failed to get object flag: %w", err)
	}

	runState := newSigMigrateRunState(time.Now())
	traceFile, traceOpenErr := os.OpenFile(runState.TraceLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if traceOpenErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to open trace log file %s: %v\n", runState.TraceLogFile, traceOpenErr)
	} else {
		runState.traceFile = traceFile
	}
	setCurrentRunState(runState)
	defer func() {
		if runState.traceFile != nil {
			_ = runState.traceFile.Sync()
			_ = runState.traceFile.Close()
		}
		setCurrentRunState(nil)
	}()
	defer func() {
		if syncErr := syncLegacyRetryFile(); syncErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to sync legacy retry file: %v\n", syncErr)
		}
	}()

	tracef("sig-migrate started: retry=%t, object=%q, log-level=%s", config.RetryFailed, config.Object, config.LogLevel)
	tracef("run artifacts: failed=%s, errors=%s, skipped=%s, trace=%s", getFailedAttemptsFilePath(), getErrorLogFilePath(), getSkippedObjectsFilePath(), runState.TraceLogFile)
	tracef("legacy retry compatibility file: %s", getLegacyRetryFilePath())

	restConfig, _, err := utilk8s.SetupK8sClientSet(config.Kubeconfig, config.Context)
	if err != nil {
		tracef("failed to setup Kubernetes client: %v", err)
		return fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	// Setup impersonation
	restConfig.Impersonate.UserName = config.KubectlAs

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		tracef("failed to create discovery client: %v", err)
		return fmt.Errorf("failed to create discovery client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		tracef("failed to create dynamic client: %v", err)
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	var objects map[string]ObjectRef
	if config.Object != "" && !config.RetryFailed {
		objects, err = collectSingleObject(discoveryClient, dynamicClient, config.Object, config.LogLevel)
		if err != nil {
			tracef("failed to collect specified object %q: %v", config.Object, err)
			return fmt.Errorf("failed to collect specified object: %w", err)
		}
		if len(objects) == 0 {
			color.Red("Specified object not found: %s", config.Object)
			tracef("specified object not found: %q", config.Object)
			return nil
		}
		color.Cyan("\nCollected one object: %s\n", config.Object)
	} else if config.RetryFailed {
		color.Cyan("Retrying failed annotations from previous runs...\n")
		objects, err = loadFailedObjects()
		if err != nil {
			tracef("failed to load failed objects from %s: %v", getLegacyRetryFilePath(), err)
			return fmt.Errorf("failed to load failed objects: %w", err)
		}
		if len(objects) == 0 {
			color.Red("No valid objects found in retry list. Exiting.")
			tracef("retry list is empty: %s", getLegacyRetryFilePath())
			return nil
		}
		if config.Object != "" {
			objects = filterObjectsByIdentifier(objects, config.Object)
			if len(objects) == 0 {
				color.Red("Specified object not found in retry list: %s", config.Object)
				tracef("specified object not found in retry list: %q", config.Object)
				return nil
			}
		}
		color.Cyan("Loaded %d objects for retry from %s.\n", len(objects), getLegacyRetryFilePath())
	} else {
		objects, err = collectAllObjects(discoveryClient, dynamicClient, config.LogLevel)
		if err != nil {
			tracef("failed to collect objects: %v", err)
			return fmt.Errorf("failed to collect objects: %w", err)
		}
		color.Cyan("\nTotal objects collected: %d\n", len(objects))
	}

	if len(objects) == 0 {
		color.Red("No objects available for annotation. Exiting.")
		tracef("no objects available for annotation")
		return nil
	}

	// Clear run-scoped files before writing fresh run data.
	truncateFile(getFailedAttemptsFilePath())
	truncateFile(getErrorLogFilePath())
	truncateFile(getSkippedObjectsFilePath())

	// Create switch account config for retry
	switchRestConfig := rest.CopyConfig(restConfig)
	switchRestConfig.Impersonate.UserName = switchAccount
	switchDynamicClient, err := dynamic.NewForConfig(switchRestConfig)
	if err != nil {
		tracef("failed to create switch dynamic client: %v", err)
		return fmt.Errorf("failed to create switch dynamic client: %w", err)
	}

	timestamp := time.Now().Unix()
	unsupportedTypes := make(map[string]bool)

	err = annotateObjects(dynamicClient, switchDynamicClient, objects, timestamp, unsupportedTypes, config.LogLevel)
	if err != nil {
		tracef("annotateObjects failed: %v", err)
		return err
	}

	// Check if there were any failed annotations
	checkFailedAnnotations()
	tracef("sig-migrate completed")

	return nil
}

func collectAllObjects(discoveryClient discovery.DiscoveryInterface, dynamicClient dynamic.Interface, logLevel string) (map[string]ObjectRef, error) {
	objects := make(map[string]ObjectRef)

	// Get all API groups first (similar to kubectl api-resources)
	apiGroupList, err := discoveryClient.ServerGroups()
	if err != nil {
		tracef("failed to discover API groups: %v", err)
		return nil, fmt.Errorf("failed to discover API groups: %w", err)
	}

	namespacedResources := []schema.GroupVersionResource{}
	clusterResources := []schema.GroupVersionResource{}

	// Track resources by GVR to collect all unique API versions
	// This allows us to process both core API resources and custom resources (like apps.kruise.io)
	type resourceInfo struct {
		gvr        schema.GroupVersionResource
		namespaced bool
	}
	resourceMap := make(map[string]resourceInfo)

	// Iterate through all API groups and their versions (like kubectl api-resources does)
	for _, group := range apiGroupList.Groups {
		for _, version := range group.Versions {
			// Get resources for this specific group version
			apiResourceList, err := discoveryClient.ServerResourcesForGroupVersion(version.GroupVersion)
			if err != nil {
				// Log but continue - some groups may fail (e.g., metrics)
				if logLevel == "TRACE" {
					fmt.Printf("Warning: failed to get resources for %s: %v\n", version.GroupVersion, err)
				}
				tracef("failed to get resources for %s: %v", version.GroupVersion, err)
				continue
			}

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

				// Use full GVR string as key to collect all unique API versions
				// This allows us to process both core API (apps/v1/daemonsets) and custom resources (apps.kruise.io/v1alpha1/daemonsets)
				resourceKey := gvr.String()

				// Only add if we haven't seen this exact GVR before
				if _, exists := resourceMap[resourceKey]; !exists {
					resourceMap[resourceKey] = resourceInfo{gvr: gvr, namespaced: apiResource.Namespaced}
				}
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
			tracef("error listing %s: %v", gvr.String(), err)
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
			tracef("error listing %s: %v", gvr.String(), err)
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
			recordSkippedObject(obj, "MethodNotSupported", fmt.Sprintf("Resource type %s does not support PATCH operation", obj.Kind))
			continue
		}

		currentObject++
		progress := (currentObject * 100) / totalObjects
		greenProgress := color.New(color.FgGreen).SprintFunc()
		fmt.Printf("\rProgress: [%s] Annotating: Kind=%s, Namespace=%s, Name=%s                    ", greenProgress(fmt.Sprintf("%d%%", progress)), obj.Kind, obj.Namespace, obj.Name)

		if logLevel == "TRACE" {
			color.Cyan("\n[TRACE] Processing object: Kind=%s, Namespace=%s, Name=%s, GVR=%s\n", obj.Kind, obj.Namespace, obj.Name, obj.GVR.String())
		}
		tracef("processing object kind=%s namespace=%s name=%s gvr=%s", obj.Kind, obj.Namespace, obj.Name, obj.GVR.String())

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
			if shouldRetryWithSwitchAccount(errStr) {
				color.Yellow("\nRetrying with different service account: %s for %s/%s/%s\n", switchAccount, obj.Kind, obj.Namespace, obj.Name)
				tracef("retrying with switch account %s for %s/%s/%s", switchAccount, obj.Kind, obj.Namespace, obj.Name)
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
						tracef("method not supported after switch account for %s/%s/%s: %s", obj.Kind, obj.Namespace, obj.Name, formatServerErrorDetails(err))
						unsupportedTypes[obj.Kind] = true
						color.Yellow("\nAdding %s to unsupported annotation types due to MethodNotSupported (after trying switch account).\n", obj.Kind)
						recordSkippedObject(obj, "MethodNotSupported", fmt.Sprintf("After switching to account %s: %v", switchAccount, err))
						continue
					}
					color.Red("\nFailed to add annotation after switching accounts for %s/%s/%s\n", obj.Kind, obj.Namespace, obj.Name)
					color.Yellow("Retry Details: %v\n", err)
					tracef("failed to add annotation after switch account for %s/%s/%s: %s", obj.Kind, obj.Namespace, obj.Name, formatServerErrorDetails(err))
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
				tracef("method not supported for %s/%s/%s: %s", obj.Kind, obj.Namespace, obj.Name, formatServerErrorDetails(err))
				unsupportedTypes[obj.Kind] = true
				color.Yellow("\nAdding %s to unsupported annotation types due to MethodNotSupported.\n", obj.Kind)
				recordSkippedObject(obj, "MethodNotSupported", fmt.Sprintf("Error: %v", err))
				continue
			}

			// Record all other errors (excluding not-found cases - skip retry for these)
			isNotFound := errors.IsNotFound(err) || strings.Contains(strings.ToLower(errStr), "not found")

			if isNotFound {
				skipReason, skipDetails := classifyNotFoundError(err)
				color.Yellow("\nSkipping %s/%s/%s: %s\n", obj.Kind, obj.Namespace, obj.Name, skipDetails)
				tracef("skipping object %s/%s/%s: reason=%s details=%s", obj.Kind, obj.Namespace, obj.Name, skipReason, skipDetails)
				recordSkippedObject(obj, skipReason, skipDetails)
			} else {
				// Other errors - definitely record them
				color.Red("\nFailed to add annotation to %s/%s/%s\n", obj.Kind, obj.Namespace, obj.Name)
				color.Yellow("Details: %v\n", err)
				tracef("failed to add annotation for %s/%s/%s: %s", obj.Kind, obj.Namespace, obj.Name, formatServerErrorDetails(err))
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
					tracef("failed to remove annotation for %s/%s/%s: %s", obj.Kind, obj.Namespace, obj.Name, formatServerErrorDetails(err))
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
		tracef("get failed for %s: %s", name, formatServerErrorDetails(err))
		return err
	}

	if logLevel == "TRACE" {
		color.Cyan("\n[TRACE] Running annotation command: add %s=%s to %s\n", key, value, name)
		color.Cyan("[TRACE] Object UID: %s, ResourceVersion: %s\n", obj.GetUID(), obj.GetResourceVersion())
	}
	tracef("add annotation key=%s value=%s object=%s uid=%s rv=%s", key, value, name, obj.GetUID(), obj.GetResourceVersion())

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
	tracef("patch payload for %s: %s", name, string(patchBytes))

	// Try MergePatchType first
	_, err = client.Patch(context.TODO(), name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		// If MergePatchType fails with MethodNotSupported, try StrategicMergePatchType
		// This is needed for some resources like pods
		if errors.IsMethodNotSupported(err) {
			if logLevel == "TRACE" {
				color.Cyan("[TRACE] MergePatchType not supported, trying StrategicMergePatchType for %s\n", name)
			}
			tracef("merge patch type not supported for %s, retry with StrategicMergePatchType", name)
			_, err = client.Patch(context.TODO(), name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
		}
		if err != nil && logLevel == "TRACE" {
			color.Cyan("[TRACE] Patch failed for %s: %v\n", name, err)
		}
		if err != nil {
			tracef("patch failed for %s: %s", name, formatServerErrorDetails(err))
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

	retryFile := getLegacyRetryFilePath()
	data, err := os.ReadFile(retryFile)
	if err != nil {
		if os.IsNotExist(err) {
			tracef("retry file does not exist: %s", retryFile)
			return objects, nil
		}
		return nil, fmt.Errorf("failed to read failed attempts file %s: %w", retryFile, err)
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
	failedAttemptsFile := getFailedAttemptsFilePath()
	errorLogFile := getErrorLogFilePath()

	tracef("recording failure for %s/%s/%s: %s", obj.Kind, obj.Namespace, obj.Name, errorMsg)

	// Append to failed attempts file
	f, err := os.OpenFile(failedAttemptsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// If we can't write to the file, log to stderr as fallback
		fmt.Fprintf(os.Stderr, "Warning: failed to write to %s: %v\n", failedAttemptsFile, err)
		tracef("failed to append failed attempts file %s: %v", failedAttemptsFile, err)
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
		tracef("failed to append error log file %s: %v", errorLogFile, err)
		return
	}
	_, _ = fmt.Fprintf(f, "%s|%s|%s|%s\n", obj.Namespace, obj.Name, obj.Kind, errorMsg)
	_ = f.Sync()
	_ = f.Close()
}

func recordSkippedObject(obj ObjectRef, reason string, details string) {
	skippedObjectsFile := getSkippedObjectsFilePath()
	tracef("recording skipped object %s/%s/%s: reason=%s details=%s", obj.Kind, obj.Namespace, obj.Name, reason, details)

	// Append to skipped objects file
	f, err := os.OpenFile(skippedObjectsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// If we can't write to the file, log to stderr as fallback
		fmt.Fprintf(os.Stderr, "Warning: failed to write to %s: %v\n", skippedObjectsFile, err)
		tracef("failed to append skipped objects file %s: %v", skippedObjectsFile, err)
		return
	}
	timestamp := time.Now().Format(time.RFC3339)
	gvrStr := obj.GVR.String()
	_, _ = fmt.Fprintf(f, "%s|%s|%s|%s|%s|%s|%s\n", timestamp, obj.Namespace, obj.Name, obj.Kind, gvrStr, reason, details)
	_ = f.Sync()
	_ = f.Close()
}

func shouldRetryWithSwitchAccount(errMsg string) bool {
	for _, phrase := range switchAccountRetryErrorPhrases {
		if strings.Contains(errMsg, phrase) {
			return true
		}
	}

	return false
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func classifyNotFoundError(err error) (string, string) {
	serverDetails := formatServerErrorDetails(err)
	if isResourceEndpointNotFound(err) {
		tracef("classified not found error as ResourceNotServed: %s", serverDetails)
		return "ResourceNotServed", fmt.Sprintf("Resource endpoint is not found or not served by API server: %s", serverDetails)
	}

	tracef("classified not found error as NotFound: %s", serverDetails)
	return "NotFound", fmt.Sprintf("Object not found (it may have been removed): %s", serverDetails)
}

func isResourceEndpointNotFound(err error) bool {
	if err == nil {
		return false
	}

	if strings.Contains(strings.ToLower(err.Error()), requestedResourceNotFoundPhrase) {
		return true
	}

	statusErr, ok := err.(*errors.StatusError)
	if !ok {
		return false
	}

	statusMessage := strings.ToLower(statusErr.Status().Message)
	return strings.Contains(statusMessage, requestedResourceNotFoundPhrase)
}

func formatServerErrorDetails(err error) string {
	if err == nil {
		return ""
	}

	statusErr, ok := err.(*errors.StatusError)
	if !ok {
		return err.Error()
	}

	status := statusErr.Status()
	return fmt.Sprintf("%s (status: code=%d, reason=%s, message=%q)", err.Error(), status.Code, status.Reason, status.Message)
}

func parseObjectIdentifier(objectIdentifier string) (namespace string, name string, kind string, err error) {
	trimmedIdentifier := strings.TrimSpace(objectIdentifier)
	parts := strings.Split(trimmedIdentifier, "/")
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("invalid object format: expected <namespace>/<name>/<kind>")
	}

	namespace = strings.TrimSpace(parts[0])
	name = strings.TrimSpace(parts[1])
	kind = strings.TrimSpace(parts[2])
	if namespace == "" || name == "" || kind == "" {
		return "", "", "", fmt.Errorf("invalid object format: namespace, name and kind must be non-empty")
	}

	return namespace, name, kind, nil
}

func collectSingleObject(
	discoveryClient discovery.DiscoveryInterface,
	dynamicClient dynamic.Interface,
	objectIdentifier string,
	logLevel string,
) (map[string]ObjectRef, error) {
	namespace, name, kind, err := parseObjectIdentifier(objectIdentifier)
	if err != nil {
		return map[string]ObjectRef{}, nil
	}

	objects := make(map[string]ObjectRef)

	apiGroupList, err := discoveryClient.ServerGroups()
	if err != nil {
		return nil, fmt.Errorf("failed to discover API groups: %w", err)
	}

	for _, group := range apiGroupList.Groups {
		for _, version := range group.Versions {
			apiResourceList, err := discoveryClient.ServerResourcesForGroupVersion(version.GroupVersion)
			if err != nil {
				if logLevel == "TRACE" {
					fmt.Printf("Warning: failed to get resources for %s: %v\n", version.GroupVersion, err)
				}
				tracef("failed to get resources for %s while collecting single object: %v", version.GroupVersion, err)
				continue
			}

			gv, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
			if err != nil {
				continue
			}

			for _, apiResource := range apiResourceList.APIResources {
				if apiResource.Name != kind {
					continue
				}

				if strings.Contains(apiResource.Name, "/") {
					continue
				}

				if !contains(apiResource.Verbs, "get") || !contains(apiResource.Verbs, "patch") {
					continue
				}

				if namespace == "clusterwide" && apiResource.Namespaced {
					continue
				}
				if namespace != "clusterwide" && !apiResource.Namespaced {
					continue
				}

				gvr := schema.GroupVersionResource{
					Group:    gv.Group,
					Version:  gv.Version,
					Resource: apiResource.Name,
				}

				resourceClient := dynamicClient.Resource(gvr)
				var objClient dynamic.ResourceInterface
				if namespace == "clusterwide" {
					objClient = resourceClient
				} else {
					objClient = resourceClient.Namespace(namespace)
				}

				_, err = objClient.Get(context.TODO(), name, metav1.GetOptions{})
				if err != nil {
					if errors.IsNotFound(err) {
						continue
					}
					if logLevel == "TRACE" {
						fmt.Printf("Warning: failed to get object %s/%s/%s in %s: %v\n", namespace, name, kind, gvr.String(), err)
					}
					tracef("failed to get object %s/%s/%s in %s: %s", namespace, name, kind, gvr.String(), formatServerErrorDetails(err))
					continue
				}

				key := fmt.Sprintf("%s|%s|%s", namespace, name, kind)
				objects[key] = ObjectRef{
					Namespace: namespace,
					Name:      name,
					Kind:      kind,
					GVR:       gvr,
				}

				return objects, nil
			}
		}
	}

	return objects, nil
}

func filterObjectsByIdentifier(objects map[string]ObjectRef, objectIdentifier string) map[string]ObjectRef {
	namespace, name, kind, err := parseObjectIdentifier(objectIdentifier)
	if err != nil {
		return map[string]ObjectRef{}
	}

	key := fmt.Sprintf("%s|%s|%s", namespace, name, kind)

	filtered := make(map[string]ObjectRef)
	if object, exists := objects[key]; exists {
		filtered[key] = object
	}

	return filtered
}

func checkFailedAnnotations() {
	state := getCurrentRunState()
	failedAttemptsFile := state.FailedAttemptsFile
	errorLogFile := state.ErrorLogFile
	traceLogFile := state.TraceLogFile

	data, err := os.ReadFile(failedAttemptsFile)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		tracef("failed to read failed attempts file %s: %v", failedAttemptsFile, err)
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
		color.Yellow("  Failed objects list: %s\n", failedAttemptsFile)
		color.Yellow("  Trace log file: %s\n\n", traceLogFile)
		color.Red("To investigate the issues:\n")
		color.Yellow("  1. Review the trace and error log files to understand why objects failed\n")
		color.Yellow("  2. Check permissions and resource availability\n")
		color.Yellow("  3. Retry migration for failed objects only using:\n")
		color.Green("     d8 tools sig-migrate --retry\n\n")
	}
}
