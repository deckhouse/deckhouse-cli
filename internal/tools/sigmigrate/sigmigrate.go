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
	stderrors "errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const (
	annotationKey            = "d8-migration"
	annotationKeyToRemove    = "d8-migration-"
	switchAccount            = "system:serviceaccount:d8-multitenancy-manager:multitenancy-manager"
	legacyFailedAttemptsFile = "/tmp/failed_annotations.txt"
	legacyErrorLogFile       = "/tmp/failed_errors.txt"
	legacySkippedObjectsFile = "/tmp/skipped_objects.txt"
	runTimestampFormat       = "20060102T150405Z"
	maxWorkerCount           = 256
	maxRequestRetries        = 5
	requestTimeout           = 30 * time.Second
	baseRetryDelay           = 200 * time.Millisecond
	defaultWorkerCount       = 10
)

var runStateMu sync.RWMutex
var fileWriteMu sync.Mutex
var traceWriteMu sync.Mutex

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
	traceWriteMu.Lock()
	defer traceWriteMu.Unlock()
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

func preferredVersionByGroup(apiGroupList *metav1.APIGroupList) map[string]string {
	preferred := make(map[string]string)
	if apiGroupList == nil {
		return preferred
	}

	for _, group := range apiGroupList.Groups {
		if group.Name == "" {
			continue
		}

		if group.PreferredVersion.Version != "" {
			preferred[group.Name] = group.PreferredVersion.Version
		}
	}

	return preferred
}

func objectCollectionKey(namespace, name string, gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("%s|%s|%s|%s", namespace, name, gvr.Group, gvr.Resource)
}

func upsertCollectedObject(
	objects map[string]ObjectRef,
	namespace string,
	name string,
	gvr schema.GroupVersionResource,
	preferredByGroup map[string]string,
) {
	key := objectCollectionKey(namespace, name, gvr)
	candidate := ObjectRef{
		Namespace: namespace,
		Name:      name,
		Kind:      gvr.Resource,
		GVR:       gvr,
	}

	existing, exists := objects[key]
	if !exists {
		objects[key] = candidate
		return
	}

	preferredVersion := preferredByGroup[gvr.Group]
	if preferredVersion == "" {
		// Keep first discovered version if preferred version is unknown.
		return
	}

	if existing.GVR.Version != preferredVersion && gvr.Version == preferredVersion {
		objects[key] = candidate
	}
}

type SigMigrateConfig struct {
	RetryFailed bool
	KubectlAs   string
	LogLevel    string
	Kubeconfig  string
	Context     string
	Object      string
	Workers     int
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

	config.Workers, err = cmd.Flags().GetInt("threads")
	if err != nil {
		return fmt.Errorf("failed to get threads flag: %w", err)
	}
	config.Workers = normalizeWorkerCount(config.Workers)

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
			traceWriteMu.Lock()
			_ = runState.traceFile.Sync()
			_ = runState.traceFile.Close()
			traceWriteMu.Unlock()
		}

		setCurrentRunState(nil)
	}()
	defer func() {
		if syncErr := syncLegacyRetryFile(); syncErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to sync legacy retry file: %v\n", syncErr)
		}
	}()

	tracef("sig-migrate started: retry=%t, object=%q, log-level=%s, threads=%d", config.RetryFailed, config.Object, config.LogLevel, config.Workers)
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

	switch {
	case config.Object != "" && !config.RetryFailed:
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
	case config.RetryFailed:
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
	default:
		objects, err = collectAllObjects(discoveryClient, dynamicClient, config.LogLevel, config.Workers)
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

	annotateObjects(dynamicClient, switchDynamicClient, objects, timestamp, unsupportedTypes, config.LogLevel, config.Workers)

	// Check if there were any failed annotations
	checkFailedAnnotations()
	tracef("sig-migrate completed")

	return nil
}

type resourceInfo struct {
	gvr        schema.GroupVersionResource
	namespaced bool
}

func collectAllObjects(discoveryClient discovery.DiscoveryInterface, dynamicClient dynamic.Interface, logLevel string, workers int) (map[string]ObjectRef, error) {
	objects := make(map[string]ObjectRef)

	apiGroupList, err := discoveryClient.ServerGroups()
	if err != nil {
		tracef("failed to discover API groups: %v", err)
		return nil, fmt.Errorf("failed to discover API groups: %w", err)
	}

	preferredByGroup := preferredVersionByGroup(apiGroupList)

	resourceMap := make(map[string]resourceInfo)
	for _, group := range apiGroupList.Groups {
		for _, version := range group.Versions {
			apiResourceList, err := discoveryClient.ServerResourcesForGroupVersion(version.GroupVersion)
			if err != nil {
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

			if gv.Group == "metrics.k8s.io" || gv.Group == "custom.metrics.k8s.io" || gv.Group == "external.metrics.k8s.io" {
				continue
			}

			for _, apiResource := range apiResourceList.APIResources {
				if strings.Contains(apiResource.Name, "/") {
					continue
				}
				if !contains(apiResource.Verbs, "list") || !contains(apiResource.Verbs, "patch") {
					continue
				}

				gvr := schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: apiResource.Name}
				resourceKey := gvr.String()
				if _, exists := resourceMap[resourceKey]; !exists {
					resourceMap[resourceKey] = resourceInfo{gvr: gvr, namespaced: apiResource.Namespaced}
				}
			}
		}
	}

	resources := make([]resourceInfo, 0, len(resourceMap))
	for _, info := range resourceMap {
		resources = append(resources, info)
	}

	if len(resources) == 0 {
		fmt.Println()
		return objects, nil
	}

	workerCount := normalizeWorkerCount(workers)
	if workerCount > len(resources) {
		workerCount = len(resources)
	}

	jobs := make(chan resourceInfo)
	var wg sync.WaitGroup
	var progressMu sync.Mutex
	var objectsMu sync.Mutex
	var processed int64
	totalResources := int64(len(resources))

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for info := range jobs {
				if logLevel == "TRACE" {
					fmt.Printf("\nFetching resource: %s\n", info.gvr.String())
				}

				list, err := withRetryResult("list "+info.gvr.String(), logLevel, func(ctx context.Context) (*unstructured.UnstructuredList, error) {
					return dynamicClient.Resource(info.gvr).List(ctx, metav1.ListOptions{})
				})
				if err != nil {
					if logLevel == "TRACE" {
						fmt.Printf("Error listing %s: %v\n", info.gvr.String(), err)
					}
					tracef("error listing %s: %v", info.gvr.String(), err)
					current := atomic.AddInt64(&processed, 1)
					if logLevel != "TRACE" {
						progress := int((current * 100) / totalResources)
						greenProgress := color.New(color.FgGreen).SprintFunc()
						progressMu.Lock()
						fmt.Printf("\rCalculating: [%s] Processed Resource: %s                                ", greenProgress(fmt.Sprintf("%d%%", progress)), info.gvr.Resource)
						progressMu.Unlock()
					}
					continue
				}

				objectsMu.Lock()
				for _, item := range list.Items {
					namespace := item.GetNamespace()
					if namespace == "" {
						namespace = "clusterwide"
					}
					name := item.GetName()
					upsertCollectedObject(objects, namespace, name, info.gvr, preferredByGroup)
				}
				objectsMu.Unlock()

				current := atomic.AddInt64(&processed, 1)
				if logLevel != "TRACE" {
					progress := int((current * 100) / totalResources)
					greenProgress := color.New(color.FgGreen).SprintFunc()
					progressMu.Lock()
					fmt.Printf("\rCalculating: [%s] Processed Resource: %s                                ", greenProgress(fmt.Sprintf("%d%%", progress)), info.gvr.Resource)
					progressMu.Unlock()
				}
			}
		}()
	}

	for _, info := range resources {
		jobs <- info
	}
	close(jobs)
	wg.Wait()

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
	workers int,
) {
	totalObjects := len(objects)
	if totalObjects == 0 {
		fmt.Println()
		return
	}

	items := make([]ObjectRef, 0, totalObjects)
	for _, obj := range objects {
		items = append(items, obj)
	}

	workerCount := normalizeWorkerCount(workers)
	if workerCount > len(items) {
		workerCount = len(items)
	}

	jobs := make(chan ObjectRef)
	var wg sync.WaitGroup
	var progressMu sync.Mutex
	var unsupportedMu sync.RWMutex
	var processed int64
	total := int64(len(items))

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for obj := range jobs {
				unsupportedMu.RLock()
				skipKind := unsupportedTypes[obj.Kind]
				unsupportedMu.RUnlock()
				if skipKind {
					if logLevel == "DEBUG" || logLevel == "TRACE" {
						color.Yellow("\nSkipping type that does not support annotation: %s\n", obj.Kind)
					}
					recordSkippedObject(obj, "MethodNotSupported", fmt.Sprintf("Resource type %s does not support PATCH operation", obj.Kind))
					current := atomic.AddInt64(&processed, 1)
					if logLevel != "TRACE" {
						progressMu.Lock()
						printAnnotationProgress(current, total, obj)
						progressMu.Unlock()
					}
					continue
				}

				if logLevel == "TRACE" {
					color.Cyan("\n[TRACE] Processing object: Kind=%s, Namespace=%s, Name=%s, GVR=%s\n", obj.Kind, obj.Namespace, obj.Name, obj.GVR.String())
				}
				tracef("processing object kind=%s namespace=%s name=%s gvr=%s", obj.Kind, obj.Namespace, obj.Name, obj.GVR.String())

				processObjectAnnotation(dynamicClient, switchDynamicClient, obj, timestamp, unsupportedTypes, &unsupportedMu, logLevel)

				current := atomic.AddInt64(&processed, 1)
				if logLevel != "TRACE" {
					progressMu.Lock()
					printAnnotationProgress(current, total, obj)
					progressMu.Unlock()
				}
			}
		}()
	}

	for _, obj := range items {
		jobs <- obj
	}
	close(jobs)
	wg.Wait()

	fmt.Println()
}

func normalizeWorkerCount(workers int) int {
	if workers <= 0 {
		return defaultWorkerCount
	}
	if workers > maxWorkerCount {
		return maxWorkerCount
	}
	return workers
}

func withRetry(operation, logLevel string, fn func(ctx context.Context) error) error {
	var lastErr error
	for attempt := 0; attempt < maxRequestRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		err := fn(ctx)
		cancel()
		if err == nil {
			return nil
		}

		lastErr = err
		if !shouldRetryRequestError(err) || attempt == maxRequestRetries-1 {
			return err
		}

		delay := baseRetryDelay * time.Duration(1<<attempt)
		if logLevel == "TRACE" || logLevel == "DEBUG" {
			tracef("retrying operation=%s attempt=%d/%d delay=%s err=%s", operation, attempt+1, maxRequestRetries, delay, formatServerErrorDetails(err))
		}
		time.Sleep(delay)
	}

	return lastErr
}

func withRetryResult[T any](operation, logLevel string, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	var lastErr error
	for attempt := 0; attempt < maxRequestRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		result, err := fn(ctx)
		cancel()
		if err == nil {
			return result, nil
		}

		lastErr = err
		if !shouldRetryRequestError(err) || attempt == maxRequestRetries-1 {
			return zero, err
		}

		delay := baseRetryDelay * time.Duration(1<<attempt)
		if logLevel == "TRACE" || logLevel == "DEBUG" {
			tracef("retrying operation=%s attempt=%d/%d delay=%s err=%s", operation, attempt+1, maxRequestRetries, delay, formatServerErrorDetails(err))
		}
		time.Sleep(delay)
	}

	return zero, lastErr
}

func shouldRetryRequestError(err error) bool {
	if err == nil {
		return false
	}

	if errors.IsTooManyRequests(err) || errors.IsServerTimeout(err) || errors.IsTimeout(err) || errors.IsServiceUnavailable(err) {
		return true
	}

	if statusErr, ok := err.(*errors.StatusError); ok {
		code := statusErr.Status().Code
		if code == 429 || code == 408 || code >= 500 {
			return true
		}
	}

	if os.IsTimeout(err) {
		return true
	}

	var netErr net.Error
	if stderrors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
		type temporary interface{ Temporary() bool }
		if te, ok := any(netErr).(temporary); ok && te.Temporary() {
			return true
		}
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "context deadline exceeded") || strings.Contains(msg, "connection reset by peer") || strings.Contains(msg, "too many requests") || strings.Contains(msg, "eof") {
		return true
	}

	return false
}

func getObjectClient(client dynamic.Interface, obj ObjectRef) dynamic.ResourceInterface {
	resourceClient := client.Resource(obj.GVR)
	if obj.Namespace == "clusterwide" {
		return resourceClient
	}

	return resourceClient.Namespace(obj.Namespace)
}

func printAnnotationProgress(current, total int64, obj ObjectRef) {
	if total <= 0 {
		return
	}
	progress := int((current * 100) / total)
	greenProgress := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("\rProgress: [%s] Annotating: Kind=%s, Namespace=%s, Name=%s                    ", greenProgress(fmt.Sprintf("%d%%", progress)), obj.Kind, obj.Namespace, obj.Name)
}

func processObjectAnnotation(
	dynamicClient dynamic.Interface,
	switchDynamicClient dynamic.Interface,
	obj ObjectRef,
	timestamp int64,
	unsupportedTypes map[string]bool,
	unsupportedMu *sync.RWMutex,
	logLevel string,
) {
	objClient := getObjectClient(dynamicClient, obj)
	err := addAnnotation(objClient, obj.Name, annotationKey, fmt.Sprintf("%d", timestamp), logLevel)
	if err != nil {
		errStr := err.Error()

		if shouldRetryWithSwitchAccount(errStr) {
			color.Yellow("\nRetrying with different service account: %s for %s/%s/%s\n", switchAccount, obj.Kind, obj.Namespace, obj.Name)
			tracef("retrying with switch account %s for %s/%s/%s", switchAccount, obj.Kind, obj.Namespace, obj.Name)
			switchObjClient := getObjectClient(switchDynamicClient, obj)

			err = addAnnotation(switchObjClient, obj.Name, annotationKey, fmt.Sprintf("%d", timestamp), logLevel)
			if err != nil {
				if errors.IsMethodNotSupported(err) {
					if logLevel == "TRACE" {
						color.Cyan("\n[TRACE] MethodNotSupported error after switching account: %v\n", err)
					}
					tracef("method not supported after switch account for %s/%s/%s: %s", obj.Kind, obj.Namespace, obj.Name, formatServerErrorDetails(err))
					unsupportedMu.Lock()
					unsupportedTypes[obj.Kind] = true
					unsupportedMu.Unlock()
					color.Yellow("\nAdding %s to unsupported annotation types due to MethodNotSupported (after trying switch account).\n", obj.Kind)
					recordSkippedObject(obj, "MethodNotSupported", fmt.Sprintf("After switching to account %s: %v", switchAccount, err))
					return
				}
				color.Red("\nFailed to add annotation after switching accounts for %s/%s/%s\n", obj.Kind, obj.Namespace, obj.Name)
				color.Yellow("Retry Details: %v\n", err)
				tracef("failed to add annotation after switch account for %s/%s/%s: %s", obj.Kind, obj.Namespace, obj.Name, formatServerErrorDetails(err))
				recordFailure(obj, err.Error())
				return
			}
			return
		}

		if errors.IsMethodNotSupported(err) {
			if logLevel == "TRACE" {
				color.Cyan("\n[TRACE] MethodNotSupported error: %v\n", err)
				color.Cyan("[TRACE] Error details for %s/%s/%s: %s\n", obj.Kind, obj.Namespace, obj.Name, errStr)
				if statusErr, ok := err.(*errors.StatusError); ok {
					color.Cyan("[TRACE] Status code: %d\n", statusErr.Status().Code)
					color.Cyan("[TRACE] Status reason: %s\n", statusErr.Status().Reason)
					color.Cyan("[TRACE] Status message: %s\n", statusErr.Status().Message)
				}
			}
			tracef("method not supported for %s/%s/%s: %s", obj.Kind, obj.Namespace, obj.Name, formatServerErrorDetails(err))
			unsupportedMu.Lock()
			unsupportedTypes[obj.Kind] = true
			unsupportedMu.Unlock()
			color.Yellow("\nAdding %s to unsupported annotation types due to MethodNotSupported.\n", obj.Kind)
			recordSkippedObject(obj, "MethodNotSupported", fmt.Sprintf("Error: %v", err))
			return
		}

		isNotFound := errors.IsNotFound(err) || strings.Contains(strings.ToLower(errStr), "not found")
		if isNotFound {
			skipReason, skipDetails := classifyNotFoundError(err)
			color.Yellow("\nSkipping %s/%s/%s: %s\n", obj.Kind, obj.Namespace, obj.Name, skipDetails)
			tracef("skipping object %s/%s/%s: reason=%s details=%s", obj.Kind, obj.Namespace, obj.Name, skipReason, skipDetails)
			recordSkippedObject(obj, skipReason, skipDetails)
			return
		}

		color.Red("\nFailed to add annotation to %s/%s/%s\n", obj.Kind, obj.Namespace, obj.Name)
		color.Yellow("Details: %v\n", err)
		tracef("failed to add annotation for %s/%s/%s: %s", obj.Kind, obj.Namespace, obj.Name, formatServerErrorDetails(err))
		recordFailure(obj, errStr)
		return
	}

	err = removeAnnotation(objClient, obj.Name, annotationKeyToRemove, logLevel)
	if err != nil {
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

func addAnnotation(client dynamic.ResourceInterface, name, key, value, logLevel string) error {
	obj, err := withRetryResult("get "+name+" for add annotation", logLevel, func(ctx context.Context) (*unstructured.Unstructured, error) {
		return client.Get(ctx, name, metav1.GetOptions{})
	})
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
	err = withRetry("patch merge "+name, logLevel, func(ctx context.Context) error {
		_, patchErr := client.Patch(ctx, name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
		return patchErr
	})
	if err != nil {
		// If MergePatchType fails with MethodNotSupported, try StrategicMergePatchType
		// This is needed for some resources like pods
		if errors.IsMethodNotSupported(err) {
			if logLevel == "TRACE" {
				color.Cyan("[TRACE] MergePatchType not supported, trying StrategicMergePatchType for %s\n", name)
			}

			tracef("merge patch type not supported for %s, retry with StrategicMergePatchType", name)
			err = withRetry("patch strategic "+name, logLevel, func(ctx context.Context) error {
				_, patchErr := client.Patch(ctx, name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
				return patchErr
			})
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

func removeAnnotation(client dynamic.ResourceInterface, name, keyPrefix, logLevel string) error {
	obj, err := withRetryResult("get "+name+" for remove annotation", logLevel, func(ctx context.Context) (*unstructured.Unstructured, error) {
		return client.Get(ctx, name, metav1.GetOptions{})
	})
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

	err = withRetry("patch remove annotation "+name, logLevel, func(ctx context.Context) error {
		_, patchErr := client.Patch(ctx, name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
		return patchErr
	})
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
		if len(parts) < 3 {
			continue
		}

		namespace := strings.TrimSpace(parts[0])
		name := strings.TrimSpace(parts[1])
		kind := strings.TrimSpace(parts[2])

		if namespace == "" || name == "" || kind == "" {
			continue
		}

		gvr := schema.GroupVersionResource{Resource: strings.ToLower(kind)}
		if len(parts) >= 5 {
			gvr.Group = strings.TrimSpace(parts[3])
			gvr.Version = strings.TrimSpace(parts[4])
		}

		key := objectCollectionKey(namespace, name, gvr)
		objects[key] = ObjectRef{
			Namespace: namespace,
			Name:      name,
			Kind:      kind,
			GVR:       gvr,
		}
	}

	return objects, nil
}

func recordFailure(obj ObjectRef, errorMsg string) {
	failedAttemptsFile := getFailedAttemptsFilePath()
	errorLogFile := getErrorLogFilePath()

	tracef("recording failure for %s/%s/%s: %s", obj.Kind, obj.Namespace, obj.Name, errorMsg)

	fileWriteMu.Lock()
	defer fileWriteMu.Unlock()

	// Append to failed attempts file
	f, err := os.OpenFile(failedAttemptsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// If we can't write to the file, log to stderr as fallback
		fmt.Fprintf(os.Stderr, "Warning: failed to write to %s: %v\n", failedAttemptsFile, err)
		tracef("failed to append failed attempts file %s: %v", failedAttemptsFile, err)

		return
	}

	_, _ = fmt.Fprintf(f, "%s|%s|%s|%s|%s\n", obj.Namespace, obj.Name, obj.Kind, obj.GVR.Group, obj.GVR.Version)
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

	fileWriteMu.Lock()
	defer fileWriteMu.Unlock()

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

func parseObjectIdentifier(objectIdentifier string) (string, string, string, error) {
	trimmedIdentifier := strings.TrimSpace(objectIdentifier)

	parts := strings.Split(trimmedIdentifier, "/")
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("invalid object format: expected <namespace>/<name>/<kind>")
	}

	namespace := strings.TrimSpace(parts[0])
	name := strings.TrimSpace(parts[1])

	kind := strings.TrimSpace(parts[2])
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

	filtered := make(map[string]ObjectRef)

	for key, object := range objects {
		if object.Namespace == namespace && object.Name == name && object.Kind == kind {
			filtered[key] = object
		}
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
