/*
Copyright 2024 Flant JSC

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

package module_releases

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/api/v1alpha1"
	constants "github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/const"
)

// =============================================================================
// Types
// =============================================================================

// ModuleReleaseInfo contains information about a module release.
// This is a simplified DTO for use in CLI commands.
type ModuleReleaseInfo struct {
	Name       string
	ModuleName string
	Version    string
	Phase      string
	Message    string
	IsApproved bool
	IsApplyNow bool
}

// newReleaseInfo converts a typed ModuleRelease to ModuleReleaseInfo.
func newReleaseInfo(mr *v1alpha1.ModuleRelease) ModuleReleaseInfo {
	version := mr.Spec.Version
	if version == "" {
		// Try to extract version from name (format: moduleName-vX.Y.Z)
		version = extractVersionFromName(mr.Name, mr.Spec.ModuleName)
	}

	return ModuleReleaseInfo{
		Name:       mr.Name,
		ModuleName: mr.Spec.ModuleName,
		Version:    version,
		Phase:      mr.Status.Phase,
		Message:    mr.Status.Message,
		IsApproved: mr.IsApproved(),
		IsApplyNow: mr.IsApplyNow(),
	}
}

// =============================================================================
// Read Operations
// =============================================================================

// ListModuleReleases returns all module releases for a given module name.
// If moduleName is empty, returns all releases.
func ListModuleReleases(dynamicClient dynamic.Interface, moduleName string) ([]ModuleReleaseInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultAPITimeout)
	defer cancel()

	resourceClient := dynamicClient.Resource(v1alpha1.ModuleReleaseGVR)

	list, err := resourceClient.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list module releases: %w", err)
	}

	releases := make([]ModuleReleaseInfo, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]

		mr, err := v1alpha1.ModuleReleaseFromUnstructured(item)
		if err != nil {
			// Skip items that can't be converted
			continue
		}

		// Filter by module name if specified, otherwise include all
		if moduleName != "" && mr.Spec.ModuleName != moduleName {
			continue
		}

		releases = append(releases, newReleaseInfo(mr))
	}

	return releases, nil
}

// GetModuleRelease returns a specific module release by module name and version.
func GetModuleRelease(dynamicClient dynamic.Interface, moduleName, version string) (*ModuleReleaseInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultAPITimeout)
	defer cancel()

	// Normalize version to have 'v' prefix
	version = NormalizeVersion(version)

	// Build release name: moduleName-version (e.g., csi-hpe-v0.3.10)
	releaseName := fmt.Sprintf("%s-%s", moduleName, version)

	resourceClient := dynamicClient.Resource(v1alpha1.ModuleReleaseGVR)

	item, err := resourceClient.Get(ctx, releaseName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	mr, err := v1alpha1.ModuleReleaseFromUnstructured(item)
	if err != nil {
		return nil, fmt.Errorf("failed to parse module release: %w", err)
	}

	info := newReleaseInfo(mr)
	// Use the requested version if spec.version is empty
	if info.Version == "" {
		info.Version = version
	}

	return &info, nil
}

// =============================================================================
// Write Operations
// =============================================================================

// ApproveModuleRelease adds the approved annotation to a module release.
func ApproveModuleRelease(dynamicClient dynamic.Interface, releaseName string) error {
	return setAnnotation(dynamicClient, releaseName, v1alpha1.ModuleReleaseApprovedAnnotation, "true")
}

// ApplyNowModuleRelease adds the apply-now annotation to a module release.
func ApplyNowModuleRelease(dynamicClient dynamic.Interface, releaseName string) error {
	return setAnnotation(dynamicClient, releaseName, v1alpha1.ModuleReleaseApplyNowAnnotation, "true")
}

// setAnnotation sets an annotation on a module release.
func setAnnotation(dynamicClient dynamic.Interface, releaseName, annotationKey, annotationValue string) error {
	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultAPITimeout)
	defer cancel()

	resourceClient := dynamicClient.Resource(v1alpha1.ModuleReleaseGVR)

	patchData := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				annotationKey: annotationValue,
			},
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return fmt.Errorf("failed to marshal patch data: %w", err)
	}

	_, err = resourceClient.Patch(ctx, releaseName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch module release '%s': %w", releaseName, err)
	}

	return nil
}
