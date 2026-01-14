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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ModuleRelease phase constants.
// Source: https://github.com/deckhouse/deckhouse/blob/main/deckhouse-controller/pkg/apis/deckhouse.io/v1alpha1/module_release.go
const (
	ModuleReleasePhasePending    = "Pending"
	ModuleReleasePhaseDeployed   = "Deployed"
	ModuleReleasePhaseSuperseded = "Superseded"
	ModuleReleasePhaseSuspended  = "Suspended"
	ModuleReleasePhaseSkipped    = "Skipped"
)

// ModuleRelease annotation keys.
// Source: https://github.com/deckhouse/deckhouse/blob/main/deckhouse-controller/pkg/apis/deckhouse.io/v1alpha1/module_release.go
const (
	// ModuleReleaseApprovedAnnotation marks a release as approved for deployment.
	ModuleReleaseApprovedAnnotation = "modules.deckhouse.io/approved"
	// ModuleReleaseApplyNowAnnotation forces immediate deployment, bypassing update windows.
	ModuleReleaseApplyNowAnnotation = "modules.deckhouse.io/apply-now"
)

// ModuleReleaseGVR is the GroupVersionResource for ModuleRelease objects.
var ModuleReleaseGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1alpha1",
	Resource: "modulereleases",
}

// ModuleRelease represents a Deckhouse module release.
// This is a minimal version containing only fields needed for approve/apply-now commands.
// Full schema: https://github.com/deckhouse/deckhouse/blob/main/deckhouse-controller/pkg/apis/deckhouse.io/v1alpha1/module_release.go
type ModuleRelease struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModuleReleaseSpec   `json:"spec"`
	Status ModuleReleaseStatus `json:"status,omitempty"`
}

// ModuleReleaseSpec contains the specification of a module release.
type ModuleReleaseSpec struct {
	ModuleName string `json:"moduleName"`
	Version    string `json:"version,omitempty"`
	Weight     uint32 `json:"weight,omitempty"`
}

// ModuleReleaseStatus contains the status of a module release.
type ModuleReleaseStatus struct {
	Phase    string `json:"phase,omitempty"`
	Approved bool   `json:"approved"`
	Message  string `json:"message"`
}

// ModuleReleaseFromUnstructured converts an unstructured object to ModuleRelease.
func ModuleReleaseFromUnstructured(obj *unstructured.Unstructured) (*ModuleRelease, error) {
	var release ModuleRelease
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &release)
	if err != nil {
		return nil, err
	}
	return &release, nil
}

// IsApproved returns true if the release has the approved annotation set to "true".
func (mr *ModuleRelease) IsApproved() bool {
	return mr.Annotations[ModuleReleaseApprovedAnnotation] == "true"
}

// IsApplyNow returns true if the release has the apply-now annotation set to "true".
func (mr *ModuleRelease) IsApplyNow() bool {
	return mr.Annotations[ModuleReleaseApplyNowAnnotation] == "true"
}
