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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true

// CNINodeMigration is the schema for the CNINodeMigration API.
// This resource is created for each node in the cluster. The `cni-switch-helper`
// agent running on the node updates this resource to report its local progress.
// The d8 cli reads these resources to display detailed status.
type CNINodeMigration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec can be empty, as all configuration is taken from the parent CNIMigration resource.
	Spec CNINodeMigrationSpec `json:"spec,omitempty"`
	// Status defines the observed state of CNINodeMigration.
	Status CNINodeMigrationStatus `json:"status,omitempty"`
}

// CNINodeMigrationSpec defines the desired state of CNINodeMigration.
// +k8s:deepcopy-gen=true
type CNINodeMigrationSpec struct {
	// The spec can be empty, as all configuration is taken from the parent CNIMigration resource.
}

// CNINodeMigrationStatus defines the observed state of CNINodeMigration.
// +k8s:deepcopy-gen=true
type CNINodeMigrationStatus struct {
	// Phase is the phase of this particular node.
	Phase string `json:"phase,omitempty"`
	// PodsToRestartCount is the number of pods on the node that were marked for restart.
	PodsToRestartCount int `json:"podsToRestartCount,omitempty"`
	// Conditions are the detailed conditions reflecting the steps performed on the node.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CNINodeMigrationList contains a list of CNINodeMigration.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CNINodeMigrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CNINodeMigration `json:"items"`
}
