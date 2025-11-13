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

// CNIMigration is the schema for the CNIMigration API.
// It is a cluster-level resource that serves as the "single source of truth"
// for the entire migration process. It defines the goal (targetCNI)
// and tracks the overall progress across all nodes.
type CNIMigration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of CNIMigration.
	Spec CNIMigrationSpec `json:"spec,omitempty"`
	// Status defines the observed state of CNIMigration.
	Status CNIMigrationStatus `json:"status,omitempty"`
}

// CNIMigrationSpec defines the desired state of CNIMigration.
// +k8s:deepcopy-gen=true
type CNIMigrationSpec struct {
	// TargetCNI is the CNI to switch to.
	// Set by the d8 cli utility when starting Phase 1.
	TargetCNI string `json:"targetCNI"`
	// Phase is the phase controlled by the d8 cli to command the agents.
	// Possible values: Prepare, Migrate, Cleanup, Abort.
	Phase string `json:"phase"`
}

// CNIMigrationStatus defines the observed state of CNIMigration.
// +k8s:deepcopy-gen=true
type CNIMigrationStatus struct {
	// CurrentCNI is the detected CNI from which the switch is being made.
	CurrentCNI string `json:"currentCNI,omitempty"`
	// ObservedPhase is the current actual phase of the operation.
	ObservedPhase string `json:"observedPhase,omitempty"`
	// NodesTotal is the total number of nodes involved in the migration.
	NodesTotal int `json:"nodesTotal,omitempty"`
	// NodesSucceeded is the number of nodes that have successfully completed the current phase.
	NodesSucceeded int `json:"nodesSucceeded,omitempty"`
	// NodesFailed is the number of nodes where an error occurred.
	NodesFailed int `json:"nodesFailed,omitempty"`
	// Conditions reflect the state of the migration as a whole.
	// The d8 cli aggregates statuses from all CNINodeMigrations here.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CNIMigrationList contains a list of CNIMigration.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type CNIMigrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CNIMigration `json:"items"`
}
