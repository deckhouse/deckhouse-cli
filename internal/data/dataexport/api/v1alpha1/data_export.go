package v1alpha1

import (
	_ "k8s.io/apimachinery/pkg/api/resource" // Register resource.Quantity types
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type DataExport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataexportSpec   `json:"spec"`
	Status DataExportStatus `json:"status"`
}

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type DataExportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []DataExport `json:"items"`
}

// +k8s:deepcopy-gen=true
type DataexportSpec struct {
	TTL       string        `json:"ttl"`
	Publish   bool          `json:"publish"`
	TargetRef TargetRefSpec `json:"targetRef"`
}

// +k8s:deepcopy-gen=true
type DataExportStatus struct {
	URL             string             `json:"url"`
	CA              string             `json:"ca,omitempty"`
	PublicURL       string             `json:"publicURL"`
	AccessTimestamp metav1.Time        `json:"accessTimestamp"`
	Conditions      []metav1.Condition `json:"conditions,omitempty"`
	VolumeMode      string             `json:"volumeMode,omitempty"`
}

// +k8s:deepcopy-gen=true
type TargetRefSpec struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}
