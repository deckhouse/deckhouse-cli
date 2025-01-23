package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ModuleConfig is a configuration for module or for global config values.
type ModuleConfig struct {
	metav1.TypeMeta `json:",inline"`

	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional

	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ModuleConfigSpec `json:"spec"`

	Status ModuleConfigStatus `json:"status,omitempty"`
}

type ModuleConfigSpec struct {
	Version      int            `json:"version,omitempty"`
	Settings     SettingsValues `json:"settings,omitempty"`
	Enabled      *bool          `json:"enabled,omitempty"`
	UpdatePolicy string         `json:"updatePolicy,omitempty"`
	Source       string         `json:"source,omitempty"`
}

type ModuleConfigStatus struct {
	Version string `json:"version"`
	Message string `json:"message"`
}

// SettingsValues empty interface in needed to handle DeepCopy generation. DeepCopy does not work with unnamed empty interfaces
type SettingsValues map[string]interface{}
