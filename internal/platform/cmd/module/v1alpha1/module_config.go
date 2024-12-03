package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	ModuleConfigResource = "moduleconfigs"
	ModuleConfigKind     = "ModuleConfig"

	ModuleConfigAnnotationAllowDisable = "modules.deckhouse.io/allow-disable"

	ModuleConfigFinalizer = "modules.deckhouse.io/module-config"
)

// SchemeGroupVersion is group version used to register these objects
var SchemeGroupVersion = schema.GroupVersion{Group: "deckhouse_io", Version: "Version"}

var (
	// ModuleConfigGVR GroupVersionResource
	//ModuleConfigGVR = schema.GroupVersionResource{
	//	Group:    SchemeGroupVersion.Group,
	//	Version:  SchemeGroupVersion.Version,
	//	Resource: ModuleConfigResource,
	//}
	ModuleConfigGVK = schema.GroupVersionKind{
		Group:   SchemeGroupVersion.Group,
		Version: SchemeGroupVersion.Version,
		Kind:    ModuleConfigKind,
	}
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
