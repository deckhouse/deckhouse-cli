package models

import (
	autoscaling "k8s.io/api/autoscaling/v1"
	v1 "k8s.io/api/core/v1"
)

// VerticalPodAutoscaler is the configuration for a vertical pod
// autoscaler, which automatically manages pod resources based on historical and
// real time resource utilization.
type VerticalPodAutoscaler struct {
	// Specification of the behavior of the autoscaler.
	// More info: https://git.k8s.io/community/contributors/devel/api-conventions.md#spec-and-status.
	Spec VerticalPodAutoscalerSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

// VerticalPodAutoscalerSpec is the specification of the behavior of the autoscaler.
type VerticalPodAutoscalerSpec struct {

	// TargetRef points to the controller managing the set of pods for the
	// autoscaler to control - e.g. Deployment, StatefulSet. VerticalPodAutoscaler
	// can be targeted at controller implementing scale subresource (the pod set is
	// retrieved from the controller's ScaleStatus) or some well known controllers
	// (e.g. for DaemonSet the pod set is read from the controller's spec).
	// If VerticalPodAutoscaler cannot use specified target it will report
	// ConfigUnsupported condition.
	// Note that VerticalPodAutoscaler does not require full implementation
	// of scale subresource - it will not use it to modify the replica count.
	// The only thing retrieved is a label selector matching pods grouped by
	// the target resource.
	TargetRef *autoscaling.CrossVersionObjectReference `json:"targetRef" protobuf:"bytes,1,name=targetRef"`

	// Describes the rbac-proxy on how changes are applied to the pods.
	// If not specified, all fields in the `PodUpdatePolicy` are set to their
	// default values.
	// +optional
	UpdatePolicy *PodUpdatePolicy `json:"updatePolicy,omitempty" protobuf:"bytes,2,opt,name=updatePolicy"`

	// Controls how the autoscaler computes recommended resources.
	// The resource policy may be used to set constraints on the recommendations
	// for individual containers. If not specified, the autoscaler computes recommended
	// resources for all containers in the pod, without additional constraints.
	// +optional
	ResourcePolicy *PodResourcePolicy `json:"resourcePolicy,omitempty" protobuf:"bytes,3,opt,name=resourcePolicy"`
}

// PodUpdatePolicy describes the rbac-proxy on how changes are applied to the pods.
type PodUpdatePolicy struct {
	// Controls when autoscaler applies changes to the pod resources.
	// The default is 'Auto'.
	// +optional
	UpdateMode *UpdateMode `json:"updateMode,omitempty" protobuf:"bytes,1,opt,name=updateMode"`
}

// UpdateMode controls when autoscaler applies changes to the pod resoures.
// +kubebuilder:validation:Enum=Off;Initial;Recreate;Auto
type UpdateMode string

const (
	// UpdateModeOff means that autoscaler never changes Pod resources.
	// The recommender still sets the recommended resources in the
	// VerticalPodAutoscaler object. This can be used for a "dry run".
	UpdateModeOff UpdateMode = "Off"
	// UpdateModeInitial means that autoscaler only assigns resources on pod
	// creation and does not change them during the lifetime of the pod.
	UpdateModeInitial UpdateMode = "Initial"
	// UpdateModeRecreate means that autoscaler assigns resources on pod
	// creation and additionally can update them during the lifetime of the
	// pod by deleting and recreating the pod.
	UpdateModeRecreate UpdateMode = "Recreate"
	// DEPRECATED since vpa version 1.5.1. It is now recommended to use InPlaceOrRecreate instead.
	// UpdateModeAuto means that autoscaler assigns resources on pod creation
	// and additionally can update them during the lifetime of the pod,
	// using any available update method. Currently this is equivalent to
	// Recreate, which is the only available update method.
	UpdateModeAuto UpdateMode = "Auto"
	// UpdateModeInPlaceOrReacreate means that autoscaler assigns resources on pod creation
	// if Kubernetes support is available, in-place resource updates are performed,
	// and if it is not available, a fallback to eviction is performed.
	UpdateModeInPlaceOrReacreate UpdateMode = "InPlaceOrRecreate"
)

// PodResourcePolicy controls how autoscaler computes the recommended resources
// for containers belonging to the pod. There can be at most one entry for every
// named container and optionally a single wildcard entry with `containerName` = '*',
// which handles all containers that don't have individual policies.
type PodResourcePolicy struct {
	// Per-container resource policies.
	// +optional
	// +patchMergeKey=containerName
	// +patchStrategy=merge
	ContainerPolicies []ContainerResourcePolicy `json:"containerPolicies,omitempty" patchStrategy:"merge" patchMergeKey:"containerName" protobuf:"bytes,1,rep,name=containerPolicies"`
}

// ContainerResourcePolicy controls how autoscaler computes the recommended
// resources for a specific container.
type ContainerResourcePolicy struct {
	// Name of the container or DefaultContainerResourcePolicy, in which
	// case the policy is used by the containers that don't have their own
	// policy specified.
	ContainerName string `json:"containerName,omitempty" protobuf:"bytes,1,opt,name=containerName"`
	// Whether autoscaler is enabled for the container. The default is "Auto".
	// +optional
	Mode *ContainerScalingMode `json:"mode,omitempty" protobuf:"bytes,2,opt,name=mode"`
	// Specifies the minimal amount of resources that will be recommended
	// for the container. The default is no minimum.
	// +optional
	MinAllowed v1.ResourceList `json:"minAllowed,omitempty" protobuf:"bytes,3,rep,name=minAllowed,casttype=ResourceList,castkey=ResourceName"`
	// Specifies the maximum amount of resources that will be recommended
	// for the container. The default is no maximum.
	// +optional
	MaxAllowed v1.ResourceList `json:"maxAllowed,omitempty" protobuf:"bytes,4,rep,name=maxAllowed,casttype=ResourceList,castkey=ResourceName"`

	// Specifies the type of recommendations that will be computed
	// (and possibly applied) by VPA.
	// If not specified, the default of [ResourceCPU, ResourceMemory] will be used.
	ControlledResources *[]v1.ResourceName `json:"controlledResources,omitempty" patchStrategy:"merge" protobuf:"bytes,5,rep,name=controlledResources"`

	// Specifies which resource values should be controlled.
	// The default is "RequestsAndLimits".
	// +optional
	ControlledValues *ContainerControlledValues `json:"controlledValues,omitempty" protobuf:"bytes,6,rep,name=controlledValues"`
}

const (
	// DefaultContainerResourcePolicy can be passed as
	// ContainerResourcePolicy.ContainerName to specify the default policy.
	DefaultContainerResourcePolicy = "*"
)

// ContainerScalingMode controls whether autoscaler is enabled for a specific
// container.
// +kubebuilder:validation:Enum=Auto;Off
type ContainerScalingMode string

const (
	// ContainerScalingModeAuto means autoscaling is enabled for a container.
	ContainerScalingModeAuto ContainerScalingMode = "Auto"
	// ContainerScalingModeOff means autoscaling is disabled for a container.
	ContainerScalingModeOff ContainerScalingMode = "Off"
)

// ContainerControlledValues controls which resource value should be autoscaled.
// +kubebuilder:validation:Enum=RequestsAndLimits;RequestsOnly
type ContainerControlledValues string

const (
	// ContainerControlledValuesRequestsAndLimits means resource request and limits
	// are scaled automatically. The limit is scaled proportionally to the request.
	ContainerControlledValuesRequestsAndLimits ContainerControlledValues = "RequestsAndLimits"
	// ContainerControlledValuesRequestsOnly means only requested resource is autoscaled.
	ContainerControlledValuesRequestsOnly ContainerControlledValues = "RequestsOnly"
)
