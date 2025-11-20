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

package cni

import (
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var (
	cniModuleConfigs = []string{"cni-cilium", "cni-flannel", "cni-simple-bridge"}
)

const (
	ControlPlaneNodeLabel            = "node-role.kubernetes.io/control-plane"
	MutatingWebhookConfigurationName = "effective-cni-annotator"
	WebhookServiceName               = "effective-cni-annotator-webhook-service"
	helperServiceAccountName         = "cni-switch-helper"
	helperClusterRoleName            = "cni-switch-helper-role"
	helperClusterRoleBindingName     = "cni-switch-helper-binding"
)

func getSwitchHelperServiceAccount(namespace string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      helperServiceAccountName,
			Namespace: namespace,
		},
	}
}

func getSwitchHelperClusterRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: helperClusterRoleName,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"network.deckhouse.io"},
				Resources: []string{"cnimigrations"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{"network.deckhouse.io"},
				Resources: []string{"cnimigrations/status"},
				Verbs:     []string{"get"},
			},
			{
				APIGroups: []string{"network.deckhouse.io"},
				Resources: []string{"cninodemigrations"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"network.deckhouse.io"},
				Resources: []string{"cninodemigrations/status"},
				Verbs:     []string{"get", "update", "patch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "watch", "patch", "update", "delete"},
			},
		}}
}

func getSwitchHelperClusterRoleBinding(namespace string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: helperClusterRoleBindingName,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     helperClusterRoleName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      helperServiceAccountName,
				Namespace: namespace,
			},
		},
	}
}

func getSwitchHelperDaemonSet(imageName string) *appsv1.DaemonSet {
	truePtr := true
	terminationGracePeriodSeconds := int64(5)

	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cni-switch-helper",
			Namespace: "d8-system",
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "cni-switch-helper"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "cni-switch-helper"},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: helperServiceAccountName,
					Containers: []corev1.Container{
						{
							Name:  "helper",
							Image: imageName,
							Ports: []corev1.ContainerPort{
								{
									Name:          "healthz",
									ContainerPort: 8081,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/healthz",
										Port: intstr.FromString("healthz"),
									},
								},
								InitialDelaySeconds: 15,
								PeriodSeconds:       20,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/readyz",
										Port: intstr.FromString("healthz"),
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
							},
							Env: []corev1.EnvVar{
								{
									Name: "NODE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
								{
									Name: "POD_NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: &truePtr,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "host-proc",
									MountPath: "/host/proc",
									ReadOnly:  true,
								},
								{
									Name:      "host-sys",
									MountPath: "/host/sys",
									ReadOnly:  true,
								},
								{
									Name:      "cni-net-d",
									MountPath: "/etc/cni/net.d",
								},
								{
									Name:      "cni-bin",
									MountPath: "/opt/cni/bin",
								},
							},
						},
					},
					// The helper needs to run on all nodes, including control-plane nodes
					// to clean up CNI configurations everywhere.
					Tolerations: []corev1.Toleration{
						{
							Key:      ControlPlaneNodeLabel,
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
					PriorityClassName:             "system-node-critical",
					HostNetwork:                   true,
					HostPID:                       true,
					HostIPC:                       true,
					TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
					Volumes: []corev1.Volume{
						{
							Name: "host-proc",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: "/proc"},
							},
						},
						{
							Name: "host-sys",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: "/sys"},
							},
						},
						{
							Name: "cni-net-d",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: "/etc/cni/net.d"},
							},
						},
						{
							Name: "cni-bin",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: "/opt/cni/bin"},
							},
						},
					},
				},
			},
		},
	}
}

func getMutatingWebhookConfiguration(namespace string) *admissionregistrationv1.MutatingWebhookConfiguration {
	path := "/mutate"
	// Exclude system namespaces and all known CNI module namespaces.
	excludedNamespaces := []string{}
	for _, moduleName := range cniModuleConfigs {
		excludedNamespaces = append(excludedNamespaces, "d8-"+moduleName)
	}

	return &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: MutatingWebhookConfigurationName,
		},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name: "effective-cni.network.deckhouse.io",
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Name:      WebhookServiceName,
						Namespace: "kube-system",
						Path:      &path,
					},
					CABundle: []byte{},
				},
				Rules: []admissionregistrationv1.RuleWithOperations{
					{
						Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
						Rule: admissionregistrationv1.Rule{
							APIGroups:   []string{""},
							APIVersions: []string{"v1"},
							Resources:   []string{"pods"},
						},
					},
				},
				NamespaceSelector: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "kubernetes.io/metadata.name",
							Operator: metav1.LabelSelectorOpNotIn,
							Values:   excludedNamespaces,
						},
					},
				},
				SideEffects: &[]admissionregistrationv1.
					SideEffectClass{admissionregistrationv1.SideEffectClassNone}[0],
				AdmissionReviewVersions: []string{"v1"},
				FailurePolicy: &[]admissionregistrationv1.
					FailurePolicyType{admissionregistrationv1.Fail}[0],
			},
		},
	}
}
