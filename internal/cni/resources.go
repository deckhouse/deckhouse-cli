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

// --- Helper Resources ---

// getSwitchHelperDaemonSet returns a DaemonSet object for the cni-switch-helper.
func getSwitchHelperDaemonSet(namespace, imageName string) *appsv1.DaemonSet {
	rootID := int64(0)
	truePtr := true
	terminationGracePeriodSeconds := int64(5)
	mountPropagationBidirectional := corev1.MountPropagationBidirectional
	hostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	hostPathDirectory := corev1.HostPathDirectory

	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cni-switch-helper",
			Namespace: namespace,
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
					ServiceAccountName: switchHelperServiceAccountName,
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
									Name:  "KUBERNETES_SERVICE_HOST",
									Value: "127.0.0.1",
								},
								{
									Name:  "KUBERNETES_SERVICE_PORT",
									Value: "6445",
								},
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged:  &truePtr,
								RunAsUser:   &rootID,
								RunAsGroup:  &rootID,
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
								{
									Name:      "host-run",
									MountPath: "/run",
								},
								{
									Name:             "host-bpf",
									MountPath:        "/sys/fs/bpf",
									MountPropagation: &mountPropagationBidirectional,
								},
								{
									Name:      "host-lib-modules",
									MountPath: "/lib/modules",
									ReadOnly:  true,
								},
								{
									Name:      "host-var-lib-cni",
									MountPath: "/var/lib/cni",
								},
							},
						},
					},
					Tolerations: []corev1.Toleration{
						{
							Operator: corev1.TolerationOpExists,
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
						{
							Name: "host-run",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: "/run"},
							},
						},
						{
							Name: "host-bpf",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/sys/fs/bpf",
									Type: &hostPathDirectoryOrCreate,
								},
							},
						},
						{
							Name: "host-lib-modules",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/lib/modules",
									Type: &hostPathDirectory,
								},
							},
						},
						{
							Name: "host-var-lib-cni",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/var/lib/cni",
									Type: &hostPathDirectoryOrCreate,
								},
							},
						},
					},
				},
			},
		},
	}
}

// --- Webhook Resources ---

// getWebhookTLSSecret returns a Secret object containing the webhook's TLS certificates.
func getWebhookTLSSecret(namespace string, cert, key []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      webhookSecretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       cert,
			corev1.TLSPrivateKeyKey: key,
		},
	}
}

// getWebhookDeployment returns a Deployment object for the webhook server.
func getWebhookDeployment(namespace, imageName, serviceAccountName string) *appsv1.Deployment {
	replicas := int32(2)
	terminationGracePeriodSeconds := int64(5)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      webhookDeploymentName,
			Namespace: namespace,
			Labels: map[string]string{
				"app": webhookDeploymentName,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": webhookDeploymentName,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": webhookDeploymentName,
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName:            serviceAccountName,
					HostNetwork:                   true,
					DNSPolicy:                     corev1.DNSClusterFirstWithHostNet,
					TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{
											"app": webhookDeploymentName,
										},
									},
									TopologyKey: "kubernetes.io/hostname",
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "webhook",
							Image: imageName,
							Args: []string{
								"--mode=webhook",
								"--health-probe-bind-address=:8082",
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: webhookPort,
									Name:          "webhook",
								},
								{
									Name:          "healthz",
									ContainerPort: 8082,
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
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "tls-certs",
									MountPath: "/etc/tls",
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "tls-certs",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: webhookSecretName,
								},
							},
						},
					},
				},
			},
		},
	}
}

// getWebhookService returns a Service object for the webhook.
func getWebhookService(namespace string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      webhookServiceName,
			Namespace: namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": webhookDeploymentName,
			},
			Ports: []corev1.ServicePort{
				{
					Protocol:   corev1.ProtocolTCP,
					Port:       443,
					TargetPort: intstr.FromInt(webhookPort),
				},
			},
		},
	}
}

// getMutatingWebhookConfiguration returns a MutatingWebhookConfiguration object for annotating pods.
func getMutatingWebhookConfiguration(namespace string, caBundle []byte) *admissionregistrationv1.MutatingWebhookConfiguration {
	path := "/mutate-pod"
	failurePolicy := admissionregistrationv1.Ignore
	sideEffects := admissionregistrationv1.SideEffectClassNone

	return &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: webhookConfigName,
		},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name:                    "annotator.cni-switch.deckhouse.io",
				AdmissionReviewVersions: []string{"v1"},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Name:      webhookServiceName,
						Namespace: namespace,
						Path:      &path,
						Port:      &[]int32{443}[0],
					},
					CABundle: caBundle,
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
							Values:   generateExcludedNamespaces(namespace),
						},
					},
				},
				FailurePolicy: &failurePolicy,
				SideEffects:   &sideEffects,
			},
		},
	}
}

// generateExcludedNamespaces creates a list of namespaces to be excluded from webhook processing.
// This includes the webhook's own namespace and CNI module namespaces.
func generateExcludedNamespaces(currentNamespace string) []string {
	excluded := []string{currentNamespace} // Exclude the webhook's own namespace (e.g., "cni-switch")
	for _, module := range CNIModuleConfigs {
		excluded = append(excluded, "d8-"+module) // Exclude "d8-cni-cilium", "d8-cni-flannel", etc.
	}
	return excluded
}

// --- RBAC Resources ---

func getSwitchHelperServiceAccount(namespace string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      switchHelperServiceAccountName,
			Namespace: namespace,
		},
	}
}

func getWebhookServiceAccount(namespace string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      webhookServiceAccountName,
			Namespace: namespace,
		},
	}
}

func getSwitchHelperClusterRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: "d8:cni-switch-helper",
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
		},
	}
}

func getWebhookClusterRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: "d8:cni-switch-webhook",
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"network.deckhouse.io"},
				Resources: []string{"cnimigrations"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}
}

func getSwitchHelperClusterRoleBinding(namespace string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "d8:cni-switch-helper",
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "d8:cni-switch-helper",
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      switchHelperServiceAccountName,
				Namespace: namespace,
			},
		},
	}
}

func getWebhookClusterRoleBinding(namespace string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "d8:cni-switch-webhook",
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "d8:cni-switch-webhook",
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      webhookServiceAccountName,
				Namespace: namespace,
			},
		},
	}
}
