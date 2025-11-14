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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	cniModuleConfigs = []string{"cni-cilium", "cni-flannel", "cni-simple-bridge"}
)

const (
	SwitchHelperDaemonSetName        = "cni-switch-helper"
	ControlPlaneNodeLabel            = "node-role.kubernetes.io/control-plane"
	MutatingWebhookConfigurationName = "cni-switch-webhook"
	WebhookServiceName               = "cni-switch-helper-webhook-service"
)

func getSwitchHelperDaemonSet() *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SwitchHelperDaemonSetName,
			Namespace: "d8-system",
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": SwitchHelperDaemonSetName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": SwitchHelperDaemonSetName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "helper",
							Image: "alpine:latest", // FIXME: Placeholder image
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
					PriorityClassName: "system-node-critical",
					HostNetwork:       true,
					HostPID:           true,
				},
			},
		},
	}
}

func getMutatingWebhookConfiguration() *admissionregistrationv1.MutatingWebhookConfiguration {
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
				Name: "effective-cni.deckhouse.io",
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
