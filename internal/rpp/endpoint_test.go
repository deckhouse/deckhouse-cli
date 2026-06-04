/*
Copyright 2026 Flant JSC

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

package rpp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func proxyIngress(host string) *networkingv1.Ingress {
	return &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: proxyIngressName, Namespace: proxyNamespace},
		Spec:       networkingv1.IngressSpec{Rules: []networkingv1.IngressRule{{Host: host}}},
	}
}

func proxyPod(name, podIP string, phase corev1.PodPhase, ready bool, terminating bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: proxyNamespace,
			Labels:    map[string]string{"app": "registry-packages-proxy"},
		},
		Status: corev1.PodStatus{
			Phase: phase,
			PodIP: podIP,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: conditionStatus(ready)},
			},
		},
	}

	if terminating {
		pod.DeletionTimestamp = &metav1.Time{}
	}

	return pod
}

func conditionStatus(ready bool) corev1.ConditionStatus {
	if ready {
		return corev1.ConditionTrue
	}

	return corev1.ConditionFalse
}

func TestDiscoverEndpoints(t *testing.T) {
	kube := fake.NewSimpleClientset(
		proxyPod("serving", "10.0.0.1", corev1.PodRunning, true, false),
		proxyPod("not-ready", "10.0.0.2", corev1.PodRunning, false, false),
		proxyPod("terminating", "10.0.0.3", corev1.PodRunning, true, true),
		proxyPod("pending", "10.0.0.4", corev1.PodPending, false, false),
		proxyPod("no-ip", "", corev1.PodRunning, true, false),
	)

	endpoints, err := DiscoverEndpoints(context.Background(), kube)
	require.NoError(t, err)
	assert.Equal(t, []string{"https://10.0.0.1:4219"}, endpoints)
}

func TestDiscoverEndpointsNoneServing(t *testing.T) {
	kube := fake.NewSimpleClientset(
		proxyPod("not-ready", "10.0.0.2", corev1.PodRunning, false, false),
	)

	_, err := DiscoverEndpoints(context.Background(), kube)
	require.Error(t, err)
}

func TestDiscoverIngressEndpoint(t *testing.T) {
	kube := fake.NewSimpleClientset(proxyIngress("registry-packages-proxy.example.com"))

	endpoint, err := DiscoverIngressEndpoint(context.Background(), kube)
	require.NoError(t, err)
	assert.Equal(t, "https://registry-packages-proxy.example.com", endpoint)
}

func TestDiscoverIngressEndpointAbsent(t *testing.T) {
	_, err := DiscoverIngressEndpoint(context.Background(), fake.NewSimpleClientset())
	require.Error(t, err)
}

func TestChooseDiscoveredEndpointPrefersIngress(t *testing.T) {
	kube := fake.NewSimpleClientset(
		proxyIngress("registry-packages-proxy.example.com"),
		proxyPod("serving", "10.0.0.1", corev1.PodRunning, true, false),
	)

	endpoint, source, err := chooseDiscoveredEndpoint(context.Background(), kube)
	require.NoError(t, err)
	assert.Equal(t, "https://registry-packages-proxy.example.com", endpoint)
	assert.Equal(t, "ingress", source)
}

func TestChooseDiscoveredEndpointFallsBackToPods(t *testing.T) {
	// No Ingress -> fall back to in-cluster pod IPs.
	kube := fake.NewSimpleClientset(
		proxyPod("serving", "10.0.0.1", corev1.PodRunning, true, false),
	)

	endpoint, source, err := chooseDiscoveredEndpoint(context.Background(), kube)
	require.NoError(t, err)
	assert.Equal(t, "https://10.0.0.1:4219", endpoint)
	assert.Equal(t, "pod", source)
}
