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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
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

func TestDiscoverPodEndpointsKeepsOnlyServingPods(t *testing.T) {
	kube := fake.NewSimpleClientset(
		proxyPod("not-ready", "10.0.0.2", corev1.PodRunning, false, false),
		proxyPod("terminating", "10.0.0.3", corev1.PodRunning, true, true),
		proxyPod("pending", "10.0.0.4", corev1.PodPending, false, false),
		proxyPod("no-ip", "", corev1.PodRunning, true, false),
		proxyPod("serving", "10.0.0.1", corev1.PodRunning, true, false),
	)

	endpoints, err := discoverPodEndpoints(context.Background(), kube)
	require.NoError(t, err)
	assert.Equal(t, []string{"https://10.0.0.1:4219"}, endpoints)
}

func TestDiscoverPodEndpointsNoneServing(t *testing.T) {
	kube := fake.NewSimpleClientset(
		proxyPod("not-ready", "10.0.0.2", corev1.PodRunning, false, false),
	)

	endpoints, err := discoverPodEndpoints(context.Background(), kube)
	require.NoError(t, err, "no serving pod is not a failure, it just yields no candidate")
	assert.Empty(t, endpoints)
}

func TestDiscoverPodEndpointsToleratesDeniedList(t *testing.T) {
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", errors.New("no access"))
	})

	endpoints, err := discoverPodEndpoints(context.Background(), kube)
	require.NoError(t, err, "an identity without pod list rights still uses the other candidates")
	assert.Empty(t, endpoints)
}

func TestDiscoverIngressEndpoint(t *testing.T) {
	kube := fake.NewSimpleClientset(proxyIngress("registry-packages-proxy.example.com"))

	endpoint, err := discoverIngressEndpoint(context.Background(), kube)
	require.NoError(t, err)
	assert.Equal(t, "https://registry-packages-proxy.example.com", endpoint)
}

func TestDiscoverIngressEndpointAbsent(t *testing.T) {
	_, err := discoverIngressEndpoint(context.Background(), fake.NewSimpleClientset())
	require.Error(t, err)
	assert.ErrorIs(t, err, errIngressUnusable, "an absent Ingress just yields no public candidate")
}

func TestDiscoverIngressEndpointNoHost(t *testing.T) {
	kube := fake.NewSimpleClientset(proxyIngress(""))

	_, err := discoverIngressEndpoint(context.Background(), kube)
	require.Error(t, err)
	assert.ErrorIs(t, err, errIngressUnusable, "an Ingress with no host just yields no public candidate")
}

func TestDiscoverCandidatesPrefersPublishedMasters(t *testing.T) {
	// The published masters come first: they need no public domain and no external
	// certificate authority. The public host and the pods follow as fallbacks.
	kube := fake.NewSimpleClientset(
		proxyConfigMap(map[string]string{
			"endpoints":      `["192.168.0.1:4219"]`,
			"ca.crt":         "CA-PEM",
			"publicEndpoint": "https://registry-packages-proxy.example.com",
		}),
		proxyPod("serving", "10.0.0.1", corev1.PodRunning, true, false),
	)

	candidates, err := discoverCandidates(context.Background(), kube)
	require.NoError(t, err)
	require.Len(t, candidates, 3)

	assert.Equal(t, "https://192.168.0.1:4219", candidates[0].endpoint)
	assert.Equal(t, sourceMaster, candidates[0].source)
	assert.Equal(t, []byte("CA-PEM"), candidates[0].caPEM)

	assert.Equal(t, "https://registry-packages-proxy.example.com", candidates[1].endpoint)
	assert.Equal(t, sourcePublic, candidates[1].source)
	assert.Empty(t, candidates[1].caPEM, "the public host is verified with the system roots")

	assert.Equal(t, "https://10.0.0.1:4219", candidates[2].endpoint)
	assert.Equal(t, sourcePod, candidates[2].source)
}

func TestDiscoverCandidatesReadsIngressWhenNothingPublished(t *testing.T) {
	// A cluster running an older module version: no published config, so the
	// public endpoint comes from the Ingress object as before.
	kube := fake.NewSimpleClientset(
		proxyIngress("registry-packages-proxy.example.com"),
		proxyPod("serving", "10.0.0.1", corev1.PodRunning, true, false),
	)

	candidates, err := discoverCandidates(context.Background(), kube)
	require.NoError(t, err)
	require.Len(t, candidates, 2)
	assert.Equal(t, sourcePublic, candidates[0].source)
	assert.Equal(t, "https://registry-packages-proxy.example.com", candidates[0].endpoint)
	assert.Equal(t, sourcePod, candidates[1].source)
}

func TestDiscoverCandidatesSkipsIngressWhenPublished(t *testing.T) {
	// With the published public endpoint the Ingress is never read, so the CLI
	// needs no permission on Ingresses.
	kube := fake.NewSimpleClientset(proxyConfigMap(map[string]string{
		"endpoints":      `["192.168.0.1:4219"]`,
		"publicEndpoint": "https://published.example.com",
	}))
	kube.PrependReactor("get", "ingresses", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("ingresses must not be read")
	})

	candidates, err := discoverCandidates(context.Background(), kube)
	require.NoError(t, err)
	require.Len(t, candidates, 2)
	assert.Equal(t, "https://published.example.com", candidates[1].endpoint)
}

func TestDiscoverCandidatesToleratesDeniedIngressRead(t *testing.T) {
	kube := fake.NewSimpleClientset(proxyPod("serving", "10.0.0.1", corev1.PodRunning, true, false))
	kube.PrependReactor("get", "ingresses", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "ingresses"}, "", errors.New("no access"))
	})

	candidates, err := discoverCandidates(context.Background(), kube)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	assert.Equal(t, sourcePod, candidates[0].source)
}

func TestDiscoverCandidatesSurfacesAPIFailure(t *testing.T) {
	// A transport or TLS failure reaching the API is surfaced instead of being
	// masked: every candidate source would fail the same way.
	kube := fake.NewSimpleClientset(proxyPod("serving", "10.0.0.1", corev1.PodRunning, true, false))
	kube.PrependReactor("get", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("tls: failed to verify certificate")
	})

	_, err := discoverCandidates(context.Background(), kube)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tls: failed to verify certificate")
}

func TestDiscoverCandidatesWithoutAnyEndpoint(t *testing.T) {
	_, err := discoverCandidates(context.Background(), fake.NewSimpleClientset())
	require.Error(t, err, "an empty cluster offers no way to reach the proxy")
}
