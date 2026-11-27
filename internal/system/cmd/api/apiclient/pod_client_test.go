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

package apiclient

import (
	"net"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

// protectedControllerPod is a Pod whose controller serves the protected API on its own HTTPS port.
func protectedControllerPod(ip string, port int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "deckhouse-selected", Namespace: Namespace},
		Spec: corev1.PodSpec{
			HostNetwork: true,
			Containers: []corev1.Container{{
				Name: ContainerName,
				Env:  []corev1.EnvVar{{Name: "ADDON_OPERATOR_LISTEN_ADDRESS", Value: "127.0.0.1"}},
				Ports: []corev1.ContainerPort{
					{Name: "self", ContainerPort: 4222},
					{Name: "https", ContainerPort: port},
				},
			}},
		},
		Status: corev1.PodStatus{PodIP: ip},
	}
}

// moveHTTPSPortToProxyContainer makes a proxy container next to the controller own the HTTPS port.
func moveHTTPSPortToProxyContainer(pod *corev1.Pod) {
	controller := &pod.Spec.Containers[0]
	httpsPort := controller.Ports[1]
	controller.Ports = controller.Ports[:1]
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "kube-rbac-proxy", Ports: []corev1.ContainerPort{httpsPort}})
}

func proxyCAConfigMap(ca []byte) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: Namespace, Name: "kube-rbac-proxy-ca.crt"},
		Data:       map[string]string{"ca.crt": string(ca)},
	}
}

func requireProxyCAReadOnly(t *testing.T, kubeCl *fake.Clientset) {
	t.Helper()
	actions := kubeCl.Actions()
	require.Len(t, actions, 1)
	require.Equal(t, "get", actions[0].GetVerb())
	require.Equal(t, Namespace, actions[0].GetNamespace())
	require.Equal(t, corev1.SchemeGroupVersion.WithResource("configmaps"), actions[0].GetResource())
	require.Equal(t, "kube-rbac-proxy-ca.crt", actions[0].(k8stesting.GetAction).GetName())
}

func TestNewForPodSelectsLegacyListenerWithoutReadingProxyCA(t *testing.T) {
	pod := protectedControllerPod("10.12.2.148", 4204)
	pod.Spec.Containers[0].Env[0] = corev1.EnvVar{
		Name:      "ADDON_OPERATOR_LISTEN_ADDRESS",
		ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}},
	}
	kubeCl := fake.NewClientset()

	client, err := NewForPod(t.Context(), &rest.Config{Host: "https://kubernetes.example.test"}, kubeCl, pod)
	require.NoError(t, err)
	require.Empty(t, kubeCl.Actions())
	legacy, ok := client.transport.(*ProxyTransport)
	require.True(t, ok, "a declared HTTPS port does not make the controller listener protected")
	require.Equal(t, pod.Name+":4222", legacy.target)
}

func TestNewForPodUsesProtectedPodAddress(t *testing.T) {
	ca := newProxyTestCA(t)
	for _, scenario := range []struct {
		name     string
		ip       string
		port     int32
		protocol corev1.Protocol
		change   func(*corev1.Pod)
		endpoint string
	}{
		{name: "IPv4", ip: "10.12.2.148", port: 4204, protocol: corev1.ProtocolTCP, endpoint: "https://10.12.2.148:4204"},
		{name: "IPv6 default protocol", ip: "fd00::42", port: 8443, endpoint: "https://[fd00::42]:8443"},
		{name: "port of a proxy container", ip: "10.12.2.148", port: 4204, change: moveHTTPSPortToProxyContainer, endpoint: "https://10.12.2.148:4204"},
		{name: "same port declared by two containers", ip: "10.12.2.148", port: 4204, change: func(pod *corev1.Pod) {
			pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "kube-rbac-proxy", Ports: []corev1.ContainerPort{{Name: "https", ContainerPort: 4204}}})
		}, endpoint: "https://10.12.2.148:4204"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			pod := protectedControllerPod(scenario.ip, scenario.port)
			pod.Status.HostIP = "10.12.2.99"
			pod.Spec.Containers[0].Ports[1].Protocol = scenario.protocol
			if scenario.change != nil {
				scenario.change(pod)
			}
			kubeCl := fake.NewClientset(proxyCAConfigMap(ca.pem))

			client, err := NewForPod(t.Context(), &rest.Config{}, kubeCl, pod)
			require.NoError(t, err)
			proxy, ok := client.transport.(*RBACProxyTransport)
			require.True(t, ok)
			require.Equal(t, scenario.endpoint, proxy.endpoint.String())
			requireProxyCAReadOnly(t, kubeCl)
		})
	}
}

func TestNewForPodCallsProtectedController(t *testing.T) {
	ca := newProxyTestCA(t)
	profile := []byte{0x1f, 0x8b, 0, 0xff}
	requests := make(chan *http.Request, 2)
	server := newProxyTestServer(t, ca.issue(t, true, nil), nil, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+rbacProxyTestToken {
			http.Error(w, "missing identity", http.StatusUnauthorized)
			return
		}
		requests <- r
		switch r.URL.Path {
		case "/controller/api/v1/queues/dump":
			_, _ = w.Write([]byte(`{"queues":{}}`))
		case "/controller/debug/pprof/heap":
			_, _ = w.Write(profile)
		default:
			http.NotFound(w, r)
		}
	})
	pod := controllerPodForTestServer(t, server.URL)
	kubeCl := fake.NewClientset(proxyCAConfigMap(ca.pem))
	config := &rest.Config{Host: "https://kubernetes.example.test", BearerToken: rbacProxyTestToken}
	client, err := NewForPod(t.Context(), config, kubeCl, pod)
	require.NoError(t, err)

	queues, err := client.Queues(t.Context(), "deckhouse")
	require.NoError(t, err)
	require.Empty(t, queues.Queues)
	request := <-requests
	require.Equal(t, "/controller/api/v1/queues/dump", request.URL.Path)
	require.Equal(t, "deckhouse", request.URL.Query().Get("name"))
	require.Equal(t, OutputJSON, request.URL.Query().Get("output"))

	body, err := client.Pprof(t.Context(), "heap", nil)
	require.NoError(t, err)
	require.Equal(t, profile, body)
	require.Equal(t, "/controller/debug/pprof/heap", (<-requests).URL.Path)
	requireProxyCAReadOnly(t, kubeCl)
}

func controllerPodForTestServer(t *testing.T, endpoint string) *corev1.Pod {
	t.Helper()
	parsed, err := url.Parse(endpoint)
	require.NoError(t, err)
	ip, portString, err := net.SplitHostPort(parsed.Host)
	require.NoError(t, err)
	port, err := strconv.Atoi(portString)
	require.NoError(t, err)
	return protectedControllerPod(ip, int32(port))
}

func TestNewForPodDoesNotFallbackAfterProxyFailure(t *testing.T) {
	ca := newProxyTestCA(t)
	otherCA := newProxyTestCA(t)
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := newProxyTestServer(t, ca.issue(t, true, nil), nil, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte("proxy response"))
			})
			kubeCl := fake.NewClientset(proxyCAConfigMap(ca.pem))
			client, err := NewForPod(t.Context(), &rest.Config{}, kubeCl, controllerPodForTestServer(t, server.URL))
			require.NoError(t, err)
			_, err = client.Get(t.Context(), "/debug/pprof/heap", nil)
			var statusErr *StatusError
			require.ErrorAs(t, err, &statusErr)
			require.Equal(t, status, statusErr.StatusCode)
			require.Equal(t, "proxy response", statusErr.Body)
			requireProxyCAReadOnly(t, kubeCl)
		})
	}
	t.Run("untrusted server certificate", func(t *testing.T) {
		server := newProxyTestServer(t, ca.issue(t, true, nil), nil, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		kubeCl := fake.NewClientset(proxyCAConfigMap(otherCA.pem))
		client, err := NewForPod(t.Context(), &rest.Config{}, kubeCl, controllerPodForTestServer(t, server.URL))
		require.NoError(t, err)
		_, err = client.Get(t.Context(), "/debug/pprof/heap", nil)
		require.Error(t, err)
		requireProxyCAReadOnly(t, kubeCl)
	})
}

func TestNewForPodRejectsUnsupportedPodTemplates(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		change func(*corev1.Pod)
	}{
		{name: "controller missing", change: func(pod *corev1.Pod) { pod.Spec.Containers[0].Name = "kube-rbac-proxy" }},
		{name: "listen env missing", change: func(pod *corev1.Pod) { pod.Spec.Containers[0].Env = nil }},
		{name: "unknown listen address", change: func(pod *corev1.Pod) { pod.Spec.Containers[0].Env[0].Value = "0.0.0.0" }},
		{name: "unsupported field reference", change: func(pod *corev1.Pod) {
			pod.Spec.Containers[0].Env[0] = corev1.EnvVar{
				Name:      "ADDON_OPERATOR_LISTEN_ADDRESS",
				ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"}},
			}
		}},
		{name: "bootstrap pod without the HTTPS port", change: func(pod *corev1.Pod) {
			pod.Spec.Containers[0].Ports = pod.Spec.Containers[0].Ports[:1]
		}},
		{name: "different HTTPS ports", change: func(pod *corev1.Pod) {
			pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "kube-rbac-proxy", Ports: []corev1.ContainerPort{{Name: "https", ContainerPort: 9443}}})
		}},
		{name: "address from ConfigMap", change: func(pod *corev1.Pod) {
			pod.Spec.Containers[0].Env[0] = corev1.EnvVar{
				Name: "ADDON_OPERATOR_LISTEN_ADDRESS",
				ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "listen-address"}, Key: "address",
				}},
			}
		}},
		{name: "ordinary pod network", change: func(pod *corev1.Pod) { pod.Spec.HostNetwork = false }},
		{name: "pod IP missing", change: func(pod *corev1.Pod) { pod.Status.PodIP = "" }},
		{name: "invalid pod IP", change: func(pod *corev1.Pod) { pod.Status.PodIP = "proxy.example.test" }},
		{name: "https port under another name", change: func(pod *corev1.Pod) { pod.Spec.Containers[0].Ports[1].Name = "other" }},
		{name: "zero port", change: func(pod *corev1.Pod) { pod.Spec.Containers[0].Ports[1].ContainerPort = 0 }},
		{name: "out of range port", change: func(pod *corev1.Pod) { pod.Spec.Containers[0].Ports[1].ContainerPort = 65536 }},
		{name: "UDP port", change: func(pod *corev1.Pod) { pod.Spec.Containers[0].Ports[1].Protocol = corev1.ProtocolUDP }},
		{name: "UDP port of a proxy container", change: func(pod *corev1.Pod) {
			moveHTTPSPortToProxyContainer(pod)
			pod.Spec.Containers[1].Ports[0].Protocol = corev1.ProtocolUDP
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			pod := protectedControllerPod("10.12.2.148", 4204)
			scenario.change(pod)
			kubeCl := fake.NewClientset()
			_, err := NewForPod(t.Context(), &rest.Config{}, kubeCl, pod)
			require.Error(t, err)
			require.Empty(t, kubeCl.Actions())
		})
	}
}

func TestNewForPodRequiresTrustedProxyCA(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		configMap *corev1.ConfigMap
		forbidden bool
	}{
		{name: "missing ConfigMap"},
		{name: "forbidden ConfigMap", forbidden: true},
		{name: "empty CA", configMap: proxyCAConfigMap(nil)},
		{name: "invalid CA", configMap: proxyCAConfigMap([]byte("not a certificate"))},
		{name: "missing ca.crt key", configMap: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: Namespace, Name: "kube-rbac-proxy-ca.crt"}}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			kubeCl := fake.NewClientset()
			if scenario.configMap != nil {
				require.NoError(t, kubeCl.Tracker().Add(scenario.configMap))
			}
			if scenario.forbidden {
				kubeCl.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "kube-rbac-proxy-ca.crt", nil)
				})
			}
			_, err := NewForPod(t.Context(), &rest.Config{}, kubeCl, protectedControllerPod("10.12.2.148", 4204))
			require.Error(t, err)
			if scenario.forbidden {
				require.True(t, apierrors.IsForbidden(err))
			}
			requireProxyCAReadOnly(t, kubeCl)
		})
	}
}
