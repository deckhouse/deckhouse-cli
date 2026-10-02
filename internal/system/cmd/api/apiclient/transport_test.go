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
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func deckhousePod(name string, labels map[string]string, ports ...corev1.ContainerPort) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: Namespace, Labels: labels},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "kube-rbac-proxy", Ports: []corev1.ContainerPort{{Name: "https", ContainerPort: 4204}}},
			{Name: ContainerName, Ports: ports},
		}},
	}
}

func TestLeaderPodAndProxyTarget(t *testing.T) {
	selfPort := []corev1.ContainerPort{{Name: "self", ContainerPort: 4222}, {Name: "webhook", ContainerPort: 4223}}
	kubeCl := fake.NewClientset(
		deckhousePod("deckhouse-standby", map[string]string{"app": "deckhouse"}, selfPort...),
		deckhousePod("deckhouse-leader", map[string]string{"app": "deckhouse", "leader": "true"}, selfPort...),
	)

	pod, err := LeaderPod(context.Background(), kubeCl)
	require.NoError(t, err)
	require.Equal(t, "deckhouse-leader", pod.Name)

	proxy, err := NewProxyTransport(&rest.Config{Host: "https://api.example.com"}, kubeCl, pod)
	require.NoError(t, err)
	require.Equal(t, "deckhouse-leader:4222", proxy.target)

	standby, err := Pod(context.Background(), kubeCl, "deckhouse-standby")
	require.NoError(t, err)
	require.Equal(t, "deckhouse-standby", standby.Name)

	_, err = NewProxyTransport(&rest.Config{}, kubeCl, deckhousePod("old", nil))
	require.ErrorIs(t, err, ErrNoSelfPort)
	require.EqualError(t, err, `pod d8-system/old, container "deckhouse": no "self" port`)

	_, err = LeaderPod(context.Background(), fake.NewClientset())
	require.ErrorIs(t, err, ErrNoLeader)
	require.EqualError(t, err, "no Deckhouse leader pod (app=deckhouse,leader=true) in d8-system")
}

func TestProxyTransportGoesThroughPodsProxy(t *testing.T) {
	var gotPath, gotQuery string

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery

		switch r.URL.Path {
		case "/api/v1/namespaces/d8-system/pods/deckhouse-0:4222/proxy/api/v1/queues/dump":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{\"queues\":{}}\n"))
		case "/api/v1/namespaces/d8-system/pods/deckhouse-0:4222/proxy/api/v1/packages/dump":
			// The controller answers with chi's plain 404: the route is socket-only.
			http.Error(w, "404 page not found", http.StatusNotFound)
		default:
			// The API server's own refusal is a metav1.Status.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","message":"pods \"deckhouse-0\" is forbidden: User \"u\" cannot get resource \"pods/proxy\"","reason":"Forbidden","code":403}`))
		}
	}))
	defer apiServer.Close()

	config := &rest.Config{Host: apiServer.URL}

	kubeCl, err := kubernetes.NewForConfig(config)
	require.NoError(t, err)

	proxy, err := NewProxyTransport(config, kubeCl, deckhousePod("deckhouse-0", nil, corev1.ContainerPort{Name: "self", ContainerPort: 4222}))
	require.NoError(t, err)

	resp, err := proxy.Get(context.Background(), "/api/v1/queues/dump", url.Values{"output": {"json"}, "name": {"global"}})
	require.NoError(t, err)
	require.Equal(t, &Response{StatusCode: http.StatusOK, Body: []byte("{\"queues\":{}}\n")}, resp)
	require.Equal(t, "/api/v1/namespaces/d8-system/pods/deckhouse-0:4222/proxy/api/v1/queues/dump", gotPath)
	require.Equal(t, url.Values{"output": {"json"}, "name": {"global"}}, mustParseQuery(t, gotQuery))

	resp, err = proxy.Get(context.Background(), "/api/v1/packages/dump", nil)
	require.NoError(t, err, "an answer of the controller is returned, whatever its status")
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Equal(t, "404 page not found\n", string(resp.Body))

	_, err = proxy.Get(context.Background(), "/metrics", nil)
	require.ErrorContains(t, err, `cannot get resource "pods/proxy"`)
}

func mustParseQuery(t *testing.T, raw string) url.Values {
	t.Helper()

	query, err := url.ParseQuery(raw)
	require.NoError(t, err)

	return query
}
