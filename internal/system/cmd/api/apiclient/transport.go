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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// Namespace is where the Deckhouse controller runs.
	Namespace = "d8-system"
	// ContainerName is the controller container of the deckhouse pod.
	ContainerName = "deckhouse"

	// selfPortName names the container port of the TCP listener (ADDON_OPERATOR_LISTEN_PORT).
	selfPortName = "self"
	leaderLabels = "app=deckhouse,leader=true"
)

// LeaderPod returns the pod of the leading controller replica.
func LeaderPod(ctx context.Context, kubeCl kubernetes.Interface) (*corev1.Pod, error) {
	pods, err := kubeCl.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{LabelSelector: leaderLabels})
	if err != nil {
		return nil, fmt.Errorf("list pods %s in %s: %w", leaderLabels, Namespace, err)
	}

	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("%w (%s) in %s", ErrNoLeader, leaderLabels, Namespace)
	}

	return &pods.Items[0], nil
}

// Pod returns the named controller pod, to query a replica other than the leader.
func Pod(ctx context.Context, kubeCl kubernetes.Interface, name string) (*corev1.Pod, error) {
	pod, err := kubeCl.CoreV1().Pods(Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get pod %s/%s: %w", Namespace, name, err)
	}

	return pod, nil
}

// ProxyTransport reaches the TCP listener through the pods/proxy subresource. The
// listener binds the pod IP rather than loopback, so a port-forward, which dials
// localhost inside the pod's network namespace, cannot reach it, while the API
// server's proxy dials the pod IP.
//
// The request goes through the bare HTTP client of the REST config: rest.Request
// drops the body and status of a non-2xx answer it cannot decode, and the
// controller answers its errors in plain text.
type ProxyTransport struct {
	kubeCl     kubernetes.Interface
	httpClient *http.Client
	target     string // "<pod>:<port>"
}

// NewProxyTransport targets the "self" port of the controller container of pod.
func NewProxyTransport(config *rest.Config, kubeCl kubernetes.Interface, pod *corev1.Pod) (*ProxyTransport, error) {
	port, err := selfPort(pod)
	if err != nil {
		return nil, err
	}

	httpClient, err := rest.HTTPClientFor(config)
	if err != nil {
		return nil, fmt.Errorf("create HTTP client: %w", err)
	}

	return &ProxyTransport{
		kubeCl:     kubeCl,
		httpClient: httpClient,
		target:     pod.Name + ":" + strconv.Itoa(int(port)),
	}, nil
}

// Get sends the request through the API server. An answer of the controller is
// returned whatever its status; an error of the API server itself, such as a
// refused pods/proxy permission, comes back as an error.
func (t *ProxyTransport) Get(ctx context.Context, path string, query url.Values) (*Response, error) {
	proxyURL := t.kubeCl.CoreV1().RESTClient().Get().
		Namespace(Namespace).
		Resource("pods").
		Name(t.target).
		SubResource("proxy").
		Suffix(path).
		URL()
	proxyURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxyURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s through pods/proxy %s/%s: %w", path, Namespace, t.target, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("GET %s through pods/proxy %s/%s: read body: %w", path, Namespace, t.target, err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if status := apiStatus(body); status != nil {
			return nil, fmt.Errorf("GET %s through pods/proxy %s/%s: %w", path, Namespace, t.target, apierrors.FromObject(status))
		}
	}

	return &Response{StatusCode: resp.StatusCode, Body: body}, nil
}

// apiStatus returns body as a metav1.Status, which the API server sends for its own
// errors, or nil: the controller never answers with one.
func apiStatus(body []byte) *metav1.Status {
	status := new(metav1.Status)
	if json.Unmarshal(body, status) != nil || status.Kind != "Status" {
		return nil
	}

	return status
}

// selfPort returns the number of the controller container's "self" port.
func selfPort(pod *corev1.Pod) (int32, error) {
	for _, container := range pod.Spec.Containers {
		if container.Name != ContainerName {
			continue
		}

		for _, port := range container.Ports {
			if port.Name == selfPortName {
				return port.ContainerPort, nil
			}
		}
	}

	return 0, fmt.Errorf("pod %s/%s, container %q: %w", pod.Namespace, pod.Name, ContainerName, ErrNoSelfPort)
}
