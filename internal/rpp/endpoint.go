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
	"fmt"
	"net"
	"net/url"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// proxyNamespace is where registry-packages-proxy and its objects live.
	proxyNamespace = "d8-cloud-instance-manager"

	// proxyPodSelector selects the registry-packages-proxy pods.
	proxyPodSelector = "app=registry-packages-proxy"

	// proxyIngressName is the Ingress the registry-packages-proxy module creates for
	// the public /v1/images route (host registry-packages-proxy.<publicDomain>).
	proxyIngressName = "registry-packages-proxy"

	// proxyPort is the kube-rbac-proxy port that fronts the proxy on every master node.
	proxyPort = 4219

	// proxyScheme is the endpoint scheme (kube-rbac-proxy serves HTTPS).
	proxyScheme = "https"
)

// errIngressUnusable marks the Ingress lookup as "the API answered, but the
// Ingress is absent or has no host" - the only case where the in-cluster pod
// fallback is worth trying. A transport/TLS/auth failure reaching the API is NOT
// this: it would fail pod listing identically, so it is surfaced as-is.
var errIngressUnusable = errors.New("registry-packages-proxy ingress unusable")

// chooseDiscoveredEndpoint resolves the proxy endpoint when none was given
// explicitly. It PREFERS the public Ingress (a valid TLS certificate, reachable
// from a workstation) and falls back to in-cluster pod IPs (which need
// --rpp-insecure-skip-tls-verify and cluster-network reachability). The second
// return value names the source ("ingress" / "pod") for logging.
//
// Only an unusable Ingress (see errIngressUnusable) triggers the pod fallback.
// Any other error is an API-leg failure, surfaced as ErrEndpointDiscovery.
func chooseDiscoveredEndpoint(ctx context.Context, kube kubernetes.Interface) (string, string, error) {
	endpoint, err := discoverIngressEndpoint(ctx, kube)
	if err == nil {
		return endpoint, "ingress", nil
	}

	if !errors.Is(err, errIngressUnusable) {
		return "", "", fmt.Errorf("%w: %w", ErrEndpointDiscovery, err)
	}

	endpoint, err = discoverEndpoint(ctx, kube)
	if err != nil {
		return "", "", fmt.Errorf("%w: %w", ErrEndpointDiscovery, err)
	}

	return endpoint, "pod", nil
}

// discoverIngressEndpoint returns the public proxy endpoint (https://<host>) taken
// from the registry-packages-proxy Ingress. This path has a valid TLS certificate
// and is reachable from outside the cluster - the right default for a workstation.
//
// An absent Ingress or one with no host yields errIngressUnusable, signalling the
// caller to try the in-cluster pod fallback. Any other error is returned raw so
// the caller can surface the API-leg failure instead of falling back.
func discoverIngressEndpoint(ctx context.Context, kube kubernetes.Interface) (string, error) {
	ingress, err := kube.NetworkingV1().Ingresses(proxyNamespace).Get(ctx, proxyIngressName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("%w: ingress %q not found", errIngressUnusable, proxyIngressName)
		}

		return "", fmt.Errorf("get registry-packages-proxy ingress: %w", err)
	}

	for _, rule := range ingress.Spec.Rules {
		if rule.Host != "" {
			return (&url.URL{Scheme: proxyScheme, Host: rule.Host}).String(), nil
		}
	}

	return "", fmt.Errorf("%w: ingress %q has no host", errIngressUnusable, proxyIngressName)
}

// discoverEndpoint returns a proxy endpoint base URL by listing the
// registry-packages-proxy pods and joining the first ready, running pod IP with the
// proxy port. Pods that are terminating or not yet ready are skipped, so callers
// do not dial draining or not-yet-serving proxies. There is no failover, so a
// single serving pod is enough.
//
// This is a master-node pod IP, reachable from inside the cluster network. A
// workstation outside the cluster usually cannot reach it and should pass an
// explicit endpoint (for example the public Ingress) instead.
func discoverEndpoint(ctx context.Context, kube kubernetes.Interface) (string, error) {
	pods, err := kube.CoreV1().Pods(proxyNamespace).List(ctx, metav1.ListOptions{LabelSelector: proxyPodSelector})
	if err != nil {
		return "", fmt.Errorf("list registry-packages-proxy pods: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if !podIsServing(pod) {
			continue
		}

		base := url.URL{Scheme: proxyScheme, Host: net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(proxyPort))}

		return base.String(), nil
	}

	return "", fmt.Errorf("no ready registry-packages-proxy pods found in namespace %q", proxyNamespace)
}

// podIsServing reports whether the pod is a usable proxy endpoint: running, not
// terminating, with an assigned IP and a Ready condition that is true.
func podIsServing(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil || pod.Status.PodIP == "" {
		return false
	}

	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}
