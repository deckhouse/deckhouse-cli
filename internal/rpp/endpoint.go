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

// Candidate sources, as reported in logs and in the failure message.
const (
	sourceMaster = "master"
	sourcePublic = "public"
	sourcePod    = "pod"
)

// errIngressUnusable marks the Ingress lookup as "the API answered, but the
// Ingress is absent or has no host". The public candidate is then simply absent.
var errIngressUnusable = errors.New("registry-packages-proxy ingress unusable")

// candidate is one way to reach the proxy: an endpoint plus the trust anchor that
// verifies it. An empty caPEM means "verify against the system roots".
type candidate struct {
	endpoint string
	source   string
	caPEM    []byte
}

// discoverCandidates returns the ways to reach the proxy, best first:
//
//  1. master addresses published by the module, verifiable with the published CA:
//     they need no public domain, no DNS record and no external certificate authority
//  2. the public endpoint, verifiable with the system roots: the only way in for a
//     client with no network path to the master nodes
//  3. pod IPs, kept for clusters that publish no configuration yet; their
//     certificate carries no verifiable CA, so this one needs insecure TLS
//
// A denied read is not a failure: it drops the candidates it would have produced
// and leaves the rest, so a narrowly permitted identity still gets a usable path.
func discoverCandidates(ctx context.Context, kube kubernetes.Interface) ([]candidate, error) {
	config, err := readClusterConfig(ctx, kube)
	if err != nil {
		return nil, err
	}

	candidates := make([]candidate, 0, len(config.endpoints)+2)

	for _, endpoint := range config.endpoints {
		candidates = append(candidates, candidate{endpoint: endpoint, source: sourceMaster, caPEM: config.caPEM})
	}

	public, err := publicEndpoint(ctx, kube, config)
	if err != nil {
		return nil, err
	}

	if public != "" {
		candidates = append(candidates, candidate{endpoint: public, source: sourcePublic})
	}

	pods, err := discoverPodEndpoints(ctx, kube)
	if err != nil {
		return nil, err
	}

	for _, endpoint := range pods {
		candidates = append(candidates, candidate{endpoint: endpoint, source: sourcePod})
	}

	if len(candidates) == 0 {
		return nil, errors.New("no registry-packages-proxy endpoint found in the cluster")
	}

	return candidates, nil
}

// publicEndpoint prefers the endpoint the module published and reads the Ingress
// only when there is none, so a cluster with the published config needs no
// permission on Ingresses at all.
func publicEndpoint(ctx context.Context, kube kubernetes.Interface, config clusterConfig) (string, error) {
	if config.publicEndpoint != "" {
		return config.publicEndpoint, nil
	}

	endpoint, err := discoverIngressEndpoint(ctx, kube)
	switch {
	case err == nil:
		return endpoint, nil
	case errors.Is(err, errIngressUnusable), isReadDenied(err):
		return "", nil
	default:
		return "", err
	}
}

// discoverIngressEndpoint returns the public proxy endpoint (https://<host>) taken
// from the registry-packages-proxy Ingress.
//
// The scheme is always https and is deliberately not taken from the Ingress TLS
// block. Every request carries the kubeconfig bearer token, so a plain http
// endpoint would put it on the wire in the clear. The TLS block also says nothing
// about a TLS terminator standing in front of the cluster, where https is the
// right choice even though the Ingress itself serves http. An endpoint that cannot
// complete a handshake is simply skipped, see discoverCandidates.
//
// An absent Ingress or one with no host yields errIngressUnusable. Any other error
// is returned raw so the caller can surface the API-leg failure.
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

// discoverPodEndpoints returns an endpoint per serving proxy pod. Terminating and
// not-yet-ready pods are skipped.
//
// These are master-node pod IPs, reachable from inside the cluster network. A
// workstation outside the cluster usually cannot reach them.
func discoverPodEndpoints(ctx context.Context, kube kubernetes.Interface) ([]string, error) {
	pods, err := kube.CoreV1().Pods(proxyNamespace).List(ctx, metav1.ListOptions{LabelSelector: proxyPodSelector})
	if err != nil {
		if isReadDenied(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("list registry-packages-proxy pods: %w", err)
	}

	endpoints := make([]string, 0, len(pods.Items))

	for i := range pods.Items {
		pod := &pods.Items[i]
		if !podIsServing(pod) {
			continue
		}

		base := url.URL{Scheme: proxyScheme, Host: net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(proxyPort))}
		endpoints = append(endpoints, base.String())
	}

	return endpoints, nil
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

// isReadDenied reports whether the API rejected the identity rather than failed.
// Such a read yields no candidates, while the rest of discovery carries on.
func isReadDenied(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err)
}
