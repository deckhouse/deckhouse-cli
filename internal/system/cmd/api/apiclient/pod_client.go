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
	"fmt"
	"net"
	"net/url"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	addonOperatorListenAddressEnv       = "ADDON_OPERATOR_LISTEN_ADDRESS"
	httpsPortName                       = "https"
	kubeRBACProxyServingCAConfigMapName = "kube-rbac-proxy-ca.crt"
)

// NewForPod creates a controller API client for the selected Pod.
// Its listener configuration selects the protected HTTPS port or legacy pods/proxy.
// Connection and authorization failures never switch transports.
func NewForPod(
	ctx context.Context,
	config *rest.Config,
	kubeCl kubernetes.Interface,
	pod *corev1.Pod,
) (*Client, error) {
	if config == nil {
		return nil, fmt.Errorf("kubeconfig REST configuration is required")
	}

	viaRBACProxy, err := controllerAPIRequiresRBACProxy(pod)
	if err != nil {
		return nil, err
	}

	if !viaRBACProxy {
		proxy, err := NewProxyTransport(config, kubeCl, pod)
		if err != nil {
			return nil, err
		}

		return New(proxy), nil
	}

	endpoint, err := controllerRBACProxyEndpoint(pod)
	if err != nil {
		return nil, err
	}

	caPEM, err := loadRBACProxyServingCA(ctx, kubeCl)
	if err != nil {
		return nil, err
	}

	proxy, err := NewRBACProxyTransport(config, endpoint, caPEM)
	if err != nil {
		return nil, fmt.Errorf("configure controller API access for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	return New(proxy), nil
}

// A Pod-IP listener supports pods/proxy; a loopback listener is reachable only through the Pod's HTTPS port.
// Unknown templates must not silently select a less restrictive transport.
func controllerAPIRequiresRBACProxy(pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, fmt.Errorf("controller pod is required")
	}

	for _, container := range pod.Spec.Containers {
		if container.Name != ContainerName {
			continue
		}

		for _, env := range container.Env {
			if env.Name != addonOperatorListenAddressEnv {
				continue
			}

			if env.ValueFrom == nil && env.Value == "127.0.0.1" {
				return true, nil
			}

			if env.Value == "" && env.ValueFrom != nil {
				fieldRef := env.ValueFrom.FieldRef
				if fieldRef != nil && fieldRef.FieldPath == "status.podIP" {
					return false, nil
				}
			}

			return false, fmt.Errorf("pod %s/%s has an unsupported %s configuration", pod.Namespace, pod.Name, addonOperatorListenAddressEnv)
		}

		return false, fmt.Errorf("pod %s/%s does not declare %s", pod.Namespace, pod.Name, addonOperatorListenAddressEnv)
	}

	return false, fmt.Errorf("pod %s/%s has no %q container", pod.Namespace, pod.Name, ContainerName)
}

// controllerRBACProxyEndpoint returns the HTTPS origin of the protected controller API.
// The Pod declares it as the port named "https". Any container may own the port:
// the controller serves it itself, or a proxy container does.
func controllerRBACProxyEndpoint(pod *corev1.Pod) (string, error) {
	if !pod.Spec.HostNetwork {
		return "", fmt.Errorf("pod %s/%s: protected controller API access requires hostNetwork", pod.Namespace, pod.Name)
	}

	if net.ParseIP(pod.Status.PodIP) == nil {
		return "", fmt.Errorf("pod %s/%s has no valid Pod IP for the protected controller API", pod.Namespace, pod.Name)
	}

	var httpsPort int32

	for _, container := range pod.Spec.Containers {
		for _, port := range container.Ports {
			if port.Name != httpsPortName {
				continue
			}

			if port.ContainerPort < 1 || port.ContainerPort > 65535 || (port.Protocol != "" && port.Protocol != corev1.ProtocolTCP) {
				return "", fmt.Errorf("pod %s/%s: HTTPS port must be TCP in range 1-65535", pod.Namespace, pod.Name)
			}

			if httpsPort != 0 && httpsPort != port.ContainerPort {
				return "", fmt.Errorf("pod %s/%s declares more than one HTTPS port", pod.Namespace, pod.Name)
			}

			httpsPort = port.ContainerPort
		}
	}

	if httpsPort == 0 {
		return "", fmt.Errorf("pod %s/%s: controller API is loopback-only; its HTTPS port is not configured yet", pod.Namespace, pod.Name)
	}

	endpoint := url.URL{
		Scheme: "https",
		Host:   net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(httpsPort))),
	}

	return endpoint.String(), nil
}

func loadRBACProxyServingCA(ctx context.Context, kubeCl kubernetes.Interface) ([]byte, error) {
	configMap, err := kubeCl.CoreV1().ConfigMaps(Namespace).Get(ctx, kubeRBACProxyServingCAConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read the serving CA of the protected controller API from ConfigMap %s/%s: %w", Namespace, kubeRBACProxyServingCAConfigMapName, err)
	}

	caPEM := configMap.Data["ca.crt"]
	if caPEM == "" {
		return nil, fmt.Errorf("ConfigMap %s/%s has no serving CA in ca.crt", Namespace, kubeRBACProxyServingCAConfigMapName)
	}

	return []byte(caPEM), nil
}
