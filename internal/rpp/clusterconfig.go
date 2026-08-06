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
	"encoding/json"
	"fmt"
	"net/url"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// proxyConfigMapName is published by the registry-packages-proxy module. It
	// answers both questions a client has: where the proxy is, and which CA signs
	// the certificate it serves.
	proxyConfigMapName = "registry-packages-proxy-config"

	configKeyEndpoints      = "endpoints"
	configKeyCA             = "ca.crt"
	configKeyPublicEndpoint = "publicEndpoint"
)

// clusterConfig is the published configuration of the proxy.
//
// A cluster running an older module version publishes nothing, and an identity
// without the cli-download role cannot read it. Both cases leave every field
// empty instead of failing: discovery still works through the Ingress.
type clusterConfig struct {
	// endpoints are base URLs built from the master addresses, verifiable with caPEM.
	endpoints []string

	// publicEndpoint is the base URL of the public Ingress, empty when the proxy
	// is not published under a public domain.
	publicEndpoint string

	// caPEM verifies the certificate served on the master addresses.
	caPEM []byte
}

// readClusterConfig returns the configuration published by the module. An absent
// ConfigMap and a denied read are reported as an empty config with no error,
// because both are normal on clusters this CLI still has to work with. Malformed
// content is an error: it means the published contract changed.
func readClusterConfig(ctx context.Context, kube kubernetes.Interface) (clusterConfig, error) {
	configMap, err := kube.CoreV1().ConfigMaps(proxyNamespace).Get(ctx, proxyConfigMapName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
			return clusterConfig{}, nil
		}

		return clusterConfig{}, fmt.Errorf("get configmap %s/%s: %w", proxyNamespace, proxyConfigMapName, err)
	}

	endpoints, err := parseConfigEndpoints(configMap.Data[configKeyEndpoints])
	if err != nil {
		return clusterConfig{}, err
	}

	config := clusterConfig{
		endpoints:      endpoints,
		publicEndpoint: configMap.Data[configKeyPublicEndpoint],
	}

	if ca := configMap.Data[configKeyCA]; ca != "" {
		config.caPEM = []byte(ca)
	}

	return config, nil
}

// parseConfigEndpoints turns the published "host:port" list into base URLs.
func parseConfigEndpoints(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}

	var addresses []string
	if err := json.Unmarshal([]byte(raw), &addresses); err != nil {
		return nil, fmt.Errorf("parse %q in configmap %s/%s: %w", configKeyEndpoints, proxyNamespace, proxyConfigMapName, err)
	}

	endpoints := make([]string, 0, len(addresses))

	for _, address := range addresses {
		if address == "" {
			continue
		}

		endpoints = append(endpoints, (&url.URL{Scheme: proxyScheme, Host: address}).String())
	}

	return endpoints, nil
}
