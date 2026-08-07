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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func proxyConfigMap(data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: proxyConfigMapName, Namespace: proxyNamespace},
		Data:       data,
	}
}

func TestReadClusterConfigReturnsPublishedValues(t *testing.T) {
	kube := fake.NewSimpleClientset(proxyConfigMap(map[string]string{
		"endpoints":      `["192.168.0.1:4219","192.168.0.2:4219"]`,
		"ca.crt":         "CA-PEM",
		"publicEndpoint": "https://registry-packages-proxy.example.com",
	}))

	config, err := readClusterConfig(context.Background(), kube)
	require.NoError(t, err)

	assert.Equal(t, []string{"https://192.168.0.1:4219", "https://192.168.0.2:4219"}, config.endpoints)
	assert.Equal(t, "https://registry-packages-proxy.example.com", config.publicEndpoint)
	assert.Equal(t, []byte("CA-PEM"), config.caPEM)
}

func TestReadClusterConfigWithoutPublicEndpoint(t *testing.T) {
	// A cluster with no public domain publishes the master addresses only, and
	// that is the case this whole path exists for.
	kube := fake.NewSimpleClientset(proxyConfigMap(map[string]string{
		"endpoints": `["192.168.0.1:4219"]`,
		"ca.crt":    "CA-PEM",
	}))

	config, err := readClusterConfig(context.Background(), kube)
	require.NoError(t, err)

	assert.Equal(t, []string{"https://192.168.0.1:4219"}, config.endpoints)
	assert.Empty(t, config.publicEndpoint)
	assert.Equal(t, []byte("CA-PEM"), config.caPEM)
}

func TestReadClusterConfigWithoutCertificate(t *testing.T) {
	// The module publishes no CA until the certificate is issued. The addresses
	// are still useful, but nothing can verify them yet.
	kube := fake.NewSimpleClientset(proxyConfigMap(map[string]string{
		"endpoints": `["192.168.0.1:4219"]`,
	}))

	config, err := readClusterConfig(context.Background(), kube)
	require.NoError(t, err)

	assert.Equal(t, []string{"https://192.168.0.1:4219"}, config.endpoints)
	assert.Nil(t, config.caPEM)
}

func TestReadClusterConfigTreatsAbsentConfigMapAsEmpty(t *testing.T) {
	config, err := readClusterConfig(context.Background(), fake.NewSimpleClientset())
	require.NoError(t, err, "an older cluster publishes no config, and that is not a failure")

	assert.Empty(t, config.endpoints)
	assert.Empty(t, config.publicEndpoint)
	assert.Empty(t, config.caPEM)
}

func TestReadClusterConfigTreatsDeniedReadAsEmpty(t *testing.T) {
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("get", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Resource: "configmaps"}, proxyConfigMapName, errors.New("no access"))
	})

	config, err := readClusterConfig(context.Background(), kube)
	require.NoError(t, err, "an identity without the role falls back to the Ingress instead of failing")

	assert.Empty(t, config.endpoints)
}

func TestReadClusterConfigRejectsMalformedEndpoints(t *testing.T) {
	kube := fake.NewSimpleClientset(proxyConfigMap(map[string]string{
		"endpoints": `192.168.0.1:4219`,
	}))

	_, err := readClusterConfig(context.Background(), kube)
	require.Error(t, err, "a broken contract must be reported, not silently ignored")
	assert.Contains(t, err.Error(), configKeyEndpoints)
}

func TestReadClusterConfigSurfacesAPIFailure(t *testing.T) {
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("get", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("tls: failed to verify certificate")
	})

	_, err := readClusterConfig(context.Background(), kube)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tls: failed to verify certificate")
}

func TestReadClusterConfigSkipsEmptyAddresses(t *testing.T) {
	kube := fake.NewSimpleClientset(proxyConfigMap(map[string]string{
		"endpoints": `["192.168.0.1:4219",""]`,
	}))

	config, err := readClusterConfig(context.Background(), kube)
	require.NoError(t, err)

	assert.Equal(t, []string{"https://192.168.0.1:4219"}, config.endpoints)
}
