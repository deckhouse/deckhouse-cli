/*
Copyright 2025 Flant JSC

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

package requirements

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiversion "k8s.io/apimachinery/pkg/version"
	fakediscovery "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
)

func deckhouseDeployment(version string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        deckhouseDeploymentName,
			Namespace:   deckhouseNamespace,
			Annotations: map[string]string{deckhouseVersionAnnotation: version},
		},
	}
}

func moduleObject(name, version string, enabled bool) *unstructured.Unstructured {
	status := "False"
	if enabled {
		status = conditionStatusTrue
	}

	return moduleObjectCond(name, version, "EnabledByModuleManager", status)
}

func moduleObjectCond(name, version, condType, condStatus string) *unstructured.Unstructured {
	properties := map[string]any{}
	if version != "" {
		properties["version"] = version
	}

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "deckhouse.io/v1alpha1",
		"kind":       "Module",
		"metadata":   map[string]any{"name": name},
		"properties": properties,
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": condType, "status": condStatus},
			},
		},
	}}
}

func fakeDynamic(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{moduleGVR: "ModuleList"},
		objs...,
	)
}

func fakeKube(version string, objs ...runtime.Object) *k8sfake.Clientset {
	kube := k8sfake.NewSimpleClientset(objs...)
	kube.Discovery().(*fakediscovery.FakeDiscovery).FakedServerVersion = &apiversion.Info{GitVersion: version}

	return kube
}

func TestLoadClusterState(t *testing.T) {
	kube := fakeKube("v1.28.3", deckhouseDeployment("v1.65.3"))
	dyn := fakeDynamic(
		moduleObject("stronghold", "v1.2.0", true),
		moduleObject("disabled-mod", "v1.0.0", false),
		moduleObject("no-version", "", true),
		moduleObjectCond("config-only", "v1.0.0", "EnabledByModuleConfig", "True"),
	)

	state, err := LoadClusterState(context.Background(), kube, dyn, dkplog.NewNop())
	require.NoError(t, err)

	require.NotNil(t, state.Kubernetes)
	assert.Equal(t, "1.28.3", state.Kubernetes.String())

	require.NotNil(t, state.Deckhouse)
	assert.Equal(t, "1.65.3", state.Deckhouse.String())

	assert.True(t, state.Modules["stronghold"].Enabled)
	require.NotNil(t, state.Modules["stronghold"].Version)
	assert.Equal(t, "1.2.0", state.Modules["stronghold"].Version.String())

	assert.False(t, state.Modules["disabled-mod"].Enabled)

	assert.True(t, state.Modules["no-version"].Enabled)
	assert.Nil(t, state.Modules["no-version"].Version, "absent version is nil, not an error")

	assert.True(t, state.Modules["config-only"].Enabled, "EnabledByModuleConfig alone counts as enabled")

	_, present := state.Modules["does-not-exist"]
	assert.False(t, present)
}

func TestLoadClusterStateDevDeckhouseIsNil(t *testing.T) {
	kube := fakeKube("v1.28.3", deckhouseDeployment("dev"))
	dyn := fakeDynamic()

	state, err := LoadClusterState(context.Background(), kube, dyn, dkplog.NewNop())
	require.NoError(t, err)
	assert.Nil(t, state.Deckhouse, "non-release deckhouse version is recorded as nil")
}

func TestLoadClusterStateDeckhouseReadErrorIsHard(t *testing.T) {
	// No deckhouse deployment object → Get returns NotFound. Inability to READ the
	// version must be a hard error (not silently skipped like the dev case).
	kube := fakeKube("v1.28.3")

	_, err := LoadClusterState(context.Background(), kube, fakeDynamic(), dkplog.NewNop())
	require.Error(t, err)
}

func TestLoadClusterStateUnparseableKubernetesVersion(t *testing.T) {
	// A version string the cluster returns but semver cannot parse → nil (not fatal),
	// so a module/Deckhouse-only plugin is not blocked.
	kube := fakeKube("v1.28+", deckhouseDeployment("dev"))

	state, err := LoadClusterState(context.Background(), kube, fakeDynamic(), dkplog.NewNop())
	require.NoError(t, err)
	assert.Nil(t, state.Kubernetes)
}
