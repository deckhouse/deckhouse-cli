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

package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func node(name, group string, addresses ...string) *corev1.Node {
	status := corev1.NodeStatus{}
	for _, address := range addresses {
		status.Addresses = append(status.Addresses, corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: address})
	}

	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{GroupLabel: group}},
		Status:     status,
	}
}

func nodeGroup(name, nodeType, systemType string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "deckhouse.io/v1",
		"kind":       "NodeGroup",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"nodeType": nodeType, "systemType": systemType},
	}}
}

func template(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.internal.deckhouse.io/v1alpha1",
		"kind":       "NodeConfigTemplate",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"nodeName": ""},
	}}
}

func dynamicClient(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()

	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		templateGVR:  "NodeConfigTemplateList",
		nodeGroupGVR: "NodeGroupList",
	}, objects...)
}

// The template of an existing immutable static group is what the command builds
// the document out of.
func TestFetchTemplateReadsTheGroupTemplate(t *testing.T) {
	got, err := FetchTemplate(context.Background(), dynamicClient(template("worker")), "worker")
	require.NoError(t, err)
	require.Equal(t, "worker", got.GetName())
}

// A 404 says nothing about why, so the refusal names the reason the operator
// can act on rather than "not found".
func TestFetchTemplateExplainsAGroupOfTheWrongType(t *testing.T) {
	_, err := FetchTemplate(context.Background(), dynamicClient(nodeGroup("worker", "CloudEphemeral", "")), "worker")
	require.ErrorContains(t, err, "CloudEphemeral")
	require.ErrorContains(t, err, "provisioned by the cluster itself")
}

func TestFetchTemplateExplainsAMissingGroup(t *testing.T) {
	_, err := FetchTemplate(context.Background(), dynamicClient(), "worker")
	require.ErrorContains(t, err, `there is no NodeGroup "worker"`)
}

func TestImmutableStaticGroupsOffersOnlyGroupsThatTakeAMachine(t *testing.T) {
	groups, err := ImmutableStaticGroups(context.Background(), dynamicClient(
		nodeGroup("worker", "Static", "Immutable"),
		nodeGroup("legacy", "Static", ""),
		nodeGroup("cloud", "CloudEphemeral", ""),
	))
	require.NoError(t, err)
	require.Equal(t, []string{"worker"}, groups)
}

// The default name has to be free: a group holding worker-0 gets worker-1.
func TestFreeNodeNameTakesTheFirstFreeNumber(t *testing.T) {
	kube := fake.NewSimpleClientset(node("worker-0", "worker"))

	name, err := FreeNodeName(context.Background(), kube, "worker")
	require.NoError(t, err)
	require.Equal(t, "worker-1", name)
}

func TestFreeNodeNameFillsAHoleAndIgnoresOtherGroups(t *testing.T) {
	kube := fake.NewSimpleClientset(
		node("worker-0", "worker"),
		node("worker-2", "worker"),
		node("worker-gpu-1", "worker-gpu"),
	)

	name, err := FreeNodeName(context.Background(), kube, "worker")
	require.NoError(t, err)
	require.Equal(t, "worker-1", name)
}

func TestFreeNodeNameStartsAtZeroForAnEmptyGroup(t *testing.T) {
	name, err := FreeNodeName(context.Background(), fake.NewSimpleClientset(), "worker")
	require.NoError(t, err)
	require.Equal(t, "worker-0", name)
}

func TestNodeNameByAddressNamesTheNodeHoldingTheAddress(t *testing.T) {
	kube := fake.NewSimpleClientset(node("worker-0", "worker", "10.12.4.55"))

	require.Equal(t, "worker-0", NodeNameByAddress(context.Background(), kube, "10.12.4.55"))
	require.Empty(t, NodeNameByAddress(context.Background(), kube, "10.12.4.56"))
}

func TestNodeExists(t *testing.T) {
	kube := fake.NewSimpleClientset(node("worker-0", "worker"))

	exists, err := NodeExists(context.Background(), kube, "worker-0")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = NodeExists(context.Background(), kube, "worker-1")
	require.NoError(t, err)
	require.False(t, exists)
}
