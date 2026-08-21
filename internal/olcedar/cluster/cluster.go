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

// Package cluster reads the cluster's half of a node configuration: the
// NodeGroup template, the names its nodes already hold, and the node that
// appears once the machine has installed itself.
package cluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// GroupLabel is the label every node of a NodeGroup carries.
const GroupLabel = "node.deckhouse.io/group"

// The NodeGroup a machine may be added to. Mirrors machineOwnedConfig of
// node-controller/src/internal/controller/nodebootstrap/template_storage.go,
// which decides the same thing on the serving side.
const (
	nodeTypeStatic      = "Static"
	systemTypeImmutable = "Immutable"
)

// templateGVR is the aggregated resource that renders a NodeGroup's node
// configuration. Nothing is stored behind it: every read renders from the
// cluster as it is now, which is why the answer carries a live bootstrap token.
var templateGVR = schema.GroupVersionResource{
	Group:    "templates.internal.deckhouse.io",
	Version:  "v1alpha1",
	Resource: "nodeconfigtemplates",
}

var nodeGroupGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1",
	Resource: "nodegroups",
}

// FetchTemplate reads the template of one NodeGroup. A group the cluster
// provisions itself has no template and answers 404, which says nothing about
// why, so the refusal is explained against the NodeGroup itself.
func FetchTemplate(ctx context.Context, dyn dynamic.Interface, group string) (*unstructured.Unstructured, error) {
	template, err := dyn.Resource(templateGVR).Get(ctx, group, metav1.GetOptions{})
	if err == nil {
		return template, nil
	}

	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("read the node configuration template of %s: %w. "+
			"It is served by an aggregated API, so this read is proxied by the kube-apiserver to node-controller "+
			"of node-manager: a node-controller that is down, unreachable or unregistered fails it. "+
			"Check it with: d8 k get apiservice v1alpha1.templates.internal.deckhouse.io", group, err)
	}

	return nil, explainMissingTemplate(ctx, dyn, group)
}

func explainMissingTemplate(ctx context.Context, dyn dynamic.Interface, group string) error {
	nodeGroup, err := dyn.Resource(nodeGroupGVR).Get(ctx, group, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("there is no NodeGroup %q in this cluster", group)
	}

	if err != nil {
		return fmt.Errorf("the cluster serves no node configuration template for %s, and reading the NodeGroup to say why failed: %w", group, err)
	}

	nodeType, systemType := groupTypes(nodeGroup)

	return fmt.Errorf(
		"NodeGroup %s is nodeType %q, systemType %q, and only nodeType %s with systemType %s has machines that read a configuration. "+
			"Nodes of this group are provisioned by the cluster itself, so there is nothing to push to a machine",
		group, nodeType, systemType, nodeTypeStatic, systemTypeImmutable)
}

// ImmutableStaticGroups lists the NodeGroups whose machines take a
// configuration by hand. It backs the completion of --group: a group that
// cannot be added to is not offered.
func ImmutableStaticGroups(ctx context.Context, dyn dynamic.Interface) ([]string, error) {
	list, err := dyn.Resource(nodeGroupGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list NodeGroups: %w", err)
	}

	var groups []string

	for i := range list.Items {
		nodeType, systemType := groupTypes(&list.Items[i])
		if nodeType == nodeTypeStatic && systemType == systemTypeImmutable {
			groups = append(groups, list.Items[i].GetName())
		}
	}

	return groups, nil
}

func groupTypes(nodeGroup *unstructured.Unstructured) (string, string) {
	nodeType, _, _ := unstructured.NestedString(nodeGroup.Object, "spec", "nodeType")
	systemType, _, _ := unstructured.NestedString(nodeGroup.Object, "spec", "systemType")

	return nodeType, systemType
}

// FreeNodeName is the first <group>-<N> no node of the group holds. A machine
// that was handed a configuration but has not registered yet holds no name
// here, so the name is checked again right before the push.
func FreeNodeName(ctx context.Context, kube kubernetes.Interface, group string) (string, error) {
	nodes, err := kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: GroupLabel + "=" + group})
	if err != nil {
		return "", fmt.Errorf("list the nodes of %s: %w", group, err)
	}

	taken := make(map[int]bool, len(nodes.Items))

	for i := range nodes.Items {
		if index, ok := nodeIndex(nodes.Items[i].Name, group); ok {
			taken[index] = true
		}
	}

	for index := 0; ; index++ {
		if !taken[index] {
			return fmt.Sprintf("%s-%d", group, index), nil
		}
	}
}

func nodeIndex(nodeName, group string) (int, bool) {
	suffix, found := strings.CutPrefix(nodeName, group+"-")
	if !found {
		return 0, false
	}

	index, err := strconv.Atoi(suffix)
	if err != nil || index < 0 {
		return 0, false
	}

	return index, true
}

// NodeNameByAddress names the node that registered with this address, so a
// machine that already is one can be refused by name rather than by address.
func NodeNameByAddress(ctx context.Context, kube kubernetes.Interface, host string) string {
	nodes, err := kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return ""
	}

	for i := range nodes.Items {
		for _, address := range nodes.Items[i].Status.Addresses {
			if address.Type == corev1.NodeInternalIP && address.Address == host {
				return nodes.Items[i].Name
			}
		}
	}

	return ""
}

// NodeExists answers whether the cluster already holds a node under this name.
func NodeExists(ctx context.Context, kube kubernetes.Interface, name string) (bool, error) {
	_, err := kube.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("read node %s: %w", name, err)
	}

	return true, nil
}

// pollInterval is how often the cluster is asked about the node, and
// tickInterval how often the wait says out loud that it is still waiting.
const (
	pollInterval = 5 * time.Second
	tickInterval = 30 * time.Second
)

// WaitForNode waits until the node registers, reporting how long it has been
// waiting through tick. Registration is what this command can promise: it means
// the machine installed itself and its kubelet reached the cluster. Readiness
// comes later, from the modules rolled onto the node.
func WaitForNode(
	ctx context.Context,
	kube kubernetes.Interface,
	name string,
	timeout time.Duration,
	tick func(elapsed time.Duration),
) error {
	started := time.Now()
	ticked := started

	err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		registered, err := NodeExists(ctx, kube, name)
		if err != nil || registered {
			return registered, err
		}

		if time.Since(ticked) >= tickInterval {
			ticked = time.Now()
			tick(time.Since(started).Round(time.Second))
		}

		return false, nil
	})
	if err != nil {
		return fmt.Errorf("wait for node %s to register after %s: %w", name, time.Since(started).Round(time.Second), err)
	}

	return nil
}
