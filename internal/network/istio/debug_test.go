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

package istio

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestEnsureRBACCreatesObjects(t *testing.T) {
	kube := fake.NewSimpleClientset()
	ctx := context.Background()

	require.NoError(t, ensureRBAC(ctx, kube, "debug-ns", "target-ns", ""))

	sa, err := kube.CoreV1().ServiceAccounts("debug-ns").Get(ctx, resourceName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "debug-ns", sa.Namespace)

	role, err := kube.RbacV1().Roles("target-ns").Get(ctx, resourceName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, istioctlDebugRules(), role.Rules)

	binding, err := kube.RbacV1().RoleBindings("target-ns").Get(ctx, resourceName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, binding.Subjects, 1)
	assert.Equal(t, rbacv1.ServiceAccountKind, binding.Subjects[0].Kind)
	assert.Equal(t, resourceName, binding.Subjects[0].Name)
	assert.Equal(t, "debug-ns", binding.Subjects[0].Namespace)
	assert.Equal(t, "Role", binding.RoleRef.Kind)
	assert.Equal(t, resourceName, binding.RoleRef.Name)
}

func TestEnsureRBACIsIdempotentAndUpdatesRules(t *testing.T) {
	kube := fake.NewSimpleClientset()
	ctx := context.Background()

	require.NoError(t, ensureRBAC(ctx, kube, "debug-ns", "target-ns", ""))

	role, err := kube.RbacV1().Roles("target-ns").Get(ctx, resourceName, metav1.GetOptions{})
	require.NoError(t, err)
	role.Rules = nil
	_, err = kube.RbacV1().Roles("target-ns").Update(ctx, role, metav1.UpdateOptions{})
	require.NoError(t, err)

	require.NoError(t, ensureRBAC(ctx, kube, "debug-ns", "target-ns", ""))

	role, err = kube.RbacV1().Roles("target-ns").Get(ctx, resourceName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, istioctlDebugRules(), role.Rules)
}

func TestEnsureRBACMergesRoleBindingSubjects(t *testing.T) {
	kube := fake.NewSimpleClientset()
	ctx := context.Background()

	require.NoError(t, ensureRBAC(ctx, kube, "debug-ns", "target-ns", ""))
	require.NoError(t, ensureRBAC(ctx, kube, "other-ns", "target-ns", ""))

	binding, err := kube.RbacV1().RoleBindings("target-ns").Get(ctx, resourceName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, binding.Subjects, 2)
	assert.Equal(t, "debug-ns", binding.Subjects[0].Namespace)
	assert.Equal(t, "other-ns", binding.Subjects[1].Namespace)
}

func TestEnsureRBACGrantsIstioNamespaceRead(t *testing.T) {
	kube := fake.NewSimpleClientset()
	ctx := context.Background()

	require.NoError(t, ensureRBAC(ctx, kube, "default", "app-ns", defaultIstioNS))

	role, err := kube.RbacV1().Roles(defaultIstioNS).Get(ctx, resourceName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, istioctlDebugRules(), role.Rules)

	for _, rule := range role.Rules {
		assert.NotContains(t, rule.Verbs, "update")
		assert.NotContains(t, rule.Verbs, "patch")
		assert.NotContains(t, rule.Verbs, "delete")
	}

	binding, err := kube.RbacV1().RoleBindings(defaultIstioNS).Get(ctx, resourceName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, binding.Subjects, 1)
	assert.Equal(t, "default", binding.Subjects[0].Namespace)

	_, err = kube.RbacV1().Roles("app-ns").Get(ctx, resourceName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestResolveDebugImage(t *testing.T) {
	ctx := context.Background()

	t.Run("override wins", func(t *testing.T) {
		kube := fake.NewSimpleClientset()
		img, err := resolveDebugImage(ctx, kube, "override:tag")
		require.NoError(t, err)
		assert.Equal(t, "override:tag", img)
	})

	t.Run("from configmap", func(t *testing.T) {
		kube := fake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: debugCMName, Namespace: debugCMNamespace},
			Data:       map[string]string{debugCMImageKey: "registry.example/debug:1"},
		})
		img, err := resolveDebugImage(ctx, kube, "")
		require.NoError(t, err)
		assert.Equal(t, "registry.example/debug:1", img)
	})

	t.Run("missing configmap", func(t *testing.T) {
		kube := fake.NewSimpleClientset()
		_, err := resolveDebugImage(ctx, kube, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pass --image")
	})

	t.Run("empty image key", func(t *testing.T) {
		kube := fake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: debugCMName, Namespace: debugCMNamespace},
			Data:       map[string]string{},
		})
		_, err := resolveDebugImage(ctx, kube, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), debugCMImageKey)
	})
}

func TestBuildDebugPod(t *testing.T) {
	pod := buildDebugPod("debug-ns", "img:tag", []string{"bash"})

	assert.Empty(t, pod.Name)
	assert.Equal(t, podGenerateName, pod.GenerateName)
	assert.Equal(t, "debug-ns", pod.Namespace)
	assert.Equal(t, resourceName, pod.Labels[labelAppName])
	assert.Equal(t, managedByD8, pod.Labels[labelManagedBy])
	assert.Equal(t, resourceName, pod.Spec.ServiceAccountName)
	require.NotNil(t, pod.Spec.AutomountServiceAccountToken)
	assert.True(t, *pod.Spec.AutomountServiceAccountToken)
	assert.Equal(t, corev1.RestartPolicyNever, pod.Spec.RestartPolicy)
	require.Len(t, pod.Spec.Containers, 1)
	c := pod.Spec.Containers[0]
	assert.Equal(t, "img:tag", c.Image)
	assert.Equal(t, []string{"bash"}, c.Command)
	assert.True(t, c.Stdin)
	assert.True(t, c.StdinOnce)
	assert.True(t, c.TTY)
}

func TestCreateDebugPodGarbageCollectsTerminalOnly(t *testing.T) {
	ctx := context.Background()

	terminal := buildDebugPod("debug-ns", "old:tag", []string{"bash"})
	terminal.Name = "istioctl-debug-old"
	terminal.UID = "uid-old"
	terminal.Status.Phase = corev1.PodSucceeded

	running := buildDebugPod("debug-ns", "live:tag", []string{"bash"})
	running.Name = "istioctl-debug-live"
	running.UID = "uid-live"
	running.Status.Phase = corev1.PodRunning

	kube := fake.NewSimpleClientset(terminal, running)
	prependGenerateName(kube)

	pod, err := createDebugPod(ctx, kube, "debug-ns", "new:tag", []string{"bash"})
	require.NoError(t, err)
	assert.Equal(t, "istioctl-debug-testhash", pod.Name)
	assert.Equal(t, "new:tag", pod.Spec.Containers[0].Image)

	_, err = kube.CoreV1().Pods("debug-ns").Get(ctx, terminal.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))

	live, err := kube.CoreV1().Pods("debug-ns").Get(ctx, running.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, corev1.PodRunning, live.Status.Phase)
}

func TestWaitForPodRunning(t *testing.T) {
	ctx := context.Background()
	pod := buildDebugPod("debug-ns", "img:tag", []string{"bash"})
	pod.Name = resourceName
	pod.Status.Phase = corev1.PodRunning
	kube := fake.NewSimpleClientset(pod)

	require.NoError(t, waitForPodRunning(ctx, kube, "debug-ns", resourceName))
}

func TestWaitForPodRunningImagePullError(t *testing.T) {
	ctx := context.Background()
	pod := buildDebugPod("debug-ns", "img:tag", []string{"bash"})
	pod.Name = resourceName
	pod.Status.Phase = corev1.PodPending
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		State: corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{
				Reason:  "ErrImagePull",
				Message: "not found",
			},
		},
	}}
	kube := fake.NewSimpleClientset(pod)

	err := waitForPodRunning(ctx, kube, "debug-ns", resourceName)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ErrImagePull")
}

func TestNewCommandFlags(t *testing.T) {
	cmd := NewCommand()
	assert.Equal(t, "istio", cmd.Use)
	for _, name := range []string{"namespace", "target-namespace", "istio-namespace", "image", "kubeconfig", "context"} {
		assert.NotNil(t, cmd.Flags().Lookup(name), "missing flag %s", name)
	}
}

func prependGenerateName(kube *fake.Clientset) {
	kube.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pod := action.(clienttesting.CreateAction).GetObject().(*corev1.Pod)
		if pod.Name == "" && pod.GenerateName != "" {
			pod.Name = pod.GenerateName + "testhash"
			pod.UID = "uid-testhash"
		}

		return false, nil, nil
	})
}
