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
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/kubectl/pkg/util/term"
	"k8s.io/utils/ptr"
)

const (
	resourceName      = "istioctl-debug"
	containerName     = "istioctl-debug"
	debugCMNamespace  = "d8-system"
	debugCMName       = "debug-container"
	debugCMImageKey   = "image"
	podReadyTimeout   = 2 * time.Minute
	podPollInterval   = time.Second
	podDeleteTimeout  = 15 * time.Second
	podDeleteGraceSec = int64(1)
)

// Options controls how the debug session is created.
type Options struct {
	Namespace       string
	TargetNamespace string
	Image           string
	Command         []string
}

// Run applies istioctl-debug RBAC, starts the debug pod, attaches to it, and
// deletes the pod when the session ends.
func Run(ctx context.Context, kube kubernetes.Interface, restConfig *rest.Config, opts Options) error {
	image, err := resolveDebugImage(ctx, kube, opts.Image)
	if err != nil {
		return err
	}

	if err := ensureRBAC(ctx, kube, opts.Namespace, opts.TargetNamespace); err != nil {
		return err
	}

	pod, err := createDebugPod(ctx, kube, opts.Namespace, image, opts.Command)
	if err != nil {
		return err
	}

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), podDeleteTimeout)
		defer cancel()

		if delErr := deletePod(cleanupCtx, kube, opts.Namespace, pod.Name); delErr != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to delete debug pod %s/%s: %v\n", opts.Namespace, pod.Name, delErr)
		}
	}()

	fmt.Fprintf(os.Stderr, "Waiting for pod %s/%s to be running...\n", opts.Namespace, pod.Name)

	if err := waitForPodRunning(ctx, kube, opts.Namespace, pod.Name); err != nil {
		return fmt.Errorf("wait for debug pod: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Attached to %s/%s (image %s). RBAC target namespace: %s\n",
		opts.Namespace, pod.Name, image, opts.TargetNamespace)

	return attachToPod(ctx, kube, restConfig, opts.Namespace, pod.Name)
}

func resolveDebugImage(ctx context.Context, kube kubernetes.Interface, override string) (string, error) {
	if override != "" {
		return override, nil
	}

	cm, err := kube.CoreV1().ConfigMaps(debugCMNamespace).Get(ctx, debugCMName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get debug image from ConfigMap %s/%s (pass --image to override): %w", debugCMNamespace, debugCMName, err)
	}

	image := cm.Data[debugCMImageKey]
	if image == "" {
		return "", fmt.Errorf("ConfigMap %s/%s has no %q key; pass --image to override", debugCMNamespace, debugCMName, debugCMImageKey)
	}

	return image, nil
}

func ensureRBAC(ctx context.Context, kube kubernetes.Interface, debugNamespace, targetNamespace string) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: debugNamespace,
		},
	}

	_, err := kube.CoreV1().ServiceAccounts(debugNamespace).Create(ctx, sa, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create ServiceAccount %s/%s: %w", debugNamespace, resourceName, err)
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: targetNamespace,
		},
		Rules: istioctlDebugRules(),
	}
	if err := createOrUpdateRole(ctx, kube, role); err != nil {
		return err
	}

	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: targetNamespace,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      resourceName,
			Namespace: debugNamespace,
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     resourceName,
		},
	}
	if err := createOrUpdateRoleBinding(ctx, kube, binding); err != nil {
		return err
	}

	return nil
}

func istioctlDebugRules() []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{
		{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{"get", "list"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"pods/portforward"},
			Verbs:     []string{"create"},
		},
	}
}

func createOrUpdateRole(ctx context.Context, kube kubernetes.Interface, role *rbacv1.Role) error {
	existing, err := kube.RbacV1().Roles(role.Namespace).Get(ctx, role.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = kube.RbacV1().Roles(role.Namespace).Create(ctx, role, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create Role %s/%s: %w", role.Namespace, role.Name, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("get Role %s/%s: %w", role.Namespace, role.Name, err)
	}

	existing.Rules = role.Rules

	_, err = kube.RbacV1().Roles(role.Namespace).Update(ctx, existing, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("update Role %s/%s: %w", role.Namespace, role.Name, err)
	}

	return nil
}

func createOrUpdateRoleBinding(ctx context.Context, kube kubernetes.Interface, binding *rbacv1.RoleBinding) error {
	existing, err := kube.RbacV1().RoleBindings(binding.Namespace).Get(ctx, binding.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = kube.RbacV1().RoleBindings(binding.Namespace).Create(ctx, binding, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create RoleBinding %s/%s: %w", binding.Namespace, binding.Name, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("get RoleBinding %s/%s: %w", binding.Namespace, binding.Name, err)
	}

	existing.Subjects = binding.Subjects
	existing.RoleRef = binding.RoleRef

	_, err = kube.RbacV1().RoleBindings(binding.Namespace).Update(ctx, existing, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("update RoleBinding %s/%s: %w", binding.Namespace, binding.Name, err)
	}

	return nil
}

func buildDebugPod(namespace, image string, command []string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       resourceName,
				"app.kubernetes.io/managed-by": "d8",
			},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:           resourceName,
			AutomountServiceAccountToken: ptr.To(true),
			RestartPolicy:                corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:            containerName,
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         command,
				Stdin:           true,
				StdinOnce:       true,
				TTY:             true,
			}},
		},
	}
}

func createDebugPod(ctx context.Context, kube kubernetes.Interface, namespace, image string, command []string) (*corev1.Pod, error) {
	if existing, err := kube.CoreV1().Pods(namespace).Get(ctx, resourceName, metav1.GetOptions{}); err == nil {
		fmt.Fprintf(os.Stderr, "Deleting leftover debug pod %s/%s\n", namespace, existing.Name)

		if err := deletePod(ctx, kube, namespace, existing.Name); err != nil {
			return nil, err
		}

		if err := waitForPodGone(ctx, kube, namespace, existing.Name); err != nil {
			return nil, err
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get debug pod %s/%s: %w", namespace, resourceName, err)
	}

	pod, err := kube.CoreV1().Pods(namespace).Create(ctx, buildDebugPod(namespace, image, command), metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create debug pod %s/%s: %w", namespace, resourceName, err)
	}

	return pod, nil
}

func deletePod(ctx context.Context, kube kubernetes.Interface, namespace, name string) error {
	err := kube.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{
		GracePeriodSeconds: ptr.To(podDeleteGraceSec),
	})
	if apierrors.IsNotFound(err) {
		return nil
	}

	return err
}

func waitForPodGone(ctx context.Context, kube kubernetes.Interface, namespace, name string) error {
	return wait.PollUntilContextTimeout(ctx, podPollInterval, podReadyTimeout, true, func(ctx context.Context) (bool, error) {
		_, err := kube.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}

		if err != nil {
			return false, err
		}

		return false, nil
	})
}

func waitForPodRunning(ctx context.Context, kube kubernetes.Interface, namespace, name string) error {
	return wait.PollUntilContextTimeout(ctx, podPollInterval, podReadyTimeout, true, func(ctx context.Context) (bool, error) {
		pod, err := kube.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		switch pod.Status.Phase {
		case corev1.PodRunning:
			return true, nil
		case corev1.PodFailed, corev1.PodSucceeded:
			return false, fmt.Errorf("pod %s/%s ended with phase %s: %s", namespace, name, pod.Status.Phase, podReason(pod))
		}

		if reason, msg, ok := terminalContainerWait(pod); ok {
			return false, fmt.Errorf("pod %s/%s cannot start: %s: %s", namespace, name, reason, msg)
		}

		return false, nil
	})
}

func podReason(pod *corev1.Pod) string {
	if pod.Status.Message != "" {
		return pod.Status.Message
	}

	if reason, msg, ok := terminalContainerWait(pod); ok {
		if msg != "" {
			return fmt.Sprintf("%s: %s", reason, msg)
		}

		return reason
	}

	return pod.Status.Reason
}

func terminalContainerWait(pod *corev1.Pod) (string, string, bool) {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting == nil {
			continue
		}

		switch cs.State.Waiting.Reason {
		case "ErrImagePull", "ImagePullBackOff", "CrashLoopBackOff", "CreateContainerConfigError", "InvalidImageName":
			return cs.State.Waiting.Reason, cs.State.Waiting.Message, true
		}
	}

	return "", "", false
}

func attachToPod(ctx context.Context, kube kubernetes.Interface, restConfig *rest.Config, namespace, name string) error {
	req := kube.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(name).
		Namespace(namespace).
		SubResource("attach").
		VersionedParams(&corev1.PodAttachOptions{
			Container: containerName,
			Stdin:     true,
			Stdout:    true,
			Stderr:    false,
			TTY:       true,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("create attach executor: %w", err)
	}

	tty := term.TTY{
		In:  os.Stdin,
		Out: os.Stdout,
		Raw: true,
	}

	var sizeQueue remotecommand.TerminalSizeQueue
	if tty.IsTerminalIn() {
		sizeQueue = tty.MonitorSize(tty.GetSize())
	}

	return tty.Safe(func() error {
		streamErr := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
			Stdin:             os.Stdin,
			Stdout:            os.Stdout,
			Stderr:            nil,
			Tty:               true,
			TerminalSizeQueue: sizeQueue,
		})
		if streamErr != nil {
			return fmt.Errorf("attach to pod %s/%s: %w", namespace, name, streamErr)
		}

		return nil
	})
}
