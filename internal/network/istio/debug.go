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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	podGenerateName   = resourceName + "-"
	labelAppName      = "app.kubernetes.io/name"
	labelManagedBy    = "app.kubernetes.io/managed-by"
	managedByD8       = "d8"
	debugCMNamespace  = "d8-system"
	debugCMName       = "debug-container"
	debugCMImageKey   = "image"
	defaultIstioNS    = "d8-istio"
	podReadyTimeout   = 2 * time.Minute
	podPollInterval   = time.Second
	podDeleteTimeout  = 15 * time.Second
	podDeleteGraceSec = int64(1)
)

// Options controls how the debug session is created.
type Options struct {
	Namespace       string
	TargetNamespace string
	IstioNamespace  string
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

	if err := ensureRBAC(ctx, kube, opts.Namespace, opts.TargetNamespace, opts.IstioNamespace); err != nil {
		return err
	}

	pod, err := createDebugPod(ctx, kube, opts.Namespace, image, opts.Command)
	if err != nil {
		return err
	}

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), podDeleteTimeout)
		defer cancel()

		if delErr := deletePod(cleanupCtx, kube, opts.Namespace, pod.Name, pod.UID); delErr != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to delete debug pod %s/%s: %v\n", opts.Namespace, pod.Name, delErr)
		}
	}()

	fmt.Fprintf(os.Stderr, "Waiting for pod %s/%s to be running...\n", opts.Namespace, pod.Name)

	if err := waitForPodRunning(ctx, kube, opts.Namespace, pod.Name); err != nil {
		return fmt.Errorf("wait for debug pod: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Attached to %s/%s (image %s). RBAC namespaces: %s\n",
		opts.Namespace, pod.Name, image, strings.Join(uniqueNonEmpty(opts.TargetNamespace, opts.IstioNamespace), ", "))

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

func ensureRBAC(ctx context.Context, kube kubernetes.Interface, debugNamespace, targetNamespace, istioNamespace string) error {
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

	for _, ns := range uniqueNonEmpty(targetNamespace, istioNamespace) {
		rules := istioctlWorkloadRules()
		if ns == istioNamespace {
			rules = istioctlIstioControlPlaneRules()
		}

		if err := ensureNamespaceAccess(ctx, kube, debugNamespace, ns, rules); err != nil {
			return err
		}
	}

	return nil
}

func ensureNamespaceAccess(
	ctx context.Context,
	kube kubernetes.Interface,
	debugNamespace, roleNamespace string,
	rules []rbacv1.PolicyRule,
) error {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: roleNamespace,
		},
		Rules: rules,
	}
	if err := createOrUpdateRole(ctx, kube, role); err != nil {
		return err
	}

	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: roleNamespace,
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

	return createOrUpdateRoleBinding(ctx, kube, binding)
}

func uniqueNonEmpty(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))

	for _, value := range values {
		if value == "" {
			continue
		}

		if _, ok := seen[value]; ok {
			continue
		}

		seen[value] = struct{}{}
		out = append(out, value)
	}

	return out
}

func istioctlWorkloadRules() []rbacv1.PolicyRule {
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

// istioctlIstioControlPlaneRules adds TokenRequest access istioctl needs to
// authenticate RPC calls to istiod (e.g. proxy-status). No Istio CR writes.
func istioctlIstioControlPlaneRules() []rbacv1.PolicyRule {
	return append(istioctlWorkloadRules(),
		rbacv1.PolicyRule{
			APIGroups: []string{""},
			Resources: []string{"serviceaccounts"},
			Verbs:     []string{"get"},
		},
		rbacv1.PolicyRule{
			APIGroups: []string{""},
			Resources: []string{"serviceaccounts/token"},
			Verbs:     []string{"create"},
		},
	)
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

	existing.Subjects = mergeSubjects(existing.Subjects, binding.Subjects)

	_, err = kube.RbacV1().RoleBindings(binding.Namespace).Update(ctx, existing, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("update RoleBinding %s/%s: %w", binding.Namespace, binding.Name, err)
	}

	return nil
}

func mergeSubjects(existing, add []rbacv1.Subject) []rbacv1.Subject {
	out := append([]rbacv1.Subject(nil), existing...)

	for _, subject := range add {
		if containsSubject(out, subject) {
			continue
		}

		out = append(out, subject)
	}

	return out
}

func containsSubject(subjects []rbacv1.Subject, want rbacv1.Subject) bool {
	for _, subject := range subjects {
		if subject.Kind == want.Kind &&
			subject.Name == want.Name &&
			subject.Namespace == want.Namespace &&
			subject.APIGroup == want.APIGroup {
			return true
		}
	}

	return false
}

func debugPodSelector() string {
	return fmt.Sprintf("%s=%s,%s=%s", labelAppName, resourceName, labelManagedBy, managedByD8)
}

func buildDebugPod(namespace, image string, command []string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: podGenerateName,
			Namespace:    namespace,
			Labels: map[string]string{
				labelAppName:   resourceName,
				labelManagedBy: managedByD8,
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
	if err := deleteTerminalDebugPods(ctx, kube, namespace); err != nil {
		return nil, err
	}

	pod, err := kube.CoreV1().Pods(namespace).Create(ctx, buildDebugPod(namespace, image, command), metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create debug pod in %s: %w", namespace, err)
	}

	return pod, nil
}

func deleteTerminalDebugPods(ctx context.Context, kube kubernetes.Interface, namespace string) error {
	list, err := kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: debugPodSelector()})
	if err != nil {
		return fmt.Errorf("list leftover debug pods in %s: %w", namespace, err)
	}

	for i := range list.Items {
		pod := &list.Items[i]
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			continue
		}

		fmt.Fprintf(os.Stderr, "Deleting leftover debug pod %s/%s\n", namespace, pod.Name)

		if delErr := deletePod(ctx, kube, namespace, pod.Name, pod.UID); delErr != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to delete leftover debug pod %s/%s: %v\n", namespace, pod.Name, delErr)
		}
	}

	return nil
}

func deletePod(ctx context.Context, kube kubernetes.Interface, namespace, name string, uid types.UID) error {
	opts := metav1.DeleteOptions{GracePeriodSeconds: ptr.To(podDeleteGraceSec)}
	if uid != "" {
		opts.Preconditions = &metav1.Preconditions{UID: &uid}
	}

	err := kube.CoreV1().Pods(namespace).Delete(ctx, name, opts)
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}

	return err
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
