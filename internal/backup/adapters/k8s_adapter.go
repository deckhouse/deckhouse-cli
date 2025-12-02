/*
Copyright 2024 Flant JSC

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

package adapters

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/backup/domain"
	"github.com/deckhouse/deckhouse-cli/internal/backup/usecase"
)

// K8sObjectWrapper wraps runtime.Object to implement domain.K8sObject
type K8sObjectWrapper struct {
	obj runtime.Object
}

// NewK8sObjectWrapper creates a new wrapper
func NewK8sObjectWrapper(obj runtime.Object) *K8sObjectWrapper {
	return &K8sObjectWrapper{obj: obj}
}

func (w *K8sObjectWrapper) GetName() string {
	if accessor, ok := w.obj.(metav1.Object); ok {
		return accessor.GetName()
	}
	return ""
}

func (w *K8sObjectWrapper) GetNamespace() string {
	if accessor, ok := w.obj.(metav1.Object); ok {
		return accessor.GetNamespace()
	}
	return ""
}

func (w *K8sObjectWrapper) GetKind() string {
	return w.obj.GetObjectKind().GroupVersionKind().Kind
}

func (w *K8sObjectWrapper) GetAPIVersion() string {
	return w.obj.GetObjectKind().GroupVersionKind().GroupVersion().String()
}

func (w *K8sObjectWrapper) MarshalYAML() ([]byte, error) {
	// Clear managed fields before serialization
	if accessor, ok := w.obj.(metav1.Object); ok {
		accessor.SetManagedFields(nil)
	}
	return yaml.Marshal(w.obj)
}

// Unwrap returns the underlying runtime.Object
func (w *K8sObjectWrapper) Unwrap() runtime.Object {
	return w.obj
}

// Compile-time check
var _ domain.K8sObject = (*K8sObjectWrapper)(nil)

// K8sClientAdapter adapts kubernetes.Clientset to usecase.K8sClient
type K8sClientAdapter struct {
	clientset  *kubernetes.Clientset
	restConfig *rest.Config
	dynamicCl  dynamic.Interface
}

// NewK8sClientAdapter creates a new K8sClientAdapter
func NewK8sClientAdapter(clientset *kubernetes.Clientset, restConfig *rest.Config) *K8sClientAdapter {
	return &K8sClientAdapter{
		clientset:  clientset,
		restConfig: restConfig,
		dynamicCl:  dynamic.New(clientset.RESTClient()),
	}
}

func (a *K8sClientAdapter) ListPods(ctx context.Context, namespace, labelSelector string) ([]domain.PodInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	pods, err := a.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, err
	}

	return lo.Map(pods.Items, func(pod corev1.Pod, _ int) domain.PodInfo {
		ready := lo.FindOrElse(pod.Status.Conditions, corev1.PodCondition{}, func(c corev1.PodCondition) bool {
			return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue
		}).Status == corev1.ConditionTrue

		containers := lo.Map(pod.Spec.Containers, func(c corev1.Container, _ int) string {
			return c.Name
		})

		return domain.PodInfo{
			Name:       pod.Name,
			Namespace:  pod.Namespace,
			Ready:      ready,
			Containers: containers,
		}
	}), nil
}

func (a *K8sClientAdapter) GetPod(ctx context.Context, namespace, name string) (*domain.PodInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	pod, err := a.clientset.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	ready := lo.FindOrElse(pod.Status.Conditions, corev1.PodCondition{}, func(c corev1.PodCondition) bool {
		return c.Type == corev1.PodReady
	}).Status == corev1.ConditionTrue

	containers := lo.Map(pod.Spec.Containers, func(c corev1.Container, _ int) string {
		return c.Name
	})

	return &domain.PodInfo{
		Name:       pod.Name,
		Namespace:  pod.Namespace,
		Ready:      ready,
		Containers: containers,
	}, nil
}

func (a *K8sClientAdapter) ExecInPod(ctx context.Context, namespace, podName, container string, command []string, stdout, stderr io.Writer) error {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("add to scheme: %w", err)
	}
	parameterCodec := runtime.NewParameterCodec(scheme)

	execOpts := &corev1.PodExecOptions{
		Stdout:    true,
		Stderr:    true,
		Container: container,
		Command:   command,
	}

	request := a.clientset.CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		SubResource("exec").
		VersionedParams(execOpts, parameterCodec).
		Namespace(namespace).
		Name(podName)

	executor, err := remotecommand.NewSPDYExecutor(a.restConfig, "POST", request.URL())
	if err != nil {
		return fmt.Errorf("create SPDY executor: %w", err)
	}

	return executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: stdout,
		Stderr: stderr,
	})
}

func (a *K8sClientAdapter) GetSecret(ctx context.Context, namespace, name string) (map[string][]byte, error) {
	secret, err := a.clientset.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return secret.Data, nil
}

func (a *K8sClientAdapter) ListNamespaces(ctx context.Context) ([]string, error) {
	nsList, err := a.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return lo.Map(nsList.Items, func(ns corev1.Namespace, _ int) string {
		return ns.Name
	}), nil
}

func (a *K8sClientAdapter) ListSecrets(ctx context.Context, namespaces []string) ([]domain.K8sObject, error) {
	var result []domain.K8sObject
	for _, ns := range namespaces {
		secrets, err := a.clientset.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list secrets in %s: %w", ns, err)
		}
		for i := range secrets.Items {
			result = append(result, NewK8sObjectWrapper(&secrets.Items[i]))
		}
	}
	return result, nil
}

func (a *K8sClientAdapter) ListConfigMaps(ctx context.Context, namespaces []string) ([]domain.K8sObject, error) {
	var result []domain.K8sObject
	for _, ns := range namespaces {
		cms, err := a.clientset.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list configmaps in %s: %w", ns, err)
		}
		for i := range cms.Items {
			result = append(result, NewK8sObjectWrapper(&cms.Items[i]))
		}
	}
	return result, nil
}

func (a *K8sClientAdapter) ListCustomResources(ctx context.Context) ([]domain.K8sObject, error) {
	// This would need to use dynamic client to list CRDs
	// For now, return empty - the original logic is in crds package
	return nil, nil
}

func (a *K8sClientAdapter) ListClusterRoles(ctx context.Context) ([]domain.K8sObject, error) {
	roles, err := a.clientset.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	result := make([]domain.K8sObject, len(roles.Items))
	for i := range roles.Items {
		result[i] = NewK8sObjectWrapper(&roles.Items[i])
	}
	return result, nil
}

func (a *K8sClientAdapter) ListClusterRoleBindings(ctx context.Context) ([]domain.K8sObject, error) {
	bindings, err := a.clientset.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	result := make([]domain.K8sObject, len(bindings.Items))
	for i := range bindings.Items {
		result[i] = NewK8sObjectWrapper(&bindings.Items[i])
	}
	return result, nil
}

func (a *K8sClientAdapter) ListStorageClasses(ctx context.Context) ([]domain.K8sObject, error) {
	scs, err := a.clientset.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	result := make([]domain.K8sObject, len(scs.Items))
	for i := range scs.Items {
		result[i] = NewK8sObjectWrapper(&scs.Items[i])
	}
	return result, nil
}

// Compile-time checks
var _ usecase.K8sClient = (*K8sClientAdapter)(nil)

