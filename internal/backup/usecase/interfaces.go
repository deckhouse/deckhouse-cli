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

package usecase

import (
	"context"
	"io"

	"github.com/deckhouse/deckhouse-cli/internal/backup/domain"
)

// K8sClient provides Kubernetes operations for backup
type K8sClient interface {
	// ListPods lists pods by namespace and label selector
	ListPods(ctx context.Context, namespace, labelSelector string) ([]domain.PodInfo, error)
	// GetPod gets a single pod by name
	GetPod(ctx context.Context, namespace, name string) (*domain.PodInfo, error)
	// ExecInPod executes a command in a pod
	ExecInPod(ctx context.Context, namespace, podName, container string, command []string, stdout, stderr io.Writer) error
	// GetSecret gets a secret by name
	GetSecret(ctx context.Context, namespace, name string) (map[string][]byte, error)
	// ListNamespaces lists all namespaces
	ListNamespaces(ctx context.Context) ([]string, error)
	// ListSecrets lists secrets in namespaces
	ListSecrets(ctx context.Context, namespaces []string) ([]domain.K8sObject, error)
	// ListConfigMaps lists configmaps in namespaces
	ListConfigMaps(ctx context.Context, namespaces []string) ([]domain.K8sObject, error)
	// ListCustomResources lists custom resources
	ListCustomResources(ctx context.Context) ([]domain.K8sObject, error)
	// ListClusterRoles lists cluster roles
	ListClusterRoles(ctx context.Context) ([]domain.K8sObject, error)
	// ListClusterRoleBindings lists cluster role bindings
	ListClusterRoleBindings(ctx context.Context) ([]domain.K8sObject, error)
	// ListStorageClasses lists storage classes
	ListStorageClasses(ctx context.Context) ([]domain.K8sObject, error)
}

// TarballWriter writes objects to a tarball
type TarballWriter interface {
	PutObject(obj domain.K8sObject) error
	Close() error
}

// Logger provides logging capabilities
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

