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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/data/domain"
	"github.com/deckhouse/deckhouse-cli/internal/data/usecase"
)

const (
	maxRetryAttempts = 60
	retryInterval    = 3 * time.Second
)

// Compile-time check that DataExportRepository implements usecase.DataExportRepository
var _ usecase.DataExportRepository = (*DataExportRepository)(nil)

// DataExportRepository implements usecase.DataExportRepository using K8s client
type DataExportRepository struct {
	client ctrlrtclient.Client
}

// NewDataExportRepository creates a new DataExportRepository
func NewDataExportRepository(client ctrlrtclient.Client) *DataExportRepository {
	return &DataExportRepository{client: client}
}

func (r *DataExportRepository) Create(ctx context.Context, params *domain.CreateExportParams) error {
	ttl := params.TTL
	if ttl == "" {
		ttl = domain.DefaultTTL
	}

	obj := &v1alpha1.DataExport{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "deckhouse.io/v1alpha1",
			Kind:       "DataExport",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      params.Name,
			Namespace: params.Namespace,
		},
		Spec: v1alpha1.DataexportSpec{
			TTL:     ttl,
			Publish: params.Publish,
			TargetRef: v1alpha1.TargetRefSpec{
				Kind: string(params.VolumeKind),
				Name: params.VolumeName,
			},
		},
	}

	if err := r.client.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create DataExport: %w", err)
	}

	return nil
}

func (r *DataExportRepository) Get(ctx context.Context, name, namespace string) (*domain.DataExport, error) {
	obj := &v1alpha1.DataExport{}
	if err := r.client.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
		return nil, fmt.Errorf("get DataExport: %w", err)
	}

	return r.toDomain(obj), nil
}

func (r *DataExportRepository) GetWithRetry(ctx context.Context, name, namespace string) (*domain.DataExport, error) {
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		obj := &v1alpha1.DataExport{}
		if err := r.client.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
			return nil, fmt.Errorf("get DataExport: %w", err)
		}

		// Check if expired and recreate
		for _, condition := range obj.Status.Conditions {
			if condition.Type == "Expired" && condition.Status == "True" {
				// Delete and recreate
				if err := r.Delete(ctx, name, namespace); err != nil {
					return nil, err
				}
				createParams := &domain.CreateExportParams{
					Name:       name,
					Namespace:  namespace,
					TTL:        obj.Spec.TTL,
					VolumeKind: domain.VolumeKind(obj.Spec.TargetRef.Kind),
					VolumeName: obj.Spec.TargetRef.Name,
					Publish:    obj.Spec.Publish,
				}
				if err := r.Create(ctx, createParams); err != nil {
					return nil, err
				}
				continue
			}
		}

		// Check if ready
		export := r.toDomain(obj)
		if !export.Status.Ready {
			if i >= maxRetryAttempts {
				return nil, fmt.Errorf("DataExport %s/%s is not ready after %d attempts", namespace, name, maxRetryAttempts)
			}
			time.Sleep(retryInterval)
			continue
		}

		// Check URL
		if !obj.Spec.Publish && obj.Status.URL == "" {
			if i >= maxRetryAttempts {
				return nil, fmt.Errorf("DataExport %s/%s has no URL", namespace, name)
			}
			time.Sleep(retryInterval)
			continue
		}
		if obj.Spec.Publish && obj.Status.PublicURL == "" {
			if i >= maxRetryAttempts {
				return nil, fmt.Errorf("DataExport %s/%s has no PublicURL", namespace, name)
			}
			time.Sleep(retryInterval)
			continue
		}

		return export, nil
	}
}

func (r *DataExportRepository) Delete(ctx context.Context, name, namespace string) error {
	obj := &v1alpha1.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	return r.client.Delete(ctx, obj)
}

func (r *DataExportRepository) toDomain(obj *v1alpha1.DataExport) *domain.DataExport {
	ready := false
	expired := false
	for _, condition := range obj.Status.Conditions {
		if condition.Type == "Ready" && condition.Status == "True" {
			ready = true
		}
		if condition.Type == "Expired" && condition.Status == "True" {
			expired = true
		}
	}

	return &domain.DataExport{
		Name:      obj.Name,
		Namespace: obj.Namespace,
		TTL:       obj.Spec.TTL,
		Publish:   obj.Spec.Publish,
		TargetRef: domain.VolumeRef{
			Kind: domain.VolumeKind(obj.Spec.TargetRef.Kind),
			Name: obj.Spec.TargetRef.Name,
		},
		Status: domain.DataExportStatus{
			URL:        obj.Status.URL,
			PublicURL:  obj.Status.PublicURL,
			CA:         obj.Status.CA,
			VolumeMode: domain.VolumeMode(obj.Status.VolumeMode),
			Ready:      ready,
			Expired:    expired,
		},
	}
}

