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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/data/domain"
	"github.com/deckhouse/deckhouse-cli/internal/data/usecase"
)

// Compile-time check that DataImportRepository implements usecase.DataImportRepository
var _ usecase.DataImportRepository = (*DataImportRepository)(nil)

// DataImportRepository implements usecase.DataImportRepository using K8s client
type DataImportRepository struct {
	client ctrlrtclient.Client
}

// NewDataImportRepository creates a new DataImportRepository
func NewDataImportRepository(client ctrlrtclient.Client) *DataImportRepository {
	return &DataImportRepository{client: client}
}

func (r *DataImportRepository) Create(ctx context.Context, params *domain.CreateImportParams) error {
	ttl := params.TTL
	if ttl == "" {
		ttl = domain.DefaultTTL
	}

	var pvcTemplate *v1alpha1.PersistentVolumeClaimTemplateSpec
	if params.PVCSpec != nil {
		pvcTemplate = r.buildPVCTemplate(params.PVCSpec)
	}

	obj := &v1alpha1.DataImport{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "DataImport",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      params.Name,
			Namespace: params.Namespace,
		},
		Spec: v1alpha1.DataImportSpec{
			TTL:                  ttl,
			Publish:              params.Publish,
			WaitForFirstConsumer: params.WFFC,
			TargetRef: v1alpha1.DataImportTargetRefSpec{
				Kind:        "PersistentVolumeClaim",
				PvcTemplate: pvcTemplate,
			},
		},
	}

	if err := r.client.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create DataImport: %w", err)
	}

	return nil
}

func (r *DataImportRepository) Get(ctx context.Context, name, namespace string) (*domain.DataImport, error) {
	obj := &v1alpha1.DataImport{}
	if err := r.client.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
		return nil, fmt.Errorf("get DataImport: %w", err)
	}

	return r.toDomain(obj), nil
}

func (r *DataImportRepository) GetWithRetry(ctx context.Context, name, namespace string) (*domain.DataImport, error) {
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		obj := &v1alpha1.DataImport{}
		if err := r.client.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
			return nil, fmt.Errorf("get DataImport: %w", err)
		}

		// Check if expired and recreate
		for _, condition := range obj.Status.Conditions {
			if condition.Type == "Expired" && condition.Status == "True" {
				if err := r.Delete(ctx, name, namespace); err != nil {
					return nil, err
				}
				var pvcSpec *domain.PVCSpec
				if obj.Spec.TargetRef.PvcTemplate != nil {
					pvcSpec = r.pvcTemplateToSpec(obj.Spec.TargetRef.PvcTemplate)
				}
				createParams := &domain.CreateImportParams{
					Name:      name,
					Namespace: namespace,
					TTL:       obj.Spec.TTL,
					Publish:   obj.Spec.Publish,
					WFFC:      obj.Spec.WaitForFirstConsumer,
					PVCSpec:   pvcSpec,
				}
				if err := r.Create(ctx, createParams); err != nil {
					return nil, err
				}
				continue
			}
		}

		// Check if ready
		dataImport := r.toDomain(obj)
		if !dataImport.Status.Ready {
			if i >= maxRetryAttempts {
				return nil, fmt.Errorf("DataImport %s/%s is not ready after %d attempts", namespace, name, maxRetryAttempts)
			}
			time.Sleep(retryInterval)
			continue
		}

		// Check URL
		if !obj.Spec.Publish && obj.Status.URL == "" {
			if i >= maxRetryAttempts {
				return nil, fmt.Errorf("DataImport %s/%s has no URL", namespace, name)
			}
			time.Sleep(retryInterval)
			continue
		}
		if obj.Spec.Publish && obj.Status.PublicURL == "" {
			if i >= maxRetryAttempts {
				return nil, fmt.Errorf("DataImport %s/%s has no PublicURL", namespace, name)
			}
			time.Sleep(retryInterval)
			continue
		}

		// Check VolumeMode
		if obj.Status.VolumeMode == "" {
			if i >= maxRetryAttempts {
				return nil, fmt.Errorf("DataImport %s/%s has no VolumeMode", namespace, name)
			}
			time.Sleep(retryInterval)
			continue
		}

		return dataImport, nil
	}
}

func (r *DataImportRepository) Delete(ctx context.Context, name, namespace string) error {
	obj := &v1alpha1.DataImport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	return r.client.Delete(ctx, obj)
}

func (r *DataImportRepository) buildPVCTemplate(spec *domain.PVCSpec) *v1alpha1.PersistentVolumeClaimTemplateSpec {
	if spec == nil {
		return nil
	}

	accessModes := make([]v1alpha1.PersistentVolumeAccessMode, len(spec.AccessModes))
	for i, mode := range spec.AccessModes {
		accessModes[i] = v1alpha1.PersistentVolumeAccessMode(mode)
	}

	storageClassName := &spec.StorageClassName

	result := &v1alpha1.PersistentVolumeClaimTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name,
			Namespace: spec.Namespace,
		},
		PersistentVolumeClaimSpec: v1alpha1.PersistentVolumeClaimSpec{
			AccessModes:      accessModes,
			StorageClassName: storageClassName,
		},
	}

	if spec.Storage != "" {
		quantity := resource.MustParse(spec.Storage)
		result.PersistentVolumeClaimSpec.Resources = v1alpha1.VolumeResourceRequirements{
			Requests: v1alpha1.ResourceList{
				v1alpha1.ResourceStorage: quantity,
			},
		}
	}

	return result
}

func (r *DataImportRepository) pvcTemplateToSpec(tpl *v1alpha1.PersistentVolumeClaimTemplateSpec) *domain.PVCSpec {
	if tpl == nil {
		return nil
	}

	accessModes := make([]string, len(tpl.PersistentVolumeClaimSpec.AccessModes))
	for i, mode := range tpl.PersistentVolumeClaimSpec.AccessModes {
		accessModes[i] = string(mode)
	}

	var storageClassName string
	if tpl.PersistentVolumeClaimSpec.StorageClassName != nil {
		storageClassName = *tpl.PersistentVolumeClaimSpec.StorageClassName
	}

	var storage string
	if requests := tpl.PersistentVolumeClaimSpec.Resources.Requests; requests != nil {
		if q, ok := requests[v1alpha1.ResourceStorage]; ok {
			storage = q.String()
		}
	}

	return &domain.PVCSpec{
		Name:             tpl.ObjectMeta.Name,
		Namespace:        tpl.ObjectMeta.Namespace,
		StorageClassName: storageClassName,
		AccessModes:      accessModes,
		Storage:          storage,
	}
}

func (r *DataImportRepository) toDomain(obj *v1alpha1.DataImport) *domain.DataImport {
	ready := false
	for _, condition := range obj.Status.Conditions {
		if condition.Type == "Ready" && condition.Status == "True" {
			ready = true
			break
		}
	}

	var pvcSpec *domain.PVCSpec
	if obj.Spec.TargetRef.PvcTemplate != nil {
		pvcSpec = r.pvcTemplateToSpec(obj.Spec.TargetRef.PvcTemplate)
	}

	return &domain.DataImport{
		Name:      obj.Name,
		Namespace: obj.Namespace,
		TTL:       obj.Spec.TTL,
		Publish:   obj.Spec.Publish,
		WFFC:      obj.Spec.WaitForFirstConsumer,
		PVCSpec:   pvcSpec,
		Status: domain.DataImportStatus{
			URL:        obj.Status.URL,
			PublicURL:  obj.Status.PublicURL,
			CA:         obj.Status.CA,
			VolumeMode: domain.VolumeMode(obj.Status.VolumeMode),
			Ready:      ready,
		},
	}
}

