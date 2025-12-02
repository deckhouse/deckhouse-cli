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

package export

import (
	"context"
	"fmt"

	"github.com/deckhouse/deckhouse-cli/internal/data/domain"
	"github.com/deckhouse/deckhouse-cli/internal/data/usecase"
)

// CreateUseCase handles DataExport creation
type CreateUseCase struct {
	repo   usecase.DataExportRepository
	logger usecase.Logger
}

// NewCreateUseCase creates a new CreateUseCase
func NewCreateUseCase(repo usecase.DataExportRepository, logger usecase.Logger) *CreateUseCase {
	return &CreateUseCase{
		repo:   repo,
		logger: logger,
	}
}

// CreateParams contains parameters for creating a DataExport
type CreateParams struct {
	Name       string
	Namespace  string
	TTL        string
	VolumeRef  string // e.g., "pvc/my-volume"
	Publish    bool
}

// Execute creates a new DataExport
func (uc *CreateUseCase) Execute(ctx context.Context, params *CreateParams) error {
	// Parse volume reference
	volRef, err := domain.ParseVolumeRef(params.VolumeRef)
	if err != nil {
		return fmt.Errorf("parse volume reference: %w", err)
	}

	// Set default TTL
	ttl := params.TTL
	if ttl == "" {
		ttl = domain.DefaultTTL
	}

	// Create DataExport
	createParams := &domain.CreateExportParams{
		Name:       params.Name,
		Namespace:  params.Namespace,
		TTL:        ttl,
		VolumeKind: volRef.Kind,
		VolumeName: volRef.Name,
		Publish:    params.Publish,
	}

	if err := uc.repo.Create(ctx, createParams); err != nil {
		return fmt.Errorf("create DataExport: %w", err)
	}

	uc.logger.Info("DataExport created",
		"name", params.Name,
		"namespace", params.Namespace,
	)

	return nil
}

