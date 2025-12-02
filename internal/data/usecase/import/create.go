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

package dataimport

import (
	"context"
	"fmt"

	"github.com/deckhouse/deckhouse-cli/internal/data/domain"
	"github.com/deckhouse/deckhouse-cli/internal/data/usecase"
)

// CreateUseCase handles DataImport creation
type CreateUseCase struct {
	repo   usecase.DataImportRepository
	logger usecase.Logger
}

// NewCreateUseCase creates a new CreateUseCase
func NewCreateUseCase(repo usecase.DataImportRepository, logger usecase.Logger) *CreateUseCase {
	return &CreateUseCase{
		repo:   repo,
		logger: logger,
	}
}

// CreateParams contains parameters for creating a DataImport
type CreateParams struct {
	Name      string
	Namespace string
	TTL       string
	Publish   bool
	WFFC      bool
	PVCSpec   *domain.PVCSpec
}

// Execute creates a new DataImport
func (uc *CreateUseCase) Execute(ctx context.Context, params *CreateParams) error {
	ttl := params.TTL
	if ttl == "" {
		ttl = domain.DefaultTTL
	}

	createParams := &domain.CreateImportParams{
		Name:      params.Name,
		Namespace: params.Namespace,
		TTL:       ttl,
		Publish:   params.Publish,
		WFFC:      params.WFFC,
		PVCSpec:   params.PVCSpec,
	}

	if err := uc.repo.Create(ctx, createParams); err != nil {
		return fmt.Errorf("create DataImport: %w", err)
	}

	uc.logger.Info("DataImport created",
		"name", params.Name,
		"namespace", params.Namespace,
	)

	return nil
}

