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

	"github.com/deckhouse/deckhouse-cli/internal/data/usecase"
)

// DeleteUseCase handles DataExport deletion
type DeleteUseCase struct {
	repo   usecase.DataExportRepository
	logger usecase.Logger
}

// NewDeleteUseCase creates a new DeleteUseCase
func NewDeleteUseCase(repo usecase.DataExportRepository, logger usecase.Logger) *DeleteUseCase {
	return &DeleteUseCase{
		repo:   repo,
		logger: logger,
	}
}

// DeleteParams contains parameters for deleting a DataExport
type DeleteParams struct {
	Name      string
	Namespace string
}

// Execute deletes a DataExport
func (uc *DeleteUseCase) Execute(ctx context.Context, params *DeleteParams) error {
	if err := uc.repo.Delete(ctx, params.Name, params.Namespace); err != nil {
		return fmt.Errorf("delete DataExport: %w", err)
	}

	uc.logger.Info("DataExport deleted",
		"name", params.Name,
		"namespace", params.Namespace,
	)

	return nil
}

