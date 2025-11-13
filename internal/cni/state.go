/*
Copyright 2025 Flant JSC

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

package cni

import (
	"context"
	"fmt"

	"github.com/deckhouse/deckhouse-cli/internal/cni/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FindActiveMigration searches for a CNIMigration resource that is not in a terminal state.
// It returns an error if more than one active migration is found.
func FindActiveMigration(ctx context.Context, rtClient client.Client) (*v1alpha1.CNIMigration, error) {
	migrationList := &v1alpha1.CNIMigrationList{}
	if err := rtClient.List(ctx, migrationList); err != nil {
		return nil, fmt.Errorf("listing CNIMigration objects: %w", err)
	}

	activeMigrations := make([]v1alpha1.CNIMigration, 0)

	for _, migration := range migrationList.Items {
		isTerminal := false
		for _, cond := range migration.Status.Conditions {
			if cond.Type == "Succeeded" && cond.Status == metav1.ConditionTrue {
				isTerminal = true
				break
			}
		}
		if migration.Status.ObservedPhase == "Failed" { // TODO: will it be like that?
			isTerminal = true
		}

		if !isTerminal {
			activeMigrations = append(activeMigrations, migration)
		}
	}

	if len(activeMigrations) == 0 {
		return nil, nil // No active migration found
	}

	if len(activeMigrations) > 1 {
		return nil, fmt.Errorf(
			"found %d active CNI migrations, which is an inconsistent state. "+
				"Please run 'd8 cni-switch cleanup' to resolve this",
			len(activeMigrations),
		)
	}

	return &activeMigrations[0], nil
}
