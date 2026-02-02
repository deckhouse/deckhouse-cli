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
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/deckhouse-cli/internal/cni/api/v1alpha1"
	saferequest "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// RunSwitch executes the logic for the 'cni-migration switch' command.
func RunSwitch(targetCNI string) error {
	// Ask for user confirmation
	confirmed, err := AskForConfirmation("switch")
	if err != nil {
		return fmt.Errorf("asking for confirmation: %w", err)
	}
	if !confirmed {
		fmt.Println("Operation cancelled by user")
		return ErrCancelled
	}

	fmt.Printf("🚀 Starting CNI switch for target '%s'\n", targetCNI)

	// Create a Kubernetes client
	safeClient, err := saferequest.NewSafeClient()
	if err != nil {
		return fmt.Errorf("creating safe client: %w", err)
	}

	rtClient, err := safeClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return fmt.Errorf("creating runtime client: %w", err)
	}

	// Check for existing migration
	existingMigration, err := FindActiveMigration(context.Background(), rtClient)
	if err != nil {
		return fmt.Errorf("checking for existing migration: %w", err)
	}
	if existingMigration != nil {
		return fmt.Errorf("a CNI migration (%s) is already in progress. "+
			"Please use 'd8 cni-migration watch' to monitor it or 'd8 cni-migration cleanup' to abort it",
			existingMigration.Name)
	}

	// Create the CNIMigration resource
	migrationName := fmt.Sprintf("cni-migration-%s", time.Now().Format("20060102-150405"))
	newMigration := &v1alpha1.CNIMigration{
		ObjectMeta: metav1.ObjectMeta{
			Name: migrationName,
		},
		Spec: v1alpha1.CNIMigrationSpec{
			TargetCNI: targetCNI,
		},
	}

	if err := rtClient.Create(context.Background(), newMigration); err != nil {
		if k8serrors.IsAlreadyExists(err) {
			fmt.Printf("🔎 Migration '%s' already exists\n", migrationName)
		} else {
			return fmt.Errorf("creating CNIMigration: %w", err)
		}
	} else {
		fmt.Printf("✅ CNIMigration '%s' created\n", migrationName)
	}

	fmt.Println("The migration is now being handled automatically by the cluster")

	return nil
}
