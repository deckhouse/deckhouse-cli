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

	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/deckhouse/deckhouse-cli/internal/cni/api/v1alpha1"
	saferequest "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// RunCleanup executes the logic for the 'cni-migration cleanup' command.
func RunCleanup() error {
	ctx := context.Background()

	fmt.Println("🚀 Starting CNI switch cleanup")

	// Create a Kubernetes client
	safeClient, err := saferequest.NewSafeClient()
	if err != nil {
		return fmt.Errorf("creating safe client: %w", err)
	}

	rtClient, err := safeClient.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return fmt.Errorf("creating runtime client: %w", err)
	}

	// Find and delete all CNIMigration resources
	migrations := &v1alpha1.CNIMigrationList{}
	if err := rtClient.List(ctx, migrations); err != nil {
		return fmt.Errorf("listing CNIMigrations: %w", err)
	}

	if len(migrations.Items) == 0 {
		fmt.Println("✅ No active migrations found")
		return nil
	}

	for _, m := range migrations.Items {
		fmt.Printf("Deleting CNIMigration '%s'...", m.Name)
		if err := rtClient.Delete(ctx, &m); err != nil {
			if !errors.IsNotFound(err) {
				return fmt.Errorf("deleting CNIMigration %s: %w", m.Name, err)
			}
			fmt.Println(" already deleted")
		} else {
			fmt.Println(" done")
		}
	}

	fmt.Println("🎉 Cleanup triggered. The cluster-internal controllers will handle the rest")
	return nil
}
