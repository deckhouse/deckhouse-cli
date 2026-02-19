//go:build e2e

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

package mirror

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/testing/e2e/mirror/internal"
)

func TestModulesE2E(t *testing.T) {
	cfg := internal.GetConfig()

	if !cfg.HasSourceAuth() {
		t.Skip("Skipping: no source authentication configured (set E2E_LICENSE_TOKEN)")
	}

	cfg.NoPlatform = true
	cfg.NoSecurity = true

	env := setupTestEnvironment(t, cfg)
	defer env.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), internal.ModulesTestTimeout)
	defer cancel()

	runModulesTest(t, ctx, cfg, env)
}

func runModulesTest(t *testing.T, ctx context.Context, cfg *internal.Config, env *testEnv) {
	internal.PrintHeader("MODULES E2E TEST")

	internal.PrintStep(1, "Reading expected modules from source")
	expectedModules := readExpectedModules(t, ctx, cfg)

	internal.PrintStep(2, "Pulling modules")
	runPullStep(t, cfg, env)

	internal.PrintStep(3, "Pushing to target registry")
	runPushStep(t, cfg, env)

	internal.PrintStep(4, "Verifying modules in target")
	verifyModulesInTarget(t, ctx, cfg, env, expectedModules)

	fmt.Printf("\n✅ Modules test passed: %d modules\n", len(expectedModules))
}

func readExpectedModules(t *testing.T, ctx context.Context, cfg *internal.Config) []string {
	t.Helper()

	reader := createSourceReader(t, cfg)
	modules, err := reader.ReadModulesList(ctx)
	require.NoError(t, err, "Failed to read modules list")

	modules = filterModules(modules, cfg.IncludeModules)

	t.Logf("Expected %d modules: %v", len(modules), modules)
	return modules
}

func verifyModulesInTarget(t *testing.T, ctx context.Context, cfg *internal.Config, env *testEnv, expectedModules []string) {
	t.Helper()
	verifyModulesImages(t, ctx, cfg, env, expectedModules)
}
