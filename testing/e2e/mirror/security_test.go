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

func TestSecurityE2E(t *testing.T) {
	cfg := internal.GetConfig()

	if !cfg.HasSourceAuth() {
		t.Skip("Skipping: no source authentication configured (set E2E_LICENSE_TOKEN)")
	}

	cfg.NoModules = true
	cfg.NoPlatform = true

	env := setupTestEnvironment(t, cfg)
	defer env.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), internal.SecurityTestTimeout)
	defer cancel()

	runSecurityTest(t, ctx, cfg, env)
}

func runSecurityTest(t *testing.T, ctx context.Context, cfg *internal.Config, env *testEnv) {
	internal.PrintHeader("SECURITY E2E TEST")

	internal.PrintStep(1, "Reading expected security databases from source")
	expected := readExpectedSecurityImages(t, ctx, cfg)

	internal.PrintStep(2, "Pulling security databases")
	runPullStep(t, cfg, env)

	internal.PrintStep(3, "Pushing to target registry")
	runPushStep(t, cfg, env)

	internal.PrintStep(4, "Verifying security databases in target")
	verifySecurityInTarget(t, ctx, cfg, env, expected)

	totalTags := 0
	for _, tags := range expected.Databases {
		totalTags += len(tags)
	}
	fmt.Printf("\n✅ Security test passed: %d databases, %d tags\n", len(expected.Databases), totalTags)
}

func readExpectedSecurityImages(t *testing.T, ctx context.Context, cfg *internal.Config) *internal.SecurityDigests {
	t.Helper()

	reader := createSourceReader(t, cfg)
	security, err := reader.ReadSecurityDigests(ctx)
	require.NoError(t, err, "Failed to read security databases")

	t.Logf("Found %d security databases:", len(security.Databases))
	for db, tags := range security.Databases {
		t.Logf("  %s: %d tags", db, len(tags))
	}

	return security
}

func verifySecurityInTarget(t *testing.T, ctx context.Context, cfg *internal.Config, env *testEnv, expected *internal.SecurityDigests) {
	t.Helper()
	verifySecurityImages(t, ctx, cfg, env)
}

