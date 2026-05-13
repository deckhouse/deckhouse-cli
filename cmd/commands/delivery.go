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

package commands

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/werf/common-go/pkg/graceful"
	werfroot "github.com/werf/werf/v2/cmd/werf/root"
	"github.com/werf/werf/v2/pkg/logging"
	"github.com/werf/werf/v2/pkg/storage/synchronization/server"
)

func NewDeliveryCommand() (*cobra.Command, context.Context) {
	server.DefaultAddress = "https://delivery-sync.deckhouse.ru"

	terminationCtx := graceful.WithTermination(context.Background())
	defer graceful.Shutdown(terminationCtx, onShutdown)

	ctx := logging.WithLogger(terminationCtx)

	const werfAlias = "dk"

	if err := setWerfSelfInvocationCommand(werfAlias); err != nil {
		graceful.Terminate(ctx, err, 1)
		return nil, ctx
	}

	werfRootCmd, err := werfroot.ConstructRootCmd(ctx)
	if err != nil {
		graceful.Terminate(ctx, err, 1)
		return nil, ctx
	}

	werfRootCmd.Use = "delivery-kit"
	werfRootCmd.Aliases = []string{werfAlias}
	werfRootCmd = ReplaceCommandName("werf", fmt.Sprintf("d8 %s", werfAlias), werfRootCmd)
	werfRootCmd.Short = "A set of tools for building, distributing, and deploying containerized applications"
	werfRootCmd.Long = werfRootCmd.Short + "."
	werfRootCmd.Long += `

LICENSE NOTE: The Deckhouse Delivery Kit functionality is exclusively available to users holding a valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2026`

	removeKubectlCmd(werfRootCmd)

	return werfRootCmd, ctx
}

// setWerfSelfInvocationCommand sets environment variables to ensure werf knows how to call itself
// (e.g., for "host cleanup" commands in the background).
// It sets WERF_SELF_INVOCATION_COMMAND to the provided subcommand (e.g., "dk"),
// while werf will detect the executable path automatically.
func setWerfSelfInvocationCommand(subcommand string) error {
	if err := os.Setenv("WERF_SELF_INVOCATION_COMMAND", subcommand); err != nil {
		return fmt.Errorf("cannot set WERF_SELF_INVOCATION_COMMAND: %w", err)
	}

	return nil
}

func removeKubectlCmd(werfRootCmd *cobra.Command) {
	kubectlCmd, _, err := werfRootCmd.Find([]string{"kubectl"})
	if err != nil {
		return
	}

	kubectlCmd.Hidden = true

	for _, cmd := range kubectlCmd.Commands() {
		kubectlCmd.RemoveCommand(cmd)
	}

	werfRootCmd.RemoveCommand(kubectlCmd)
}

func onShutdown(_ context.Context, desc graceful.TerminationDescriptor) {
	if desc.Signal() != nil {
		logging.Default(fmt.Sprintf("Signal: %s", desc.Signal()))
		os.Exit(desc.ExitCode())
	} else if desc.Err() != nil {
		logging.Error(desc.Err().Error())
		os.Exit(desc.ExitCode())
	}
}
