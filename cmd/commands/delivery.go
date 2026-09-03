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

// DeliveryKitCommandName is the top-level command name for the built-in werf
// re-skin. A plugin contract uses this same name to depend on delivery-kit
// while it ships as a built-in command rather than a standalone plugin.
const DeliveryKitCommandName = "delivery-kit"

// NewRootContext returns the process-wide graceful-termination context that the
// whole d8 command tree runs under: it installs the SIGINT/SIGTERM handler that
// Execute's graceful.Terminate and telemetry shutdown depend on, and carries the
// werf logger.
//
// It is separate from NewDeliveryCommand because the context is root
// infrastructure, still required when an installed plugin serves `delivery-kit`
// and the built-in werf tree is never constructed.
func NewRootContext() context.Context {
	return logging.WithLogger(graceful.WithTermination(context.Background()))
}

// NewDeliveryCommand builds the built-in werf re-skin under the root context.
// It returns nil only on a path that has already asked for termination, which
// onShutdown turns into a process exit before the caller sees the nil.
func NewDeliveryCommand(ctx context.Context) *cobra.Command {
	server.DefaultAddress = "https://delivery-sync.deckhouse.ru"

	// Drains a termination requested below - or a signal caught while werf builds
	// its command tree - into the os.Exit that onShutdown performs.
	defer graceful.Shutdown(ctx, onShutdown)

	const werfAlias = "dk"

	if err := setWerfSelfInvocationCommand(werfAlias); err != nil {
		graceful.Terminate(ctx, err, 1)
		return nil
	}

	werfRootCmd, err := werfroot.ConstructRootCmd(ctx)
	if err != nil {
		graceful.Terminate(ctx, err, 1)
		return nil
	}

	werfRootCmd.Use = DeliveryKitCommandName
	werfRootCmd.Aliases = []string{werfAlias}
	werfRootCmd = ReplaceCommandName("werf", fmt.Sprintf("d8 %s", werfAlias), werfRootCmd)
	werfRootCmd.Short = "A set of tools for building, distributing, and deploying containerized applications"
	werfRootCmd.Long = werfRootCmd.Short + "."
	werfRootCmd.Long += `

LICENSE NOTE: The Deckhouse Delivery Kit functionality is exclusively available to users holding a valid license for any commercial version of the Deckhouse Kubernetes Platform.

© Flant JSC 2026`

	removeKubectlCmd(werfRootCmd)

	return werfRootCmd
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
