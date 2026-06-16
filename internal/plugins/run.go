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

package plugins

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal"
	d8flags "github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

// pluginStopGracePeriod is how long a plugin gets to exit after SIGTERM
// (forwarded on d8's own cancellation) before it is killed.
const pluginStopGracePeriod = 10 * time.Second

// Contract env-var names of the d8<->plugin protocol. Declared once so the
// injection switch (pluginRunEnv) and the help text (ProvidesEnv callers in
// the command layer) cannot drift.
const (
	envKubeconfig       = "KUBECONFIG"
	envPluginsCaller    = "PLUGINS_CALLER"
	envModuleConfigInfo = "MODULE_CONFIG_INFO"
)

// ProvidesEnv reports whether d8 actually injects a value for a contract-requested
// env var (vs leaving it to pass through from the inherited environment). It is the
// single source of truth for both injection and help. MODULE_CONFIG_INFO is not yet
// provided (it needs a defined module mapping in the contract).
func ProvidesEnv(name string) bool {
	return name == envKubeconfig || name == envPluginsCaller
}

// RunInstalled ensures the plugin is installed, enforces its contract
// requirements, then execs its binary with args. stdin/stdout/stderr are inherited;
// the contract's requested env vars are injected.
func (m *Manager) RunInstalled(ctx context.Context, pluginName string, args []string) error {
	installed, err := m.checkInstalled(pluginName)
	if err != nil {
		return fmt.Errorf("check installed: %w", err)
	}

	if !installed {
		// The plugin source is needed only to install; initialize it lazily so an
		// already-installed plugin is exec'd without any registry/cluster round-trip.
		if err := m.InitPluginServices(ctx); err != nil {
			return fmt.Errorf("init plugin services: %w", err)
		}

		fmt.Println("Not installed, installing...")

		if err := m.InstallPlugin(ctx, pluginName); err != nil {
			return fmt.Errorf("install: %w", err)
		}

		fmt.Println("Installed successfully")
	}

	// The cached contract drives the pre-run requirement gate and env injection. A
	// genuinely absent contract is best-effort (run ungated); a present-but-corrupt
	// one is a hard error - failing open there would silently disable the gate.
	contract, err := m.InstalledPluginContract(pluginName)

	switch {
	case err == nil:
	case errors.Is(err, os.ErrNotExist):
		m.logger.Debug("no cached contract for plugin; running without requirement check or contract env",
			slog.String("plugin", pluginName))

		contract = nil
	default:
		return fmt.Errorf("read %q contract (reinstall with 'd8 plugins install %s --force'): %w",
			pluginName, pluginName, err)
	}

	// Requirement gate (ADR: check before run). Skipped for purely local args
	// (help/version/completion) so a plugin's own help/version stays readable offline
	// even when it declares cluster requirements.
	if contract != nil && !isLocalPluginInvocation(args) {
		if err := m.ensurePluginRequirements(ctx, contract); err != nil {
			return err
		}
	}

	absPath, err := filepath.Abs(layout.CurrentLinkPath(m.pluginDirectory, pluginName))
	if err != nil {
		return fmt.Errorf("absolute path: %w", err)
	}

	m.logger.Debug("Executing plugin", slog.String("plugin", pluginName), slog.Any("args", args))
	cmd := exec.CommandContext(ctx, absPath, args...)

	cmd.Env = m.pluginRunEnv(contract)
	cmd.Stdin = os.Stdin
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr

	// On ctx cancellation (d8's SIGINT/SIGTERM handling) forward SIGTERM and give
	// the plugin a grace period instead of the CommandContext default - an
	// immediate SIGKILL that would deny the plugin any cleanup. The terminal's own
	// Ctrl-C reaches the child anyway via the process group.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = pluginStopGracePeriod

	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() > 0 {
			// The plugin already reported its failure on the inherited stderr; do
			// not wrap it in another error line - propagate its exact exit code.
			os.Exit(exitErr.ExitCode())
		}

		return fmt.Errorf("plugin run: %w", err)
	}

	return nil
}

// ensurePluginRequirements enforces the plugin's contract requirements before it is
// launched (ADR: check before run, not only at install). It is a hard gate, like
// install: an unsatisfied Kubernetes/Deckhouse/module/plugin requirement - or an
// unreachable cluster for a plugin that declares cluster requirements - blocks the
// run. The wrapper forwards flags to the plugin, so the override here is the env var
// D8_PLUGINS_SKIP_CLUSTER_CHECKS=1 (surfaced in the cluster-unreachable error).
func (m *Manager) ensurePluginRequirements(ctx context.Context, contract *internal.Plugin) error {
	failed, err := m.validateRequirements(ctx, contract)
	if err != nil {
		return err
	}

	if len(failed) == 0 {
		return nil
	}

	return failed.helpfulError(fmt.Sprintf("plugin %q requirements not satisfied", contract.Name), false)
}

// isLocalPluginInvocation reports whether the forwarded args are a purely local
// query (help/version/completion) that needs no cluster, so the requirement gate is
// skipped and a plugin's own help/version stays available offline.
func isLocalPluginInvocation(args []string) bool {
	if len(args) == 0 {
		return false
	}

	switch args[0] {
	case "--help", "-h", "--version", "-v", "help", "completion",
		cobra.ShellCompRequestCmd, cobra.ShellCompNoDescRequestCmd:
		return true
	}

	// `--help/-h` after a subcommand (e.g. `d8 system status --help`) is still a
	// help query; past a literal `--` it is plugin payload, not a flag.
	for _, arg := range args {
		if arg == "--" {
			break
		}

		if arg == "--help" || arg == "-h" {
			return true
		}
	}

	return false
}

// pluginRunEnv returns the environment for the plugin process: the inherited
// environment plus the variables the contract asks d8 to provide. KUBECONFIG (the
// path d8 itself uses) and PLUGINS_CALLER (d8's own executable path) are injected;
// MODULE_CONFIG_INFO is not yet supported (it needs a defined module mapping in the
// contract). Unrecognized requested vars are left to pass through from the parent.
func (m *Manager) pluginRunEnv(contract *internal.Plugin) []string {
	env := os.Environ()
	if contract == nil {
		return env
	}

	for _, want := range contract.Env {
		switch want.Name {
		case envKubeconfig:
			env = append(env, envKubeconfig+"="+d8flags.Kubeconfig)
		case envPluginsCaller:
			exe, err := os.Executable()
			if err != nil {
				m.logger.Debug("cannot resolve PLUGINS_CALLER", slog.String("plugin", contract.Name), slog.String("error", err.Error()))

				continue
			}

			env = append(env, envPluginsCaller+"="+exe)
		case envModuleConfigInfo:
			// Declared in the protocol but not provided yet (needs a module mapping in
			// the contract); passes through from the inherited environment if set.
			m.logger.Debug("contract env var deferred, passed through if set",
				slog.String("plugin", contract.Name), slog.String("env", want.Name))
		default:
			// Unrecognized requested var, not part of the d8<->plugin protocol; passes
			// through from the inherited environment if set. The help marks non-injected
			// vars so a contract author is not misled.
			m.logger.Debug("contract env var not provided by d8; passed through if set",
				slog.String("plugin", contract.Name), slog.String("env", want.Name))
		}
	}

	return env
}

func (m *Manager) checkInstalled(commandName string) (bool, error) {
	absPath, err := filepath.Abs(layout.CurrentLinkPath(m.pluginDirectory, commandName))
	if err != nil {
		return false, fmt.Errorf("failed to compute absolute path: %w", err)
	}

	_, err = os.Stat(absPath)
	if os.IsNotExist(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}
