/*
Copyright 2026 Flant JSC

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

// Package selfupdatecmd implements the `d8 cli` command tree on top of the
// internal/selfupdate machinery (store, updater).
package selfupdatecmd

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/rpp"
	rppflags "github.com/deckhouse/deckhouse-cli/internal/rpp/flags"
	"github.com/deckhouse/deckhouse-cli/internal/selfupdate"
	systemflags "github.com/deckhouse/deckhouse-cli/internal/system/flags"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
	"github.com/deckhouse/deckhouse-cli/internal/version"
)

// Output styling, aligned with `d8 cli versions` (list.go): green = the version being
// moved to, cyan+bold = the active version, faint = a superseded version. fatih/color
// drops ANSI on a non-TTY and under NO_COLOR.
var (
	okMark = color.New(color.FgGreen, color.Bold)
	verNew = color.New(color.FgGreen)
	verCur = color.New(color.FgCyan, color.Bold)
	verOld = color.New(color.Faint)
)

// NewCommand returns the `d8 cli` command tree for managing the d8 binary itself.
// It reaches the registry-packages-proxy with the caller's kubeconfig identity.
func NewCommand(logger *dkplog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cli",
		Short: "Manage the deckhouse-cli (d8) binary",
		Long: "Check for and install newer deckhouse-cli versions via the in-cluster registry-packages-proxy.\n\n" +
			"Update on demand with 'd8 cli update'.\n\n" +
			"Environment variables:\n" +
			"  " + rppflags.EnvEndpoint + "  registry-packages-proxy base URL (otherwise discovered from the cluster)\n" +
			"  " + rppflags.EnvCAFile + "   PEM CA bundle to verify the proxy TLS certificate\n" +
			"  KUBECONFIG       path to the kubeconfig file",
	}

	cmd.AddCommand(newCheckCommand(logger))
	cmd.AddCommand(newUpdateCommand(logger))
	cmd.AddCommand(newUseCommand(logger))
	cmd.AddCommand(newVersionsCommand(logger))

	systemflags.AddPersistentFlags(cmd)
	rppflags.AddFlags(cmd.PersistentFlags())

	return cmd
}

func newCheckCommand(logger *dkplog.Logger) *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "Report whether a newer deckhouse-cli version is available",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			updater, err := newUpdater(cmd.Context(), cmd, logger)
			if err != nil {
				return err
			}

			latest, newer, err := updater.LatestVersion(cmd.Context(), version.Version)
			if err != nil {
				return err
			}

			if newer {
				fmt.Printf("A newer deckhouse-cli is available: %s (current: %s). Run 'd8 cli update' to upgrade.\n",
					verNew.Sprint(latest), verOld.Sprint(version.Version))
			} else {
				fmt.Printf("deckhouse-cli is up to date (%s).\n", verCur.Sprint(version.Version))
			}

			return nil
		},
	}
}

func newUpdateCommand(logger *dkplog.Logger) *cobra.Command {
	var targetVersion string

	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update deckhouse-cli to the latest version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			updater, err := newUpdater(cmd.Context(), cmd, logger)
			if err != nil {
				return err
			}

			tag := targetVersion
			if tag == "" {
				latest, newer, err := updater.LatestVersion(cmd.Context(), version.Version)
				if err != nil {
					return err
				}

				if !newer {
					fmt.Printf("deckhouse-cli is already up to date (%s).\n", verCur.Sprint(version.Version))

					return nil
				}

				tag = latest
			}

			fmt.Printf("Updating deckhouse-cli to %s...\n", verNew.Sprint(tag))

			res, err := updater.Apply(cmd.Context(), tag)
			if err != nil {
				return err
			}

			fmt.Printf("%s deckhouse-cli updated to %s.\n", okMark.Sprint("✓"), verNew.Sprint(tag))
			printSwitchNotes(res)

			return nil
		},
	}

	cmd.Flags().StringVar(&targetVersion, "version", "", "Exact version to install; downgrades are allowed (default: the latest).")

	return cmd
}

// newUpdater builds an Updater backed by the registry-packages-proxy, reached
// with the kubeconfig identity from the command's flags.
func newUpdater(ctx context.Context, cmd *cobra.Command, logger *dkplog.Logger) (*selfupdate.Updater, error) {
	kubeconfig, _ := cmd.Flags().GetString("kubeconfig")
	kubeContext, _ := cmd.Flags().GetString("context")

	restConfig, kube, err := utilk8s.SetupK8sClientSet(kubeconfig, kubeContext)
	if err != nil {
		return nil, fmt.Errorf("set up kubernetes client: %w", err)
	}

	client, err := rpp.NewClusterClient(
		ctx, kube, restConfig, logger.Named("registry-packages-proxy"),
		rppflags.Endpoint, rppflags.CAFile, rppflags.InsecureSkipTLSVerify,
	)
	if err != nil {
		return nil, fmt.Errorf("build registry-packages-proxy client: %w", err)
	}

	store, err := selfupdate.NewStore()
	if err != nil {
		// A nil store only disables retention for `d8 cli use`; updating still works.
		logger.Debug("version store unavailable", dkplog.Err(err))
	}

	return selfupdate.NewUpdater(selfupdate.NewRPPSource(client), store, logger.Named("selfupdate")), nil
}
