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

// Package local implements the offline `d8 snapshot local` command group.
// All subcommands operate on a locally downloaded snapshot archive directory
// and never contact a cluster or read a kubeconfig.
package local

import (
	"log/slog"

	"github.com/spf13/cobra"
)

// NewCommand builds the `d8 snapshot local` parent cobra command.
// It is fully offline: no kubeconfig flags, no cluster contact.
// Subcommands (get, describe) are attached by their own tasks.
func NewCommand(_ *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "local",
		Short:         "Operate on a locally downloaded snapshot archive (no kubeconfig required)",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, _ []string) {
			_ = cmd.Help()
		},
	}

	return cmd
}
