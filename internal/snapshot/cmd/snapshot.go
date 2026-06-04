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

package cmd

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/cmd/download"
)

const (
	cmdName = "snapshot"

	debugEnvVar = "D8_SNAPSHOT_DEBUG"

	cmdShort = "Snapshot operations (download namespace snapshots)"

	cmdLong = `Manage Deckhouse namespace snapshots.

The snapshot command lets you download namespace manifests captured by the
state-snapshotter module. The Snapshot CR must already exist and be Ready.

Future sub-commands will support uploading archives and restoring manifests.`
)

// NewCommand returns the top-level snapshot cobra command (alias: snap).
func NewCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           cmdName,
		Aliases:       []string{"snap"},
		Short:         cmdShort,
		Long:          cmdLong,
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, _ []string) {
			_ = cmd.Help()
		},
	}

	root.SetOut(os.Stdout)

	log := setupLogger()

	root.AddCommand(download.NewCommand(log))

	return root
}

func setupLogger() *slog.Logger {
	level := slog.LevelInfo
	if debugEnabled() {
		level = slog.LevelDebug
	}

	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

func debugEnabled() bool {
	v := os.Getenv(debugEnvVar)

	return v != "" && v != "0" && v != "false"
}
