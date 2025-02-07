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

package loki

import (
	"fmt"
	"github.com/spf13/pflag"
	"os"

	"github.com/spf13/cobra"
)

func addFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&endTimestamp,
		"end",
		"",
		"Set end timestamp range to dump logs from Loki. (Default get max end timestamp from Loki, ex. \"2025-01-14 15:04:05\".)",
	)
	flagSet.StringVar(
		&startTimestamp,
		"start",
		"",
		"Set start timestamp range to dump logs from Loki. (Default get max start timestamp from Loki, ex. \"2025-01-12 15:04:05\".)",
	)
	flagSet.StringVar(
		&limitFlag,
		"limit",
		"",
		"Limit max string to output per queue from Loki. (Default 100000 max limit strings per queue.)",
	)
}

func validateFlags(cmd *cobra.Command) error {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	stats, err := os.Stat(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Invalid --kubeconfig: %w", err)
	}
	if !stats.Mode().IsRegular() {
		return fmt.Errorf("Invalid --kubeconfig: %s is not a regular file", kubeconfigPath)
	}

	return nil
}
