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

package etcd

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var etcdLong = templates.LongDesc(`
Take a snapshot of ETCD state.
		
This command creates a snapshot of the Kubernetes underlying key-value database ETCD.

© Flant JSC 2025`)

var config = &Config{}

func NewCommand() *cobra.Command {
	etcdCmd := &cobra.Command{
		Use:           "etcd <snapshot-path>",
		Short:         "Take a snapshot of ETCD state",
		Long:          etcdLong,
		ValidArgs:     []string{"snapshot-path"},
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			return validateFlags(cmd)
		},
		RunE: runETCD,
	}

	addFlags(etcdCmd.Flags())
	return etcdCmd
}

func runETCD(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("this command requires exactly 1 argument")
	}

	config.SnapshotPath = args[0]

	runner := NewRunner(config)
	return runner.Run(cmd.Context(), cmd)
}
