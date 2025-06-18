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

package get

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	cmdName = "get"
)

func cmdExamples() string {
	resp := []string{
		fmt.Sprintf(`  ... %s my-volume`, cmdName),
	}
	return strings.Join(resp, "\n")
}

func NewCommand(clientCmdConfig clientcmd.ClientConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] [data_export_name [/path/[file.ext]]]",
		Short:   "Get DataExport object information",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(cmd, args, clientCmdConfig)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			_, _, err := parseArgs(args)
			return err
		},
	}

	return cmd
}

func parseArgs(args []string) (deName, srcPath string, err error) {
	if len(args) == 0 {
		return "", "", nil
	}
	if len(args) == 1 {
		return args[0], "", nil
	}
	if len(args) == 2 {
		return args[0], args[1], nil
	}

	return "", "", fmt.Errorf("invalid arguments")
}

func Run(cmd *cobra.Command, args []string, clientCmdConfig clientcmd.ClientConfig) error {
	fmt.Println("Not implemented.")
	fmt.Println("Use `d8 k -n <namespace> get de|data-export <DataExport resource name>` to get DataExport resources")
	return nil
}
