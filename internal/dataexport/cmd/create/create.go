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

package create

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/deckhouse/deckhouse-cli/internal/dataexport/util"
)

const (
	cmdName = "create"
)

func cmdExamples() string {
	resp := []string{
		fmt.Sprintf("  # Start data exporting for PVC 'test-pvc-name'"),
		fmt.Sprintf("    ... %s export-name pvc/test-pvc-name", cmdName),
		fmt.Sprintf("  # Start data exporting with extra flags"),
		fmt.Sprintf("    ... %s --kubeconfig='kube_tmp.conf' -n target-namespace --ttl 17m export-name pvc/test-pvc-name", cmdName),
	}
	return strings.Join(resp, "\n")
}

func NewCommand(clientCmdConfig clientcmd.ClientConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cmdName + " [flags] data_export_name volume_type/volume_name",
		Short:   "Create dataexport kubernetes resource",
		Example: cmdExamples(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(cmd, args, clientCmdConfig)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			_, _, _, err := parseArgs(args)
			return err
		},
	}

	cmd.Flags().String("ttl", "30m", "Time to live")
	cmd.Flags().Bool("publish", false, "Provide access outside of cluster")

	return cmd
}

func parseArgs(args []string) (deName, volumeKind, volumeName string, err error) {
	if len(args) != 2 {
		err = fmt.Errorf("invalid arguments")
		return
	}
	deName = args[0]
	resourceTypeAndName := strings.Split(args[1], "/")
	if len(resourceTypeAndName) != 2 {
		err = fmt.Errorf("invalid volume format, expect: <type>/<name>")
		return
	}
	volumeKind, volumeName = resourceTypeAndName[0], resourceTypeAndName[1]
	switch {
	case volumeKind == "pvc" || volumeKind == "PVC":
		volumeKind = "PersistentVolumeClaim"
	case volumeKind == "vs" || volumeKind == "VS":
		volumeKind = "VolumeSnapshot"
	default:
		err = fmt.Errorf("invalid volume type, expect: 'pvc' or 'vs'")
		return
	}

	return
}

func Run(cmd *cobra.Command, args []string, clientCmdConfig clientcmd.ClientConfig) error {
	namespace, _ := cmd.Flags().GetString("namespace")
	ttl, _ := cmd.Flags().GetString("ttl")
	publish, _ := cmd.Flags().GetBool("publish")

	deName, volumeKind, volumeName, err := parseArgs(args)
	if err != nil {
		return err
	}

	rtClient, err := util.NewKubeRTClient(clientCmdConfig)
	if err != nil {
		return err
	}

	err = util.CreateDataExport(deName, namespace, ttl, volumeKind, volumeName, publish, rtClient)
	if err != nil {
		return err
	}

	fmt.Printf("Created %s\n", deName)
	return nil
}
