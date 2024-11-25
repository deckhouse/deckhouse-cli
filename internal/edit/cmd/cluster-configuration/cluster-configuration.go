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

package cluster_config

import (
	"log"
	"k8s.io/kubectl/pkg/util/templates"
	"github.com/spf13/cobra"
        "github.com/deckhouse/deckhouse-cli/pkg/utilk8s"
        "github.com/deckhouse/deckhouse-cli/internal/edit/flags"
)

var clusterConfigurationLong = templates.LongDesc(`
Edit cluster-configuration in Kubernetes cluster.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	clusterConfigurationCmd := &cobra.Command{
		Use:           "cluster-configuration",
		Short:         "Edit cluster-configuration.",
		Long:          clusterConfigurationLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       flags.ValidateParameters,
		RunE:          editClusterConfig,
	}

	flags.AddFlags(clusterConfigurationCmd.Flags())
	return clusterConfigurationCmd
}

func editClusterConfig(cmd *cobra.Command, _ []string) error {
	err := utilk8s.BaseEditConfigCMD(cmd, "cluster-configuration", "d8-cluster-configuration", "cluster-configuration.yaml")
	if err != nil {
		log.Fatalf("Error updating secret: %s", err.Error())
	}
	return err
}
