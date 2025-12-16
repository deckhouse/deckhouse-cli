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

package clusterconfig

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	get "github.com/deckhouse/deckhouse-cli/internal/system/cmd/get/getconfig"
)

var clusterConfigurationLong = templates.LongDesc(`
Get cluster-configuration from Kubernetes cluster.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	clusterConfigurationCmd := &cobra.Command{
		Use:           "cluster-configuration",
		Short:         "Get cluster-configuration.",
		Long:          clusterConfigurationLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          getClusterConfig,
	}
	return clusterConfigurationCmd
}

func getClusterConfig(cmd *cobra.Command, _ []string) error {
	err := get.BaseGetConfigCMD(cmd, "cluster-configuration", "d8-cluster-configuration", "cluster-configuration.yaml")
	if err != nil {
		return fmt.Errorf("Error getting secret: %w", err)
	}
	return nil
}


