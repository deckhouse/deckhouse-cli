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

package staticconfig

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	get "github.com/deckhouse/deckhouse-cli/internal/system/cmd/get/getconfig"
)

var staticClusterConfigurationLong = templates.LongDesc(`
Get static-cluster-configuration from Kubernetes cluster.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	staticClusterConfigurationCmd := &cobra.Command{
		Use:           "static-cluster-configuration",
		Short:         "Get static-cluster-configuration.",
		Long:          staticClusterConfigurationLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          getStaticClusterConfig,
	}
	return staticClusterConfigurationCmd
}

func getStaticClusterConfig(cmd *cobra.Command, _ []string) error {
	err := get.BaseGetConfigCMD(cmd, "static-cluster-configuration", "d8-static-cluster-configuration", "static-cluster-configuration.yaml")
	if err != nil {
		return fmt.Errorf("Error getting secret: %w", err)
	}
	return nil
}


