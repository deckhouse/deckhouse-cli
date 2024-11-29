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

package static_config

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/edit"
)

var staticClusterConfigurationLong = templates.LongDesc(`
Edit static-cluster-configuration in Kubernetes cluster.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	staticClusterConfigurationCmd := &cobra.Command{
		Use:           "static-cluster-configuration",
		Short:         "Edit static-cluster-configuration.",
		Long:          staticClusterConfigurationLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          editStaticClusterConfig,
	}
	return staticClusterConfigurationCmd
}

func editStaticClusterConfig(cmd *cobra.Command, _ []string) error {
	err := edit.BaseEditConfigCMD(cmd, "static-cluster-configuration", "d8-static-cluster-configuration", "static-cluster-configuration.yaml")
	if err != nil {
		return fmt.Errorf("Error updating secret: %w", err)
	}
	return err
}
