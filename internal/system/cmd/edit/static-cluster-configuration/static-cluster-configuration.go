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

	edit "github.com/deckhouse/deckhouse-cli/internal/system/cmd/edit/editconfig"
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/edit/flags"
)

var staticClusterConfigurationLong = templates.LongDesc(`
Edit static-cluster-configuration in Kubernetes cluster.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	staticClusterConfigurationCmd := &cobra.Command{
		Use:           "static-cluster-configuration",
		Short:         "Edit static-cluster-configuration.",
		Long:          staticClusterConfigurationLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          editStaticClusterConfig,
	}
	flags.AddFlags(staticClusterConfigurationCmd.Flags())

	return staticClusterConfigurationCmd
}

const staticClusterConfigurationSecret = "d8-static-cluster-configuration"

func editStaticClusterConfig(cmd *cobra.Command, _ []string) error {
	createOpts := &edit.CreateOptions{
		Labels: map[string]string{"name": staticClusterConfigurationSecret},
	}

	err := edit.BaseEditConfigCMD(cmd, "static-cluster-configuration", staticClusterConfigurationSecret, "static-cluster-configuration.yaml", createOpts)
	if err != nil {
		return fmt.Errorf("Error updating secret: %w", err)
	}

	return err
}
