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

package edit

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	cluster_config "github.com/deckhouse/deckhouse-cli/internal/edit/cmd/cluster-configuration"
)

// TODO texts
var editLong = templates.LongDesc(`
Edit cluster configuration

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	editCmd := &cobra.Command{
		Use:   "edit",
		Short: "Edit cluster configuration",
		Long:  editLong,
	}

	editCmd.AddCommand(
		cluster_config.NewCommand(),
	)

	return editCmd
}
