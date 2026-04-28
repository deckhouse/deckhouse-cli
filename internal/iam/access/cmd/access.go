/*
Copyright 2026 Flant JSC

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

package access

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
)

var accessLong = templates.LongDesc(`
Grant and revoke access in Deckhouse (current authz model).

For inspecting existing rules and effective access (warnings, group
cycles, manual rules), use the top-level "d8 iam get" / "d8 iam list".

© Flant JSC 2026`)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "access",
		Short:         "Grant or revoke access (current authz model)",
		Long:          accessLong,
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	flags.AddPersistentFlags(cmd)

	cmd.AddCommand(
		newGrantCommand(),
		newRevokeCommand(),
	)

	return cmd
}
