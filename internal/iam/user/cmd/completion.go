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

package user

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// completeUserNames is a cobra ValidArgsFunction that returns User CR names
// for the first positional argument; subsequent positional arguments fall
// back to file completion via ShellCompDirectiveNoFileComp suppression.
func completeUserNames(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) >= 1 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return utilk8s.CompleteResourceNames(cmd, userGVR, "", toComplete)
}
