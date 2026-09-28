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

// Package cmd holds the olcedar command group: what an operator does to a
// machine running the immutable OS from the cluster side.
package cmd

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/olcedar/cmd/add"
)

var olcedarLong = templates.LongDesc(`
Operate machines running olcedar, the immutable OS of the platform.

© Flant JSC 2026`)

func NewCommand() *cobra.Command {
	olcedarCmd := &cobra.Command{
		Use:   "olcedar",
		Short: "Operate machines running the immutable OS",
		Long:  olcedarLong,
	}

	nodeCmd := &cobra.Command{
		Use:   "node",
		Short: "Operate the nodes such machines become",
	}

	nodeCmd.AddCommand(add.NewCommand())
	olcedarCmd.AddCommand(nodeCmd)

	return olcedarCmd
}
