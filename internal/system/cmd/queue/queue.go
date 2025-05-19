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

package queue

import (
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/queue/list"
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/queue/mainqueue"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var queueLong = templates.LongDesc(`
Dump queues from Deckhouse Kubernetes Platform.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	queueCmd := &cobra.Command{
		Use: "queue", Short: "Dump queues.",
		Long: queueLong,
	}

	queueCmd.AddCommand(
		list.NewCommand(),
		mainqueue.NewCommand(),
	)

	return queueCmd
}
