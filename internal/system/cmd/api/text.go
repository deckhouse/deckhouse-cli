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

package api

import (
	"cmp"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/api/apiclient"
)

// printQueues lists the queues that hold tasks, then counts all of them.
func printQueues(w io.Writer, dump *apiclient.QueuesDump) {
	active, tasks := 0, 0

	for _, name := range slices.Sorted(maps.Keys(dump.Queues)) {
		queue := dump.Queues[name]
		if queue.Length == 0 {
			continue
		}

		active++
		tasks += queue.Length

		fmt.Fprintf(w, "Queue '%s': %d task(s)\n", name, queue.Length)

		for _, task := range queue.Tasks {
			fmt.Fprintf(w, "  %d. %s (enqueued %s, next retry %s)\n", task.Index, task.Name, task.Enqueued, task.NextRetry)

			if task.Error != nil {
				fmt.Fprintf(w, "     error: %s\n", *task.Error)
			}
		}
	}

	fmt.Fprintf(w, "Summary: %d queue(s), %d active, %d task(s).\n", len(dump.Queues), active, tasks)
}

// printSchedulerNodes tables the nodes in scheduling order.
func printSchedulerNodes(w io.Writer, nodes map[string]apiclient.SchedulerNode) error {
	names := slices.SortedFunc(maps.Keys(nodes), func(a, b string) int {
		return cmp.Or(cmp.Compare(nodes[a].EffectiveOrder, nodes[b].EffectiveOrder), cmp.Compare(a, b))
	})

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tVERSION\tSTATE\tORDER\tEFFECTIVE\tDECISION\tSCHEDULE REASON")

	for _, name := range names {
		node := nodes[name]

		decision := string(node.Decision.Kind)
		if node.Decision.Reason != "" {
			decision += " (" + node.Decision.Reason + ")"
		}

		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\t%s\n",
			name, node.Version, node.State, node.Order, node.EffectiveOrder, decision, orDash(node.ScheduleReason))
	}

	return tw.Flush()
}

// printPackages tables the applications and modules with the conditions that need attention.
func printPackages(w io.Writer, dump *apiclient.PackagesDump) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "KIND\tNAME\tVERSION\tRUNNING\tISSUES")

	for _, name := range slices.Sorted(maps.Keys(dump.Apps)) {
		app := dump.Apps[name]
		fmt.Fprintf(tw, "app\t%s\t%s\t%t\t%s\n", name, app.Definition.Version, app.Running, issues(app.Status.Conditions))
	}

	for _, name := range slices.Sorted(maps.Keys(dump.Modules)) {
		module := dump.Modules[name]
		fmt.Fprintf(tw, "module\t%s\t%s\t%t\t%s\n", name, module.Definition.Version, module.Running, issues(module.Status.Conditions))
	}

	return tw.Flush()
}

// issues lists the conditions that are not in their good state: everything but
// True, except MaintenanceMode, whose polarity is inverted.
func issues(conditions []apiclient.Condition) string {
	var parts []string

	for _, condition := range conditions {
		good := condition.Status == "True"
		if condition.Type == apiclient.ConditionMaintenanceMode {
			good = condition.Status != "True"
		}

		if good {
			continue
		}

		part := string(condition.Type) + "=" + condition.Status
		if condition.Reason != "" {
			part += " (" + condition.Reason + ")"
		}

		parts = append(parts, part)
	}

	return orDash(strings.Join(parts, ", "))
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}

	return s
}
