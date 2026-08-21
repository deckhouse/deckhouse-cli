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

// Package add introduces a machine waiting on its maintenance port into the
// cluster as a static node with an immutable OS.
package add

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/olcedar/cluster"
	"github.com/deckhouse/deckhouse-cli/internal/olcedar/machine"
	"github.com/deckhouse/deckhouse-cli/internal/olcedar/plan"
	"github.com/deckhouse/deckhouse-cli/internal/olcedar/prompt"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const (
	networkDHCP   = "dhcp"
	networkStatic = "static"
)

var addLong = templates.LongDesc(`
Add a machine to the cluster as a static node running an immutable OS (olcedar).

The machine waits for its configuration on port 50000, and the cluster renders
the rest of that configuration for the NodeGroup. This command reads both
halves, asks about what only an operator can decide — the disk, the network and
the node name — and hands the result to the machine.

Only a NodeGroup with nodeType Static and systemType Immutable has machines that
read a configuration. Nodes of any other group are provisioned by the cluster
itself, and --group offers no such group.

© Flant JSC 2026`)

type options struct {
	group            string
	name             string
	diskSelector     string
	network          string
	networkInterface string
	wipe             bool
	yes              bool
	dryRun           bool
	wait             bool
	waitTimeout      time.Duration
}

func NewCommand() *cobra.Command {
	opts := &options{}

	addCmd := &cobra.Command{
		Use:   "add <address>",
		Short: "Add a static node running an immutable OS",
		Long:  addLong,
		Args:  cobra.ExactArgs(1),
		Example: `  # Ask about the disk and the network, then add the machine as worker-1.
  d8 platform olcedar node add 10.12.4.55 --group worker

  # Decide everything up front, for a script.
  d8 platform olcedar node add 10.12.4.55 --group worker \
    --name worker-1 --disk-selector serial=S3Z8NB0K700002 --network dhcp --yes`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args[0], opts)
		},
	}

	flags := addCmd.Flags()
	flags.StringVar(&opts.group, "group", "", "NodeGroup to add the machine to")
	flags.StringVar(&opts.name, "name", "", "Name to register the node under (default <group>-<first free number>)")
	flags.StringVar(&opts.diskSelector, "disk-selector", "", "Disk to install onto, as key=value (serial, wwid, name, busPath, model)")
	flags.StringVar(&opts.network, "network", "", "Network configuration: dhcp or static (default asks, dhcp)")
	flags.StringVar(&opts.networkInterface, "network-interface", "", "Interface to configure, when the address reaches the machine through a forward")
	flags.BoolVar(&opts.wipe, "wipe", false, "Erase the disk and reinstall onto it; only for a machine booted from installation media")
	flags.BoolVar(&opts.yes, "yes", false, "Answer every question with its default, and refuse where there is no default")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Print the document with its secrets redacted instead of pushing it")
	flags.BoolVar(&opts.wait, "wait", true, "Wait for the node to register in the cluster")
	flags.DurationVar(&opts.waitTimeout, "wait-timeout", 20*time.Minute, "How long to wait for the node to register")

	if err := addCmd.MarkFlagRequired("group"); err != nil {
		panic(err)
	}

	if err := addCmd.RegisterFlagCompletionFunc("group", completeGroups); err != nil {
		panic(err)
	}

	return addCmd
}

func completeGroups(cmd *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
	defer cancel()

	groups, err := cluster.ImmutableStaticGroups(ctx, dyn)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}

	return groups, cobra.ShellCompDirectiveNoFileComp
}

func run(cmd *cobra.Command, target string, opts *options) error {
	if err := validate(opts, term.IsTerminal(int(os.Stdin.Fd()))); err != nil {
		return err
	}

	ctx := cmd.Context()
	address := machine.Address(target)
	p := prompt.New(cmd.InOrStdin(), cmd.OutOrStdout(), opts.yes)

	kube, dyn, err := clients(cmd)
	if err != nil {
		return err
	}

	if err := checkMachineIsNotANode(ctx, kube, address); err != nil {
		return err
	}

	template, err := cluster.FetchTemplate(ctx, dyn, opts.group)
	if err != nil {
		return err
	}

	inventory, err := machine.FetchInventory(ctx, address)
	if err != nil {
		return err
	}

	choices, err := decide(ctx, p, kube, inventory, machine.Host(address), opts)
	if err != nil {
		return err
	}

	document, err := plan.BuildDocument(template, *choices)
	if err != nil {
		return err
	}

	if opts.dryRun {
		return printRedacted(cmd, document)
	}

	if err := refuseTakenName(ctx, kube, choices.NodeName); err != nil {
		return err
	}

	if err := machine.PushNodeConfig(ctx, address, document); err != nil {
		return err
	}

	p.Printf("\n%s accepted the configuration and is installing itself as %s.\n", address, choices.NodeName)

	if !opts.wait {
		return nil
	}

	return waitForNode(ctx, p, kube, choices.NodeName, opts.waitTimeout)
}

func validate(opts *options, interactive bool) error {
	switch opts.network {
	case "", networkDHCP, networkStatic:
	default:
		return fmt.Errorf("--network is %q, and it takes %s or %s", opts.network, networkDHCP, networkStatic)
	}

	if opts.yes || interactive {
		return nil
	}

	return errors.New("this command asks which disk to install onto and how the node reaches the network, " +
		"and there is no terminal to ask on: run it with --yes, and name what it would have asked with " +
		"--name, --disk-selector, --wipe, --network and --network-interface")
}

func clients(cmd *cobra.Command) (kubernetes.Interface, dynamic.Interface, error) {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return nil, nil, fmt.Errorf("read the kubeconfig flag: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return nil, nil, fmt.Errorf("read the context flag: %w", err)
	}

	restConfig, kube, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return nil, nil, fmt.Errorf("set up the Kubernetes client: %w", err)
	}

	dyn, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("set up the dynamic Kubernetes client: %w", err)
	}

	return kube, dyn, nil
}

// checkMachineIsNotANode refuses a machine whose port is held by the agent of
// an installed node: a second configuration would take a working node down.
func checkMachineIsNotANode(ctx context.Context, kube kubernetes.Interface, address string) error {
	who, err := machine.Whoami(ctx, address)
	if err != nil {
		return err
	}

	switch who {
	case machine.WhoamiInstaller:
		return nil
	case machine.WhoamiAgent:
		return fmt.Errorf("%s is already %s: its port is held by the node agent, and a second configuration "+
			"would replace the configuration that node runs on", address, nodeDescription(ctx, kube, address))
	default:
		return fmt.Errorf("%s answered %q to /whoami, which is neither %s nor %s: whatever holds port %s is not olcedar",
			address, who, machine.WhoamiInstaller, machine.WhoamiAgent, machine.MaintenancePort)
	}
}

func nodeDescription(ctx context.Context, kube kubernetes.Interface, address string) string {
	if name := cluster.NodeNameByAddress(ctx, kube, machine.Host(address)); name != "" {
		return "node " + name
	}

	return "a node of this cluster"
}

func decide(
	ctx context.Context,
	p *prompt.Prompt,
	kube kubernetes.Interface,
	inventory *machine.Inventory,
	host string,
	opts *options,
) (*plan.Choices, error) {
	selector, err := parseSelector(opts.diskSelector)
	if err != nil {
		return nil, err
	}

	disk, err := plan.ChooseDisk(p, inventory, selector, opts.wipe)
	if err != nil {
		return nil, err
	}

	iface, err := plan.ChooseInterface(p, inventory, host, opts.networkInterface)
	if err != nil {
		return nil, err
	}

	static, err := chooseStatic(p, iface, opts.network)
	if err != nil {
		return nil, err
	}

	name, err := chooseName(ctx, p, kube, opts)
	if err != nil {
		return nil, err
	}

	return &plan.Choices{
		NodeName:      name,
		Disk:          disk,
		Selector:      machine.SelectorFor(disk),
		Wipe:          opts.wipe,
		Interface:     iface,
		StaticAddress: static,
	}, nil
}

func parseSelector(raw string) (machine.Selector, error) {
	if raw == "" {
		return nil, nil
	}

	return machine.ParseSelector(raw)
}

func chooseStatic(p *prompt.Prompt, iface machine.Interface, network string) (bool, error) {
	if network == networkDHCP {
		return false, nil
	}

	if network == networkStatic {
		if len(iface.Addresses) == 0 {
			return false, fmt.Errorf("--network %s pins the addresses interface %s holds, and it holds none",
				networkStatic, iface.Name)
		}

		return true, nil
	}

	if len(iface.Addresses) == 0 {
		return false, nil
	}

	return p.Confirm(fmt.Sprintf("Interface %s holds %s. Pin it statically instead of using DHCP?",
		iface.Name, iface.Addresses[0]), false)
}

func chooseName(ctx context.Context, p *prompt.Prompt, kube kubernetes.Interface, opts *options) (string, error) {
	if opts.name != "" {
		return opts.name, nil
	}

	free, err := cluster.FreeNodeName(ctx, kube, opts.group)
	if err != nil {
		return "", err
	}

	return p.Line("Node name", free)
}

func refuseTakenName(ctx context.Context, kube kubernetes.Interface, name string) error {
	taken, err := cluster.NodeExists(ctx, kube, name)
	if err != nil {
		return err
	}

	if taken {
		return fmt.Errorf("node %s is already in this cluster: pick another name with --name", name)
	}

	return nil
}

func printRedacted(cmd *cobra.Command, document []byte) error {
	redacted, err := plan.Redact(document)
	if err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "%s", redacted)

	return nil
}

func waitForNode(ctx context.Context, p *prompt.Prompt, kube kubernetes.Interface, name string, timeout time.Duration) error {
	started := time.Now()

	p.Printf("Waiting for %s to register, up to %s.\n", name, timeout)

	if err := cluster.WaitForNode(ctx, kube, name, timeout); err != nil {
		return err
	}

	p.Printf("%s registered in %s. It becomes Ready once the cluster rolls its modules onto it:\n  d8 k wait --for=condition=Ready node/%s\n",
		name, time.Since(started).Round(time.Second), name)

	return nil
}
