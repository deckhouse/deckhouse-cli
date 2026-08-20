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

// Package plan turns what the machine reports and what the operator answers
// into the one document the machine boots from.
package plan

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/olcedar/machine"
	"github.com/deckhouse/deckhouse-cli/internal/olcedar/prompt"
)

// The document a machine reads. Mirrors payloadAPIVersion and nodeConfigKind of
// dhctl/pkg/immutable/constants.go, which spells the same pair when the
// installer builds one.
const (
	documentAPIVersion = "internal.deckhouse.io/v1alpha1"
	documentKind       = "NodeConfig"
)

// redactedPaths are the fields a template carries live on every read. They are
// blanked before a document is ever shown to anyone.
var redactedPaths = [][]string{
	{"spec", "kubelet", "bootstrapToken"},
	{"spec", "registry", "auth"},
	{"spec", "registryPackagesProxyAccessTokenB64"},
}

// Redacted is what a secret reads as once it is not printed.
const Redacted = "REDACTED"

// Choices is everything the operator decided about this machine.
type Choices struct {
	NodeName  string
	Disk      machine.Disk
	Selector  machine.Selector
	Wipe      bool
	Interface machine.Interface
	// StaticAddress pins the addresses the interface currently holds instead of
	// leaving it on DHCP.
	StaticAddress bool
}

// ChooseDisk settles which disk the OS is installed onto, and whether the
// install may erase what is on it. A selector given on the command line has to
// resolve to exactly one disk; without one the operator picks from the list.
func ChooseDisk(p *prompt.Prompt, inventory *machine.Inventory, selector machine.Selector, wipe bool) (machine.Disk, bool, error) {
	if len(inventory.Disks) == 0 {
		return machine.Disk{}, false, errors.New("this machine reports no disks, so there is nothing to install onto")
	}

	disk, err := pickDisk(p, inventory, selector)
	if err != nil {
		return machine.Disk{}, false, err
	}

	if disk.State == machine.StateBlank {
		return disk, false, nil
	}

	if wipe {
		return disk, true, nil
	}

	confirmed, err := p.Confirm(fmt.Sprintf(
		"Disk %s is %s and the install will erase it. Continue?", disk.Name, disk.State), false)
	if err != nil {
		return machine.Disk{}, false, err
	}

	if !confirmed {
		return machine.Disk{}, false, fmt.Errorf(
			"disk %s is %s, not %s: erasing it has to be confirmed, or allowed with --wipe",
			disk.Name, disk.State, machine.StateBlank)
	}

	return disk, true, nil
}

func pickDisk(p *prompt.Prompt, inventory *machine.Inventory, selector machine.Selector) (machine.Disk, error) {
	if len(selector) > 0 {
		return matchOne(selector, inventory.Disks)
	}

	options := make([]string, 0, len(inventory.Disks))
	for _, disk := range inventory.Disks {
		options = append(options, describeDisk(disk))
	}

	index, err := p.Choose("Disks this machine reports:", options, defaultDisk(inventory.Disks))
	if err != nil {
		return machine.Disk{}, fmt.Errorf("%w. Name the disk with --disk-selector, e.g. --disk-selector serial=%s",
			err, firstNonEmptySerial(inventory.Disks))
	}

	return inventory.Disks[index], nil
}

func matchOne(selector machine.Selector, disks []machine.Disk) (machine.Disk, error) {
	matched, err := selector.Match(disks)
	if err != nil {
		return machine.Disk{}, err
	}

	switch len(matched) {
	case 1:
		return matched[0], nil
	case 0:
		return machine.Disk{}, fmt.Errorf("the disk selector matches no disk of this machine, which has:\n%s", describeDisks(disks))
	default:
		return machine.Disk{}, fmt.Errorf("the disk selector matches %d disks and only one can hold the system:\n%s",
			len(matched), describeDisks(matched))
	}
}

// defaultDisk offers the one blank disk, and nothing when there is a choice to
// make: a disk holding data is never picked for the operator.
func defaultDisk(disks []machine.Disk) int {
	found := prompt.NoDefault

	for i, disk := range disks {
		if disk.State != machine.StateBlank {
			continue
		}

		if found != prompt.NoDefault {
			return prompt.NoDefault
		}

		found = i
	}

	return found
}

// ChooseInterface settles which NIC the node configures. The interface the CLI
// reached the machine on is proven by the connection itself, so it is the
// default; an address that belongs to none of them (a port forward, a NAT) has
// to be answered for.
func ChooseInterface(p *prompt.Prompt, inventory *machine.Inventory, host, named string) (machine.Interface, error) {
	if len(inventory.Interfaces) == 0 {
		return machine.Interface{}, errors.New("this machine reports no interfaces, so there is nothing to configure")
	}

	if named != "" {
		index := slices.IndexFunc(inventory.Interfaces, func(i machine.Interface) bool { return i.Name == named })
		if index < 0 {
			return machine.Interface{}, fmt.Errorf("this machine has no interface %q, it has:\n%s", named, describeInterfaces(inventory.Interfaces))
		}

		return inventory.Interfaces[index], nil
	}

	if index := interfaceOfHost(inventory.Interfaces, host); index >= 0 {
		return inventory.Interfaces[index], nil
	}

	options := make([]string, 0, len(inventory.Interfaces))
	for _, iface := range inventory.Interfaces {
		options = append(options, describeInterface(iface))
	}

	title := fmt.Sprintf("%s is not an address of any interface this machine reports:", host)

	index, err := p.Choose(title, options, prompt.NoDefault)
	if err != nil {
		return machine.Interface{}, fmt.Errorf("%w. Name the interface with --network-interface", err)
	}

	return inventory.Interfaces[index], nil
}

func interfaceOfHost(interfaces []machine.Interface, host string) int {
	for i, iface := range interfaces {
		for _, address := range iface.Addresses {
			if addressOf(address) == host {
				return i
			}
		}
	}

	return -1
}

// addressOf drops the prefix length an inventory address carries.
func addressOf(address string) string {
	bare, _, _ := strings.Cut(address, "/")

	return bare
}

// BuildDocument fills the machine's half into the cluster's template. The
// result is the document the machine boots from, and it carries a live
// bootstrap token: it is built in memory and never written anywhere.
func BuildDocument(template *unstructured.Unstructured, choices Choices) ([]byte, error) {
	spec, found, err := unstructured.NestedMap(template.Object, "spec")
	if err != nil || !found {
		return nil, fmt.Errorf("the node configuration template of this group carries no spec: %w", err)
	}

	document := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": documentAPIVersion,
		"kind":       documentKind,
		"metadata":   map[string]any{"name": choices.NodeName},
		"spec":       spec,
	}}

	if err := unstructured.SetNestedField(document.Object, choices.NodeName, "spec", "nodeName"); err != nil {
		return nil, fmt.Errorf("set the node name: %w", err)
	}

	if err := unstructured.SetNestedMap(document.Object, storageOf(choices), "spec", "storage"); err != nil {
		return nil, fmt.Errorf("set the storage: %w", err)
	}

	if err := unstructured.SetNestedMap(document.Object, networkOf(choices), "spec", "network"); err != nil {
		return nil, fmt.Errorf("set the network: %w", err)
	}

	body, err := yaml.Marshal(document.Object)
	if err != nil {
		return nil, fmt.Errorf("render the node configuration: %w", err)
	}

	return body, nil
}

func storageOf(choices Choices) map[string]any {
	selector := map[string]any{}
	for key, value := range choices.Selector {
		selector[key] = value
	}

	return map[string]any{"diskSelector": selector, "wipe": choices.Wipe}
}

func networkOf(choices Choices) map[string]any {
	iface := map[string]any{"name": choices.Interface.Name, "dhcp": !choices.StaticAddress}

	if choices.StaticAddress {
		addresses := make([]any, 0, len(choices.Interface.Addresses))
		for _, address := range choices.Interface.Addresses {
			addresses = append(addresses, address)
		}

		iface["addresses"] = addresses

		if choices.Interface.Gateway != "" {
			iface["gateway"] = choices.Interface.Gateway
		}
	}

	return map[string]any{"interfaces": []any{iface}}
}

// Redact blanks the three fields a template carries live, so a document may be
// shown without handing over the right to add a node to the cluster.
func Redact(document []byte) ([]byte, error) {
	object := map[string]any{}
	if err := yaml.Unmarshal(document, &object); err != nil {
		return nil, fmt.Errorf("read the document to redact it: %w", err)
	}

	for _, path := range redactedPaths {
		if _, found, _ := unstructured.NestedString(object, path...); !found {
			continue
		}

		if err := unstructured.SetNestedField(object, Redacted, path...); err != nil {
			return nil, fmt.Errorf("redact %s: %w", strings.Join(path, "."), err)
		}
	}

	body, err := yaml.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("render the redacted document: %w", err)
	}

	return body, nil
}

func describeDisks(disks []machine.Disk) string {
	lines := make([]string, 0, len(disks))
	for _, disk := range disks {
		lines = append(lines, "  "+describeDisk(disk))
	}

	return strings.Join(lines, "\n")
}

func describeDisk(disk machine.Disk) string {
	line := fmt.Sprintf("%-10s %8s  %-14s %s", disk.Name, HumanSize(disk.Size), disk.State, strings.TrimSpace(disk.Vendor+" "+disk.Model))

	if disk.Serial != "" {
		line += fmt.Sprintf(" (serial %s)", disk.Serial)
	}

	if disk.State == machine.StateSystemLayout {
		line += " [holds an OS]"
	}

	return strings.TrimSpace(line)
}

func describeInterfaces(interfaces []machine.Interface) string {
	lines := make([]string, 0, len(interfaces))
	for _, iface := range interfaces {
		lines = append(lines, "  "+describeInterface(iface))
	}

	return strings.Join(lines, "\n")
}

func describeInterface(iface machine.Interface) string {
	addresses := strings.Join(iface.Addresses, ", ")
	if addresses == "" {
		addresses = "no address"
	}

	line := fmt.Sprintf("%-8s %-18s %-5s %s", iface.Name, iface.MAC, iface.Link, addresses)

	if iface.Gateway != "" {
		line += " gw " + iface.Gateway
	}

	return strings.TrimSpace(line)
}

func firstNonEmptySerial(disks []machine.Disk) string {
	for _, disk := range disks {
		if disk.Serial != "" {
			return disk.Serial
		}
	}

	return "<serial>"
}

// HumanSize spells a byte count the way an operator reads a disk size.
func HumanSize(size uint64) string {
	const unit = 1024

	if size < unit {
		return fmt.Sprintf("%dB", size)
	}

	value, exponent := float64(size), 0
	for value >= unit && exponent < 5 {
		value /= unit
		exponent++
	}

	return fmt.Sprintf("%.0f%ci", value, "KMGTP"[exponent-1])
}
