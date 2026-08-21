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

package plan

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/olcedar/machine"
	"github.com/deckhouse/deckhouse-cli/internal/olcedar/prompt"
)

// bootstrapToken is the fixture secret every leak test looks for by value.
const bootstrapToken = "abcdef.0123456789abcdef"

const registryAuth = "ZGVja2hvdXNlOnBhc3N3b3Jk"

const proxyToken = "cmVnaXN0cnktcGFja2FnZXMtcHJveHktdG9rZW4="

func inventory() *machine.Inventory {
	return &machine.Inventory{
		Disks: []machine.Disk{
			{Name: "nvme0n1", Size: 512110190592, Model: "MZVL2512", Serial: "S3Z8NB0K700002", State: machine.StateBlank},
			{Name: "sda", Size: 2000398934016, Model: "ST2000DM008", Serial: "ZA20ABCD", State: machine.StateSystemLayout},
		},
		Interfaces: []machine.Interface{
			{Name: "eno1", MAC: "aa:bb:cc:dd:ee:01", Link: "up", Addresses: []string{"10.12.4.55/24"}, Gateway: "10.12.4.1"},
			{Name: "eno2", MAC: "aa:bb:cc:dd:ee:02", Link: "down"},
		},
	}
}

func template() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.internal.deckhouse.io/v1alpha1",
		"kind":       "NodeConfigTemplate",
		"metadata":   map[string]any{"name": "worker"},
		"spec": map[string]any{
			"nodeName":                            "",
			"osImage":                             map[string]any{"name": "olcedar"},
			"kubelet":                             map[string]any{"bootstrapToken": bootstrapToken, "maxPods": int64(120)},
			"registry":                            map[string]any{"auth": registryAuth},
			"registryPackagesProxyAccessTokenB64": proxyToken,
			"network":                             map[string]any{},
			"storage":                             map[string]any{},
		},
	}}
}

func asking(answers string) (*prompt.Prompt, *bytes.Buffer) {
	out := &bytes.Buffer{}

	return prompt.New(strings.NewReader(answers), out, false), out
}

func assuming() *prompt.Prompt {
	return prompt.New(strings.NewReader(""), &bytes.Buffer{}, true)
}

// A disk that already carries the layout is adopted, not installed onto: init
// identifies it itself and provisions nothing. Erasing it is a separate,
// explicit decision.
func TestChooseDiskAdoptsADiskThatCarriesTheLayout(t *testing.T) {
	p, out := asking("")

	disk, err := ChooseDisk(p, inventory(), machine.Selector{"serial": "ZA20ABCD"}, false)
	require.NoError(t, err)
	require.Equal(t, "sda", disk.Name)
	require.Contains(t, out.String(), "adopted as it is")
	require.Contains(t, out.String(), "--wipe")
}

// --wipe is the reinstall, and a reinstall needs installation media: init
// erases the disk first and only then looks for the UKI under /run/media.
func TestDiskNotesWarnThatWipeNeedsInstallationMedia(t *testing.T) {
	notes := DiskNotes(inventory(), inventory().Disks[1], true)
	require.Len(t, notes, 1)
	require.Contains(t, notes[0], "/run/media")
	require.Contains(t, notes[0], "nothing to boot from")
}

// The node identifies its disk by the layout before it reads any selector, so a
// second disk carrying one silently wins over what the document names.
func TestDiskNotesWarnAboutAnotherDiskCarryingTheLayout(t *testing.T) {
	notes := DiskNotes(inventory(), inventory().Disks[0], false)
	require.Len(t, notes, 1)
	require.Contains(t, notes[0], "sda also carries")
	require.Contains(t, notes[0], "Detach sda")
}

func TestDiskNotesAreSilentOnASingleBlankDisk(t *testing.T) {
	oneDisk := &machine.Inventory{Disks: []machine.Disk{{Name: "nvme0n1", State: machine.StateBlank}}}
	require.Empty(t, DiskNotes(oneDisk, oneDisk.Disks[0], false))
}

// The single blank disk is the default, and it is still confirmed by pressing
// enter on a list that shows what else the machine has.
func TestChooseDiskDefaultsToTheOnlyBlankDisk(t *testing.T) {
	p, out := asking("\n")

	disk, err := ChooseDisk(p, inventory(), nil, false)
	require.NoError(t, err)
	require.Equal(t, "nvme0n1", disk.Name)
	require.Contains(t, out.String(), "477Gi")
	require.Contains(t, out.String(), "holds an OS")
}

func TestChooseDiskRefusesToGuessBetweenTwoBlankDisks(t *testing.T) {
	twoBlank := inventory()
	twoBlank.Disks[1].State = machine.StateBlank

	_, err := ChooseDisk(assuming(), twoBlank, nil, false)
	require.ErrorIs(t, err, prompt.ErrNoDefault)
	require.ErrorContains(t, err, "--disk-selector")
}

func TestChooseDiskRefusesASelectorMatchingSeveralDisks(t *testing.T) {
	_, err := ChooseDisk(assuming(), inventory(), machine.Selector{"name": "*"}, false)
	require.ErrorContains(t, err, "matches 2 disks")
}

func TestChooseDiskRefusesASelectorMatchingNothing(t *testing.T) {
	_, err := ChooseDisk(assuming(), inventory(), machine.Selector{"serial": "nosuchserial"}, false)
	require.ErrorContains(t, err, "matches no disk")
}

// The interface the CLI reached the machine on is proven by the connection, so
// it is taken without a question.
func TestChooseInterfaceTakesTheOneTheAddressIsOn(t *testing.T) {
	iface, err := ChooseInterface(assuming(), inventory(), "10.12.4.55", "")
	require.NoError(t, err)
	require.Equal(t, "eno1", iface.Name)
}

func TestChooseInterfaceAsksWhenTheAddressIsOnNone(t *testing.T) {
	_, err := ChooseInterface(assuming(), inventory(), "192.0.2.10", "")
	require.ErrorIs(t, err, prompt.ErrNoDefault)
	require.ErrorContains(t, err, "--network-interface")

	p, out := asking("2\n")

	iface, err := ChooseInterface(p, inventory(), "192.0.2.10", "")
	require.NoError(t, err)
	require.Equal(t, "eno2", iface.Name)
	require.Contains(t, out.String(), "is not an address of any interface")
}

func TestChooseInterfaceRefusesAnInterfaceTheMachineLacks(t *testing.T) {
	_, err := ChooseInterface(assuming(), inventory(), "10.12.4.55", "eth9")
	require.ErrorContains(t, err, `no interface "eth9"`)
}

func TestBuildDocumentFillsTheMachineHalfIntoTheClusterHalf(t *testing.T) {
	document, err := BuildDocument(template(), Choices{
		NodeName:  "worker-1",
		Disk:      inventory().Disks[0],
		Selector:  machine.Selector{"serial": "S3Z8NB0K700002"},
		Interface: inventory().Interfaces[0],
	})
	require.NoError(t, err)

	object := map[string]any{}
	require.NoError(t, yaml.Unmarshal(document, &object))

	require.Equal(t, "internal.deckhouse.io/v1alpha1", object["apiVersion"])
	require.Equal(t, "NodeConfig", object["kind"])

	name, _, _ := unstructured.NestedString(object, "metadata", "name")
	require.Equal(t, "worker-1", name)

	nodeName, _, _ := unstructured.NestedString(object, "spec", "nodeName")
	require.Equal(t, "worker-1", nodeName)

	serial, _, _ := unstructured.NestedString(object, "spec", "storage", "diskSelector", "serial")
	require.Equal(t, "S3Z8NB0K700002", serial)

	// Nothing asked for an erase, so nothing in the document asks for one: with
	// wipe unset the node installs onto a blank disk and provisions nothing on
	// a disk that already carries the layout.
	_, found, _ := unstructured.NestedBool(object, "spec", "storage", "wipe")
	require.False(t, found)

	interfaces, _, _ := unstructured.NestedSlice(object, "spec", "network", "interfaces")
	require.Len(t, interfaces, 1)
	require.Equal(t, "eno1", interfaces[0].(map[string]any)["name"])
	require.Equal(t, true, interfaces[0].(map[string]any)["dhcp"])

	// The cluster's half has to survive: without the token kubelet has nothing
	// to present on first contact.
	token, _, _ := unstructured.NestedString(object, "spec", "kubelet", "bootstrapToken")
	require.Equal(t, bootstrapToken, token)
}

// A machine whose disk already carries the layout is handed no storage at all:
// the node identifies that disk itself, and a selector disagreeing with the pin
// recorded at install would send it through a reinstall it has no media for.
func TestBuildDocumentOmitsStorageForAnAdoptedDisk(t *testing.T) {
	document, err := BuildDocument(template(), Choices{
		NodeName:  "worker-1",
		Disk:      inventory().Disks[1],
		Selector:  machine.Selector{"serial": "ZA20ABCD"},
		Interface: inventory().Interfaces[0],
	})
	require.NoError(t, err)

	object := map[string]any{}
	require.NoError(t, yaml.Unmarshal(document, &object))

	_, found, _ := unstructured.NestedString(object, "spec", "storage", "diskSelector", "serial")
	require.False(t, found)

	_, found, _ = unstructured.NestedBool(object, "spec", "storage", "wipe")
	require.False(t, found)
}

// --wipe is the only thing that writes wipe, and it names the disk to erase.
func TestBuildDocumentWritesWipeOnlyWhenAskedTo(t *testing.T) {
	document, err := BuildDocument(template(), Choices{
		NodeName:  "worker-1",
		Disk:      inventory().Disks[1],
		Selector:  machine.Selector{"serial": "ZA20ABCD"},
		Wipe:      true,
		Interface: inventory().Interfaces[0],
	})
	require.NoError(t, err)

	object := map[string]any{}
	require.NoError(t, yaml.Unmarshal(document, &object))

	wipe, _, _ := unstructured.NestedBool(object, "spec", "storage", "wipe")
	require.True(t, wipe)

	serial, _, _ := unstructured.NestedString(object, "spec", "storage", "diskSelector", "serial")
	require.Equal(t, "ZA20ABCD", serial)
}

func TestBuildDocumentPinsTheAddressWhenAskedTo(t *testing.T) {
	document, err := BuildDocument(template(), Choices{
		NodeName:      "worker-1",
		Disk:          inventory().Disks[0],
		Selector:      machine.Selector{"serial": "S3Z8NB0K700002"},
		Interface:     inventory().Interfaces[0],
		StaticAddress: true,
	})
	require.NoError(t, err)

	object := map[string]any{}
	require.NoError(t, yaml.Unmarshal(document, &object))

	interfaces, _, _ := unstructured.NestedSlice(object, "spec", "network", "interfaces")
	iface := interfaces[0].(map[string]any)
	require.Equal(t, false, iface["dhcp"])
	require.Equal(t, []any{"10.12.4.55/24"}, iface["addresses"])
	require.Equal(t, "10.12.4.1", iface["gateway"])
}

// Reading a template is the right to add a node to the cluster: the three live
// secrets it carries never reach an output stream.
func TestRedactHidesEverySecretTheTemplateCarries(t *testing.T) {
	document, err := BuildDocument(template(), Choices{
		NodeName:  "worker-1",
		Disk:      inventory().Disks[0],
		Selector:  machine.Selector{"serial": "S3Z8NB0K700002"},
		Interface: inventory().Interfaces[0],
	})
	require.NoError(t, err)
	require.Contains(t, string(document), bootstrapToken)

	redacted, err := Redact(document)
	require.NoError(t, err)

	require.NotContains(t, string(redacted), bootstrapToken)
	require.NotContains(t, string(redacted), registryAuth)
	require.NotContains(t, string(redacted), proxyToken)
	require.Equal(t, 3, strings.Count(string(redacted), Redacted))

	// What is not a secret still has to be readable, or the redaction hid the
	// document rather than its secrets.
	require.Contains(t, string(redacted), "worker-1")
	require.Contains(t, string(redacted), "S3Z8NB0K700002")
	require.Contains(t, string(redacted), "maxPods")
}

func TestHumanSize(t *testing.T) {
	require.Equal(t, "477Gi", HumanSize(512110190592))
	require.Equal(t, "2Ti", HumanSize(2000398934016))
	require.Equal(t, "512B", HumanSize(512))
}
