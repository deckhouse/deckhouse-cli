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

// Package machine talks to a machine waiting for its node configuration on the
// maintenance port. The wire contract mirrors dhctl/pkg/immutable of the
// deckhouse repository (inventory.go, push.go, constants.go), which the
// installer uses for the same three calls.
package machine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"
)

// MaintenancePort is where olcedar-init waits for a node configuration. It
// closes the moment a document is accepted.
const MaintenancePort = "50000"

const (
	inventoryPath = "/inventory.json"
	configPath    = "/config"
	whoamiPath    = "/whoami"
)

// The two answers /whoami gives.
const (
	WhoamiInstaller = "installer"
	WhoamiAgent     = "agent"
)

const (
	whoamiTimeout = 5 * time.Second
	// pushTimeout bounds one PUT: the machine writes the document to its config
	// partition before answering, which is a disk write.
	pushTimeout = 30 * time.Second
)

// maxErrorBody caps how much of a failing response is quoted back.
const maxErrorBody = 512

// Disk states the machine reports, which the size cannot say.
const (
	StateBlank        = "blank"
	StateFormatted    = "formatted"
	StateSystemLayout = "system-layout"
)

// ErrNoInventory means the machine serves no inventory: an image too old to
// have the endpoint answers 404 to it.
var ErrNoInventory = errors.New("the machine serves no inventory")

// ErrAlreadyConfigured means the port is held by the agent of an installed
// node, which will not take a second configuration without a maintenance token.
var ErrAlreadyConfigured = errors.New("the machine is already a node")

// Inventory is what a machine says about itself before anything is installed.
// The shape is the wire contract: Inventory in images/init/src/0.1/inventory.go
// of the initramfs repository.
type Inventory struct {
	Disks      []Disk      `json:"disks"`
	Interfaces []Interface `json:"interfaces"`
}

type Disk struct {
	Name       string      `json:"name"`
	Size       uint64      `json:"size"`
	Model      string      `json:"model"`
	Vendor     string      `json:"vendor"`
	Serial     string      `json:"serial"`
	WWID       string      `json:"wwid"`
	Rotational bool        `json:"rotational"`
	Transport  string      `json:"transport"`
	ByPath     string      `json:"byPath"`
	ByID       []string    `json:"byId"`
	BusPath    string      `json:"busPath"`
	State      string      `json:"state"`
	Partitions []Partition `json:"partitions"`
}

type Partition struct {
	Name   string `json:"name"`
	Size   uint64 `json:"size"`
	FSType string `json:"fsType"`
	Label  string `json:"label"`
}

type Interface struct {
	Name      string   `json:"name"`
	MAC       string   `json:"mac"`
	Link      string   `json:"link"`
	Addresses []string `json:"addresses"`
	Gateway   string   `json:"gateway"`
	Source    string   `json:"source"`
}

// Address adds the maintenance port to a bare host, and keeps one already
// written with a port.
func Address(hostOrAddress string) string {
	if _, _, err := net.SplitHostPort(hostOrAddress); err == nil {
		return hostOrAddress
	}

	return net.JoinHostPort(hostOrAddress, MaintenancePort)
}

// Host is the address without its port, as the cluster spells a node address.
func Host(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}

	return host
}

// Whoami tells which of the two servers holds the maintenance port: the
// installer waiting for a configuration, or the agent of an installed node.
func Whoami(ctx context.Context, address string) (string, error) {
	body, err := get(ctx, address, whoamiPath, whoamiTimeout)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(body)), nil
}

// FetchInventory reads what the machine says about its own hardware. An image
// too old to serve the endpoint answers 404, which is ErrNoInventory: there is
// then nothing to pick a disk out of.
func FetchInventory(ctx context.Context, address string) (*Inventory, error) {
	body, err := get(ctx, address, inventoryPath, pushTimeout)
	if err != nil {
		return nil, err
	}

	inventory := &Inventory{}
	if err := json.Unmarshal(body, inventory); err != nil {
		return nil, fmt.Errorf("read the inventory of %s: %w", address, err)
	}

	return inventory, nil
}

// PushNodeConfig hands the machine the document it boots from. The endpoint is
// unauthenticated by design — the machine holds no secret at this point — so
// the caller answers for the network the address lives on.
func PushNodeConfig(ctx context.Context, address string, document []byte) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://"+address+configPath, bytes.NewReader(document))
	if err != nil {
		return fmt.Errorf("build the push request for %s: %w", address, err)
	}

	request.Header.Set("Content-Type", "application/yaml")

	response, err := do(request, pushTimeout)
	if err != nil {
		return fmt.Errorf("push the node configuration to %s: %w", address, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("push the node configuration to %s: %w", address, ErrAlreadyConfigured)
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("push the node configuration to %s: %s: %s", address, response.Status, errorBody(response))
	}

	return nil
}

func get(ctx context.Context, address, urlPath string, timeout time.Duration) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+urlPath, nil)
	if err != nil {
		return nil, fmt.Errorf("build the request for %s%s: %w", address, urlPath, err)
	}

	response, err := do(request, timeout)
	if err != nil {
		return nil, fmt.Errorf("read %s%s: %w", address, urlPath, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode == http.StatusNotFound && urlPath == inventoryPath {
		return nil, fmt.Errorf("%w: %s answered 404 to %s, which an image built before the endpoint does", ErrNoInventory, address, urlPath)
	}

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s answered %s to %s: %s", address, response.Status, urlPath, errorBody(response))
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read the answer of %s%s: %w", address, urlPath, err)
	}

	return body, nil
}

func do(request *http.Request, timeout time.Duration) (*http.Response, error) {
	client := &http.Client{Timeout: timeout}
	defer client.CloseIdleConnections()

	return client.Do(request)
}

func errorBody(response *http.Response) string {
	body, err := io.ReadAll(io.LimitReader(response.Body, maxErrorBody))
	if err != nil {
		return "the refusal could not be read: " + err.Error()
	}

	return string(bytes.TrimSpace(body))
}

// Selector picks a disk by the attributes the machine reports. Every field set
// must match, shell-style patterns included, the way DiskSelector of the
// NodeConfig CRD is matched on the node itself.
type Selector map[string]string

// SelectorKeys are the attributes a selector may name, in the spelling the
// NodeConfig spec.storage.diskSelector uses.
var SelectorKeys = []string{"serial", "wwid", "name", "busPath", "model"}

// ParseSelector reads a "key=value" pair into a selector.
func ParseSelector(raw string) (Selector, error) {
	key, value, found := strings.Cut(raw, "=")
	if !found {
		return nil, fmt.Errorf("disk selector %q is not key=value, where key is one of %s", raw, strings.Join(SelectorKeys, ", "))
	}

	key, value = strings.TrimSpace(key), strings.TrimSpace(value)
	if !slices.Contains(SelectorKeys, key) {
		return nil, fmt.Errorf("disk selector key %q is none of %s", key, strings.Join(SelectorKeys, ", "))
	}

	if value == "" {
		return nil, fmt.Errorf("disk selector %q has an empty value, which matches nothing", raw)
	}

	return Selector{key: value}, nil
}

// Match lists the disks the selector describes. Mirrors matchDisk in
// images/init/src/0.1/disk.go of the initramfs repository: the machine matches
// these fields itself and the two must not disagree.
func (s Selector) Match(disks []Disk) ([]Disk, error) {
	var matched []Disk

	for _, disk := range disks {
		ok, err := s.matches(disk)
		if err != nil {
			return nil, err
		}

		if ok {
			matched = append(matched, disk)
		}
	}

	return matched, nil
}

func (s Selector) matches(disk Disk) (bool, error) {
	for key, pattern := range s {
		ok, err := path.Match(pattern, attribute(key, disk))
		if err != nil {
			return false, fmt.Errorf("disk selector %s=%q is not a valid pattern: %w", key, pattern, err)
		}

		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func attribute(key string, disk Disk) string {
	switch key {
	case "serial":
		return disk.Serial
	case "wwid":
		return disk.WWID
	case "name":
		return disk.Name
	case "busPath":
		return disk.BusPath
	case "model":
		return disk.Model
	default:
		panic("unknown disk selector key " + key)
	}
}

// SelectorFor names the disk by the most stable attribute it reports, so the
// document survives the machine renaming sda to sdb between boots.
func SelectorFor(disk Disk) Selector {
	for _, key := range []string{"wwid", "serial", "busPath", "name"} {
		if value := attribute(key, disk); value != "" {
			return Selector{key: value}
		}
	}

	return Selector{"name": disk.Name}
}
