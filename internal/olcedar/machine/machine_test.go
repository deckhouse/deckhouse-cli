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

package machine

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const inventoryBody = `{
  "disks": [
    {"name": "nvme0n1", "size": 512110190592, "model": "MZVL2512", "serial": "S3Z8NB0K700002", "wwid": "eui.0025", "state": "blank"},
    {"name": "sda", "size": 2000398934016, "model": "ST2000DM008", "serial": "ZA20ABCD", "state": "system-layout"}
  ],
  "interfaces": [
    {"name": "eno1", "mac": "aa:bb:cc:dd:ee:01", "link": "up", "addresses": ["10.12.4.55/24"], "gateway": "10.12.4.1"}
  ]
}`

func serve(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return strings.TrimPrefix(server.URL, "http://")
}

func TestWhoamiReadsTheAnswer(t *testing.T) {
	address := serve(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, whoamiPath, r.URL.Path)
		_, _ = io.WriteString(w, WhoamiAgent+"\n")
	})

	who, err := Whoami(context.Background(), address)
	require.NoError(t, err)
	require.Equal(t, WhoamiAgent, who)
}

func TestFetchInventoryParsesTheWireContract(t *testing.T) {
	address := serve(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, inventoryPath, r.URL.Path)
		_, _ = io.WriteString(w, inventoryBody)
	})

	inventory, err := FetchInventory(context.Background(), address)
	require.NoError(t, err)
	require.Len(t, inventory.Disks, 2)
	require.Equal(t, StateBlank, inventory.Disks[0].State)
	require.Equal(t, "S3Z8NB0K700002", inventory.Disks[0].Serial)
	require.Equal(t, StateSystemLayout, inventory.Disks[1].State)
	require.Equal(t, []string{"10.12.4.55/24"}, inventory.Interfaces[0].Addresses)
}

// An image built before the endpoint answers 404, and this command has nothing
// to pick a disk out of then.
func TestFetchInventoryReportsAnImageWithoutTheEndpoint(t *testing.T) {
	address := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	_, err := FetchInventory(context.Background(), address)
	require.ErrorIs(t, err, ErrNoInventory)
}

func TestPushNodeConfigSendsTheDocument(t *testing.T) {
	var got []byte

	address := serve(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPut, r.Method)
		require.Equal(t, configPath, r.URL.Path)
		require.Equal(t, "application/yaml", r.Header.Get("Content-Type"))

		got, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	require.NoError(t, PushNodeConfig(context.Background(), address, []byte("kind: NodeConfig\n")))
	require.Equal(t, "kind: NodeConfig\n", string(got))
}

func TestPushNodeConfigReportsAnInstalledNode(t *testing.T) {
	address := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})

	err := PushNodeConfig(context.Background(), address, []byte("kind: NodeConfig\n"))
	require.ErrorIs(t, err, ErrAlreadyConfigured)
}

func TestAddressKeepsAPortAndAddsTheDefaultOne(t *testing.T) {
	require.Equal(t, "10.12.4.55:"+MaintenancePort, Address("10.12.4.55"))
	require.Equal(t, "10.12.4.55:9000", Address("10.12.4.55:9000"))
	require.Equal(t, "10.12.4.55", Host("10.12.4.55:50000"))
}

func TestParseSelectorRefusesWhatMatchesNothing(t *testing.T) {
	selector, err := ParseSelector("serial=S3Z8NB0K700002")
	require.NoError(t, err)
	require.Equal(t, Selector{"serial": "S3Z8NB0K700002"}, selector)

	_, err = ParseSelector("serial")
	require.Error(t, err)

	_, err = ParseSelector("colour=blue")
	require.Error(t, err)

	_, err = ParseSelector("serial=")
	require.Error(t, err)
}

func TestSelectorMatchesByGlob(t *testing.T) {
	disks := []Disk{
		{Name: "nvme0n1", Serial: "S3Z8NB0K700002", WWID: "eui.0025"},
		{Name: "sda", Serial: "ZA20ABCD"},
	}

	matched, err := Selector{"serial": "S3Z*"}.Match(disks)
	require.NoError(t, err)
	require.Len(t, matched, 1)
	require.Equal(t, "nvme0n1", matched[0].Name)

	matched, err = Selector{"name": "*"}.Match(disks)
	require.NoError(t, err)
	require.Len(t, matched, 2)
}

// The document has to survive the machine renaming sda to sdb between boots,
// so the most stable attribute the disk reports is the one written down.
func TestSelectorForPrefersTheStablestAttribute(t *testing.T) {
	require.Equal(t, Selector{"wwid": "eui.0025"}, SelectorFor(Disk{Name: "nvme0n1", Serial: "S3Z", WWID: "eui.0025"}))
	require.Equal(t, Selector{"serial": "S3Z"}, SelectorFor(Disk{Name: "nvme0n1", Serial: "S3Z"}))
	require.Equal(t, Selector{"busPath": "pci-0000:00"}, SelectorFor(Disk{Name: "sda", BusPath: "pci-0000:00"}))
	require.Equal(t, Selector{"name": "sda"}, SelectorFor(Disk{Name: "sda"}))
}
