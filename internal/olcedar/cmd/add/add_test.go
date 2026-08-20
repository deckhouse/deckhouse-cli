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

package add

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/olcedar/machine"
	"github.com/deckhouse/deckhouse-cli/internal/olcedar/prompt"
)

func assuming() *prompt.Prompt {
	return prompt.New(strings.NewReader(""), io.Discard, true)
}

// Nothing is chosen for the operator: with no terminal to ask on and no --yes,
// the command refuses instead of picking a disk itself.
func TestValidateRefusesWithoutATerminalAndWithoutYes(t *testing.T) {
	err := validate(&options{}, false)
	require.ErrorContains(t, err, "--yes")
	require.ErrorContains(t, err, "--disk-selector")

	require.NoError(t, validate(&options{yes: true}, false))
	require.NoError(t, validate(&options{}, true))
}

func TestValidateRefusesAnUnknownNetworkMode(t *testing.T) {
	require.ErrorContains(t, validate(&options{network: "bridge", yes: true}, false), "--network")
	require.NoError(t, validate(&options{network: networkDHCP, yes: true}, false))
	require.NoError(t, validate(&options{network: networkStatic, yes: true}, false))
}

func TestParseSelectorPassesAnEmptyFlagThrough(t *testing.T) {
	selector, err := parseSelector("")
	require.NoError(t, err)
	require.Nil(t, selector)

	selector, err = parseSelector("serial=S3Z8NB0K700002")
	require.NoError(t, err)
	require.Equal(t, machine.Selector{"serial": "S3Z8NB0K700002"}, selector)
}

func TestChooseStaticFollowsTheFlag(t *testing.T) {
	iface := machine.Interface{Name: "eno1", Addresses: []string{"10.12.4.55/24"}}

	static, err := chooseStatic(assuming(), iface, networkDHCP)
	require.NoError(t, err)
	require.False(t, static)

	static, err = chooseStatic(assuming(), iface, networkStatic)
	require.NoError(t, err)
	require.True(t, static)

	_, err = chooseStatic(assuming(), machine.Interface{Name: "eno2"}, networkStatic)
	require.ErrorContains(t, err, "holds none")
}

// Left unsaid, the network stays on DHCP: pinning an address is offered, never
// assumed.
func TestChooseStaticDefaultsToDHCP(t *testing.T) {
	static, err := chooseStatic(assuming(), machine.Interface{Name: "eno1", Addresses: []string{"10.12.4.55/24"}}, "")
	require.NoError(t, err)
	require.False(t, static)
}
