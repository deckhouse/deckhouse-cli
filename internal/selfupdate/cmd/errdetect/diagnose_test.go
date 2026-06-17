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

package errdetect

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/rpp"
	"github.com/deckhouse/deckhouse-cli/pkg/diagnostic"
)

func TestDiagnose(t *testing.T) {
	cases := []struct {
		name     string
		sentinel error
		wantCat  string
		wantSol  string
	}{
		{"401", rpp.ErrUnauthorized, "unauthorized (401)", "OIDC"},
		{"403", rpp.ErrForbidden, "forbidden (403)", "cli-download"},
		{"404", rpp.ErrNotFound, "version not found (404)", "d8 cli versions"},
		{"5xx", rpp.ErrUpstream, "upstream error (5xx)", "registry-packages-proxy pods"},
		{"discovery", rpp.ErrEndpointDiscovery, "endpoint discovery via the Kubernetes API failed", "--rpp-endpoint"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			he := Diagnose(fmt.Errorf("GET /v1/images/deckhouse-cli/tags: %w", tc.sentinel))
			require.NotNil(t, he)
			assert.Contains(t, he.Category, tc.wantCat)
			require.Len(t, he.Suggestions, 1)
			assert.Contains(t, strings.Join(he.Suggestions[0].Solutions, " "), tc.wantSol)
			assert.ErrorIs(t, he, tc.sentinel, "the original cause is preserved")
		})
	}
}

func TestDiagnoseReturnsNil(t *testing.T) {
	assert.Nil(t, Diagnose(nil))
	assert.Nil(t, Diagnose(errors.New("some other failure")), "an unrecognized error is left alone")
	assert.Nil(t, Diagnose(&diagnostic.HelpfulError{Category: "preexisting"}), "an already-diagnosed error is left alone")
}
