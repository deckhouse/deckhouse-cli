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

package cliversion

import (
	"strings"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/version"
)

func TestFormatCLIVersion(t *testing.T) {
	output := formatCLIVersion("v0.32.0")

	for _, want := range []string{"┌ Deckhouse CLI Version:", "└ v0.32.0"} {
		if !strings.Contains(output, want) {
			t.Errorf("formatCLIVersion output missing %q:\n%s", want, output)
		}
	}
}

func TestStatusReportsInjectedVersion(t *testing.T) {
	orig := version.Version
	t.Cleanup(func() { version.Version = orig })

	version.Version = "v9.9.9-test"
	result := Status()

	if result.Title != "Deckhouse CLI Version" {
		t.Errorf("Title = %q, want %q", result.Title, "Deckhouse CLI Version")
	}

	if !strings.Contains(result.Output, "v9.9.9-test") {
		t.Errorf("output missing injected version:\n%s", result.Output)
	}
}
