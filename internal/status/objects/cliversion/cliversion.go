/*
Copyright 2025 Flant JSC

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
	"fmt"
	"strings"

	"github.com/fatih/color"

	"github.com/deckhouse/deckhouse-cli/internal/status/tools/statusresult"
	"github.com/deckhouse/deckhouse-cli/internal/version"
)

// Status reports the version of the running d8 CLI binary.
// This is a property of the local binary, so no cluster access is needed.
func Status() statusresult.StatusResult {
	return statusresult.StatusResult{
		Title:  "Deckhouse CLI Version",
		Level:  0,
		Output: formatCLIVersion(version.Version),
	}
}

// Format returns a readable view of the CLI version for CLI display.
func formatCLIVersion(v string) string {
	yellow := color.New(color.FgYellow).SprintFunc()

	var sb strings.Builder
	sb.WriteString(yellow("┌ Deckhouse CLI Version:\n"))
	fmt.Fprintf(&sb, "%s %s\n", yellow("└"), v)

	return sb.String()
}
