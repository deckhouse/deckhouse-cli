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

package cr_test

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	cr "github.com/deckhouse/deckhouse-cli/internal/cr/cmd"
)

// completionLineValues parses cobra __complete output into the leading
// values of each suggestion line. Cobra prints "<value>\t<description>" per
// suggestion and finishes the stream with a ":<directive>" line - keeping
// only the first column makes assertions exact (no false matches against
// the human-readable description text).
func completionLineValues(out string) []string {
	var values []string
	for line := range strings.SplitSeq(out, "\n") {
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		values = append(values, strings.SplitN(line, "\t", 2)[0])
	}
	return values
}

// End-to-end checks of cobra's hidden __complete subcommand against the cr
// tree. These verify the wiring (ValidArgsFunction / RegisterFlagCompletionFunc)
// at the cobra layer - the unit-level coverage of the completion functions
// themselves lives in internal/cr/cmd/completion/.
//
// Two invocation styles are exercised:
//
//   - Direct: cmd := cr.NewCommand(); cmd.Execute() - the public API any
//     embedder (including d8) is expected to use.
//   - Nested: a synthetic d8-style root with cr added as a subcommand. This
//     guards against a regression where cobra would fail to reach our leaf
//     ValidArgsFunctions through one extra command level.

func TestComplete_DirectRoot(t *testing.T) {
	cmd := cr.NewCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"__complete", "pull", "--format", ""})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	values := completionLineValues(out.String())
	for _, w := range []string{"tarball", "legacy", "oci"} {
		if !slices.Contains(values, w) {
			t.Errorf("--format completion missing %q; values=%v\noutput:\n%s", w, values, out.String())
		}
	}
}

func TestComplete_NestedUnderD8Root(t *testing.T) {
	root := &cobra.Command{Use: "d8", Run: func(cmd *cobra.Command, _ []string) { _ = cmd.Help() }}
	root.AddCommand(cr.NewCommand())
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"__complete", "cr", "pull", "--format", ""})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v\noutput: %s", err, out.String())
	}
	values := completionLineValues(out.String())
	for _, w := range []string{"tarball", "legacy", "oci"} {
		if !slices.Contains(values, w) {
			t.Errorf("--format completion missing %q; values=%v\noutput:\n%s", w, values, out.String())
		}
	}
}
