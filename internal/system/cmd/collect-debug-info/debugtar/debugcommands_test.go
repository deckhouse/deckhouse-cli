package debugtar

import (
	"slices"
	"strings"
	"testing"
)

// TestDebugCommandsModuleExpansionInvariant guards the contract documented on
// command.RequiredModule: a command whose File or Args contains the
// {module-name} placeholder must also set RequiredModule, since that is what
// filterAndExpandCommands uses to resolve the placeholder into a real module
// name. Without RequiredModule, needsModuleExpansion is never even checked,
// so the placeholder would leak into the collected archive as a literal
// string instead of a resolved module name.
func TestDebugCommandsModuleExpansionInvariant(t *testing.T) {
	for _, cmd := range debugCommands {
		if cmd.RequiredModule != "" {
			continue
		}

		if !needsModuleExpansion(cmd) {
			continue
		}

		t.Errorf("command %q uses the {module-name} placeholder but has no RequiredModule set, so it will never be resolved", cmd.File)
	}
}

// TestBashPipelinesSetPipefail guards the exit status the collection sees. In a
// pipeline bash reports only the status of the last stage, so
// `kubectl get ... | jq ...` exits 0 when kubectl fails and jq happily consumes
// the empty input: ExecCommandInPod returns no error, the ERROR branch in
// runCommands never fires, the failure is absent from collection-errors.txt,
// and the archive gets an empty file indistinguishable from "the resource
// exists but holds nothing". `set -o pipefail` is what makes that failure
// observable.
//
// The check deliberately over-approximates: a "|" inside a quoted regexp or
// jsonpath counts as a pipeline too. Deciding otherwise would mean parsing
// shell here, and an extra `set -o pipefail` costs nothing.
func TestBashPipelinesSetPipefail(t *testing.T) {
	for _, cmd := range slices.Concat(debugCommands, virtualizationCommands) {
		if cmd.Cmd != "bash" {
			continue
		}

		for _, arg := range cmd.Args {
			if !hasShellPipeline(arg) || strings.Contains(arg, "set -o pipefail") {
				continue
			}

			t.Errorf("command %q runs a pipeline without `set -o pipefail`, a failure of its left-hand side would be collected as an empty file: %s", cmd.File, arg)
		}
	}
}

// hasShellPipeline reports whether script contains a "|" that is not part of
// the "||" operator.
func hasShellPipeline(script string) bool {
	for i := 0; i < len(script); i++ {
		if script[i] != '|' {
			continue
		}

		if i+1 < len(script) && script[i+1] == '|' {
			i++
			continue
		}

		if i > 0 && script[i-1] == '|' {
			continue
		}

		return true
	}

	return false
}
