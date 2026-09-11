package debugtar

import "testing"

// TestDebugCommandsModuleExpansionInvariant guards the contract documented on
// Command.RequiredModule: a command whose File or Args contains the
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
