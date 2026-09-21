package debugtar

import (
	"slices"
	"testing"
)

// TestFilterAndExpandCommandsWithoutModuleList covers the degraded path taken
// when `kubectl get module` fails: the collection must go on, but no command
// may reach the archive with an unresolved {module-name} placeholder in its
// file name.
func TestFilterAndExpandCommandsWithoutModuleList(t *testing.T) {
	commands, _ := filterAndExpandCommands(debugCommands, nil, false, nil)

	var expected int

	for _, cmd := range debugCommands {
		if !needsModuleExpansion(cmd) {
			expected++
		}
	}

	if len(commands) != expected {
		t.Errorf("got %d commands without a module list, want %d", len(commands), expected)
	}

	for _, cmd := range commands {
		if needsModuleExpansion(cmd) {
			t.Errorf("command %q still carries an unresolved {module-name} placeholder", cmd.File)
		}
	}
}

// TestFilterAndExpandCommandsKeepsFields guards the expansion against silently
// dropping a field of command that a later change adds.
func TestFilterAndExpandCommandsKeepsFields(t *testing.T) {
	source := command{
		File:           "d8-{module-name}-ccm-logs.txt",
		Cmd:            "kubectl",
		Args:           []string{"-n", "d8-{module-name}", "logs"},
		RequiredModule: "cloud-provider",
	}

	expanded, _ := filterAndExpandCommands([]command{source}, map[string]bool{"cloud-provider-aws": true}, true, nil)
	if len(expanded) != 1 {
		t.Fatalf("got %d expanded commands, want 1", len(expanded))
	}

	got := expanded[0]
	if got.File != "d8-cloud-provider-aws-ccm-logs.txt" {
		t.Errorf("File = %q, want %q", got.File, "d8-cloud-provider-aws-ccm-logs.txt")
	}

	if got.Cmd != source.Cmd || got.RequiredModule != source.RequiredModule {
		t.Errorf("expansion dropped a field: %+v", got)
	}
}

// TestSelectionMatrix pins every combination of the two knobs documented on
// command.RequiredModule: whether the command is gated on a module, whether it
// carries the {module-name} placeholder, and whether the module list could be
// fetched at all. The expected file lists also pin the order, which must stay
// deterministic (templates in declaration order, per-module copies sorted by
// module name) so two runs of the same cluster produce the same archive.
func TestSelectionMatrix(t *testing.T) {
	templates := []command{
		{File: "plain.txt", Cmd: "kubectl"},
		{File: "literal-{module-name}.txt", Cmd: "kubectl"},
		{File: "gated.txt", Cmd: "kubectl", RequiredModule: "istio"},
		{
			File:           "d8-{module-name}-logs.txt",
			Cmd:            "kubectl",
			Args:           []string{"-n", "d8-{module-name}", "logs"},
			RequiredModule: "cloud-provider",
		},
	}

	cases := []struct {
		name          string
		activeModules map[string]bool
		modulesKnown  bool
		want          []string
	}{
		{
			name:          "gate matched by prefix and by exact name",
			activeModules: map[string]bool{"istio": true, "cloud-provider-yandex": true, "cloud-provider-aws": true},
			modulesKnown:  true,
			want: []string{
				"plain.txt",
				"literal-{module-name}.txt",
				"gated.txt",
				"d8-cloud-provider-aws-logs.txt",
				"d8-cloud-provider-yandex-logs.txt",
			},
		},
		{
			name:          "no module matches the gate",
			activeModules: map[string]bool{"cert-manager": true},
			modulesKnown:  true,
			want:          []string{"plain.txt", "literal-{module-name}.txt"},
		},
		{
			name:          "no module is Ready",
			activeModules: map[string]bool{},
			modulesKnown:  true,
			want:          []string{"plain.txt", "literal-{module-name}.txt"},
		},
		{
			name:         "module list unavailable",
			modulesKnown: false,
			want:         []string{"plain.txt", "literal-{module-name}.txt", "gated.txt"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			selected, _ := filterAndExpandCommands(templates, tc.activeModules, tc.modulesKnown, nil)

			got := make([]string, 0, len(selected))
			for _, cmd := range selected {
				got = append(got, cmd.File)
			}

			if !slices.Equal(got, tc.want) {
				t.Errorf("selected %v, want %v", got, tc.want)
			}
		})
	}
}

// TestExpansionSubstitutesArgs checks the other half of the substitution: the
// module name must reach Args too, not just the archive entry name.
func TestExpansionSubstitutesArgs(t *testing.T) {
	template := command{
		File:           "d8-{module-name}-logs.txt",
		Cmd:            "kubectl",
		Args:           []string{"-n", "d8-{module-name}", "logs"},
		RequiredModule: "cloud-provider",
	}

	selected, _ := filterAndExpandCommands([]command{template}, map[string]bool{"cloud-provider-aws": true}, true, nil)
	if len(selected) != 1 {
		t.Fatalf("got %d commands, want 1", len(selected))
	}

	want := []string{"-n", "d8-cloud-provider-aws", "logs"}
	if !slices.Equal(selected[0].Args, want) {
		t.Errorf("Args = %v, want %v", selected[0].Args, want)
	}

	if !slices.Equal(template.Args, []string{"-n", "d8-{module-name}", "logs"}) {
		t.Errorf("expansion mutated the template Args: %v", template.Args)
	}
}
