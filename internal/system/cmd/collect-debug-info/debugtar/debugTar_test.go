package debugtar

import (
	"slices"
	"strings"
	"testing"
)

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
// dropping a field of Command that a later change adds.
func TestFilterAndExpandCommandsKeepsFields(t *testing.T) {
	source := Command{
		File:           "d8-{module-name}-ccm-logs.txt",
		Cmd:            "kubectl",
		Args:           []string{"-n", "d8-{module-name}", "logs"},
		RequiredModule: "cloud-provider",
	}

	expanded, _ := filterAndExpandCommands([]Command{source}, map[string]bool{"cloud-provider-aws": true}, true, nil)
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

// TestGetExcludableFilesAreUsableTokens guards the contract of --list-exclude:
// every printed name must be usable verbatim with --exclude, so none of them
// may carry an unresolved placeholder.
func TestGetExcludableFilesAreUsableTokens(t *testing.T) {
	tokens := GetExcludableFiles()
	if len(tokens) != len(debugCommands) {
		t.Errorf("got %d tokens for %d commands", len(tokens), len(debugCommands))
	}

	for _, token := range tokens {
		if strings.Contains(token, "{module-name}") {
			t.Errorf("token %q cannot be typed by a user", token)
		}
	}

	for _, want := range []string{"cluster-events.json", "ccm-logs", "csi-controller-logs"} {
		if !slices.Contains(tokens, want) {
			t.Errorf("token %q is missing from --list-exclude", want)
		}
	}
}

// TestExcludeAcceptedSpellings pins the accepted --exclude spellings and, just
// as importantly, the rejected ones: a group prefix must not drop a whole family
// of files. The per-module entries are checked through the real selection, since
// that is where a command is matched against the exclude list.
func TestExcludeAcceptedSpellings(t *testing.T) {
	templates := []Command{
		{File: "cluster-events.json", Cmd: "kubectl"},
		{
			File:           "d8-{module-name}-ccm-logs.txt",
			Cmd:            "kubectl",
			Args:           []string{"-n", "d8-{module-name}", "logs"},
			RequiredModule: "cloud-provider",
		},
	}
	activeModules := map[string]bool{"cloud-provider-aws": true, "cloud-provider-yandex": true}

	cases := []struct {
		token string
		want  []string
	}{
		{"", []string{"cluster-events.json", "d8-cloud-provider-aws-ccm-logs.txt", "d8-cloud-provider-yandex-ccm-logs.txt"}},
		{"ccm-logs", []string{"cluster-events.json"}},
		{"ccm-logs.txt", []string{"cluster-events.json"}},
		{"d8-cloud-provider-aws-ccm-logs.txt", []string{"cluster-events.json", "d8-cloud-provider-yandex-ccm-logs.txt"}},
		{"d8-cloud-provider-aws-ccm-logs", []string{"cluster-events.json", "d8-cloud-provider-yandex-ccm-logs.txt"}},
		{"cluster-events", []string{"d8-cloud-provider-aws-ccm-logs.txt", "d8-cloud-provider-yandex-ccm-logs.txt"}},
		{" cluster-events ", []string{"d8-cloud-provider-aws-ccm-logs.txt", "d8-cloud-provider-yandex-ccm-logs.txt"}},
		{"d8", []string{"cluster-events.json", "d8-cloud-provider-aws-ccm-logs.txt", "d8-cloud-provider-yandex-ccm-logs.txt"}},
		{"cluster", []string{"cluster-events.json", "d8-cloud-provider-aws-ccm-logs.txt", "d8-cloud-provider-yandex-ccm-logs.txt"}},
	}

	for _, tc := range cases {
		var exclude []string
		if tc.token != "" {
			exclude = []string{tc.token}
		}

		selected, _ := filterAndExpandCommands(templates, activeModules, true, newExcludeSet(exclude))

		got := make([]string, 0, len(selected))
		for _, cmd := range selected {
			got = append(got, cmd.File)
		}

		if !slices.Equal(got, tc.want) {
			t.Errorf("--exclude %q left %v, want %v", tc.token, got, tc.want)
		}
	}
}

// TestValidateExcludeNames checks that a name which can never match is reported
// instead of silently collecting everything, while a module-independent token
// stays valid on a cluster where that module is not enabled.
func TestValidateExcludeNames(t *testing.T) {
	_, accepted := filterAndExpandCommands(debugCommands, nil, false, nil)

	if err := validateExcludeNames([]string{"ccm-logs", "cluster-events", "cluster-events.json"}, accepted); err != nil {
		t.Errorf("valid names rejected: %v", err)
	}

	err := validateExcludeNames([]string{"d8"}, accepted)
	if err == nil {
		t.Fatal("group prefix d8 was accepted, it silently excludes nothing now")
	}

	if !strings.Contains(err.Error(), "did you mean") {
		t.Errorf("error %q offers no suggestion", err)
	}

	err = validateExcludeNames([]string{"ccm-log"}, accepted)
	if err == nil || !strings.Contains(err.Error(), "did you mean: ccm-logs") {
		t.Errorf("a near miss should be pointed at its name, got %v", err)
	}

	err = validateExcludeNames([]string{"zzzz"}, accepted)
	if err == nil {
		t.Fatal("a name matching nothing was accepted")
	}

	if strings.Contains(err.Error(), "did you mean") {
		t.Errorf("error %q invents a suggestion for a name with no near miss", err)
	}

	// A repeated or empty accepted name must not confuse the lookup.
	if err := validateExcludeNames([]string{"plain"}, []string{"plain.txt", "plain.txt", ""}); err != nil {
		t.Errorf("duplicate accepted names broke the lookup: %v", err)
	}
}

// TestExcludedEntryStaysAValidName guards the interaction between exclusion and
// validation: the name a user just excluded must not then be reported as
// unknown, even though its command is gone from the selection.
func TestExcludedEntryStaysAValidName(t *testing.T) {
	for _, name := range []string{"ccm-logs", "cluster-events.json", "d8-cloud-provider-aws-ccm-logs.txt"} {
		_, accepted := filterAndExpandCommands(debugCommands, map[string]bool{"cloud-provider-aws": true}, true, newExcludeSet([]string{name}))

		if err := validateExcludeNames([]string{name}, accepted); err != nil {
			t.Errorf("--exclude %q reported as unknown: %v", name, err)
		}
	}
}

// TestSelectionMatrix pins every combination of the two knobs documented on
// Command.RequiredModule: whether the command is gated on a module, whether it
// carries the {module-name} placeholder, and whether the module list could be
// fetched at all. The expected file lists also pin the order, which must stay
// deterministic (templates in declaration order, per-module copies sorted by
// module name) so two runs of the same cluster produce the same archive.
func TestSelectionMatrix(t *testing.T) {
	templates := []Command{
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
	template := Command{
		File:           "d8-{module-name}-logs.txt",
		Cmd:            "kubectl",
		Args:           []string{"-n", "d8-{module-name}", "logs"},
		RequiredModule: "cloud-provider",
	}

	selected, _ := filterAndExpandCommands([]Command{template}, map[string]bool{"cloud-provider-aws": true}, true, nil)
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
