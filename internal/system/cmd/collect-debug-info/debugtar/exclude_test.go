package debugtar

import (
	"slices"
	"strings"
	"testing"
)

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
	templates := []command{
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
