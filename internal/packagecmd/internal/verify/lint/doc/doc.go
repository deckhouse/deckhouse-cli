// Package doc serves the reference documentation for the verify linters and their
// rules, as printed by `package doc`.
//
// Rule text lives here instead of in the individual linter packages so the whole
// catalog stays reviewable in one place. Everything that already exists elsewhere
// is referenced rather than restated: identifiers come from the linter packages
// and default severities from the settings package, so a documented ID or
// severity cannot drift from the one verify uses.
package doc

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
)

// Linter documents one linter: what it inspects and which rules it runs.
type Linter struct {
	// ID is the identifier used in .pkglint.yaml and in diagnostics.
	ID string
	// Summary is the one-line description shown when listing linters.
	Summary string
	// Description explains what the linter inspects, one paragraph per element.
	Description []string
	// Rules documents every rule the linter runs, in execution order.
	Rules []Rule
	// Notes hold caveats printed after the rule list.
	Notes []string
}

// Rule documents a single rule of a linter.
type Rule struct {
	// ID is the identifier used under the linter's rules key in .pkglint.yaml.
	ID string
	// Summary is the one-line description shown in the linter's rule list.
	Summary string
	// Description explains why the rule exists, one paragraph per element.
	Description []string
	// Reports lists the conditions under which the rule emits a finding.
	Reports []string
	// Example shows the rule in practice. Every rule carries one.
	Example Example
	// Fix tells the user how to satisfy the rule.
	Fix string
	// Tunable reports whether the rule carries its own severity. Rules that are
	// not tunable encode hard contracts and always run at the linter severity.
	Tunable bool
	// Impact is the built-in severity of a rule whose level is fixed in the rule
	// body rather than read from settings. It applies to the rules of a linter that
	// carries no settings at all, where there is no linter severity to inherit and
	// the rules do not all report at the same level. Leave it unset for a rule that
	// follows its linter.
	Impact *lint.Level
	// Notes hold caveats printed after the rule body.
	Notes []string
}

// Example contrasts a shape that makes a rule report with the shape that
// satisfies it. Lines are printed verbatim, so each element carries its own
// indentation — file trees, manifest fragments and file contents all appear
// as the user would write them.
type Example struct {
	// Reported is the shape the rule reports a finding for.
	Reported []string
	// Accepted is the shape that satisfies the rule.
	Accepted []string
}

// catalog is every documented linter, in the order verify runs them.
var catalog = []Linter{
	packageDoc,
	templatesDoc,
	docsDoc,
	imagesDoc,
	iconDoc,
	ossDoc,
}

// Options selects the documentation to print.
type Options struct {
	// Linter is the linter to document. Empty lists every linter instead.
	Linter string
	// Rule narrows the output to a single rule of Linter.
	Rule string
}

// Print writes the documentation selected by opts to w. An empty Options prints
// the linter list, which is what `package doc` without arguments shows.
func Print(w io.Writer, opts Options) error {
	if opts.Linter == "" {
		if opts.Rule != "" {
			return errors.New("--rule requires a linter argument")
		}

		printList(w)

		return nil
	}

	linter, err := Lookup(opts.Linter)
	if err != nil {
		return err
	}

	if opts.Rule == "" {
		printLinter(w, linter)

		return nil
	}

	rule, err := linter.Rule(opts.Rule)
	if err != nil {
		return err
	}

	printRule(w, linter, rule)

	return nil
}

// Lookup returns the documentation of the linter with the given ID.
func Lookup(linterID string) (Linter, error) {
	for _, linter := range catalog {
		if linter.ID == linterID {
			return linter, nil
		}
	}

	return Linter{}, fmt.Errorf("unknown linter %q, available linters: %s", linterID, strings.Join(LinterIDs(), ", "))
}

// LinterIDs returns the ID of every documented linter, in catalog order.
func LinterIDs() []string {
	ids := make([]string, 0, len(catalog))
	for _, linter := range catalog {
		ids = append(ids, linter.ID)
	}

	return ids
}

// RuleIDs returns the rule IDs of the linter with the given ID, or nothing when
// no such linter is documented. It backs shell completion of the --rule flag.
func RuleIDs(linterID string) []string {
	linter, err := Lookup(linterID)
	if err != nil {
		return nil
	}

	return linter.RuleIDs()
}

// Rule returns the documentation of one rule of the linter.
func (l Linter) Rule(ruleID string) (Rule, error) {
	for _, rule := range l.Rules {
		if rule.ID == ruleID {
			return rule, nil
		}
	}

	return Rule{}, fmt.Errorf("unknown rule %q for linter %q, available rules: %s",
		ruleID, l.ID, strings.Join(l.RuleIDs(), ", "))
}

// RuleIDs returns the ID of every rule the linter runs, in execution order.
func (l Linter) RuleIDs() []string {
	ids := make([]string, 0, len(l.Rules))
	for _, rule := range l.Rules {
		ids = append(ids, rule.ID)
	}

	return ids
}
