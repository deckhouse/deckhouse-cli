package doc

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/fatih/color"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
)

// Text styles, kept in line with the diagnostic output of `package verify`.
var (
	bold = color.New(color.Bold).SprintFunc()
	dim  = color.New(color.Faint).SprintFunc()
)

const (
	// wrapWidth is the column at which prose is wrapped.
	wrapWidth = 88
	// indent is the left margin of every section body.
	indent = "  "
	// bullet marks one entry of a list.
	bullet = "• "
)

// impactLegend explains what a configured impact does to a finding.
const impactLegend = "impact caps the severity of a finding: ignored hides it, " +
	"warn reports it without failing, error fails verification."

// printList writes a one-line summary of every documented linter.
func printList(w io.Writer) {
	fmt.Fprintln(w, bold("LINTERS"))
	fmt.Fprintln(w)

	width := 0
	for _, linter := range catalog {
		width = max(width, len(linter.ID))
	}

	for _, linter := range catalog {
		fmt.Fprintf(w, "%s%s  %s\n", indent, pad(linter.ID, width), linter.Summary)
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, indent+dim("Run 'package doc <linter>' to see what a linter checks."))
}

// printLinter writes the full documentation of one linter: what it checks, the
// rules it runs with their default severities, and how to configure it.
func printLinter(w io.Writer, linter Linter) {
	scopes := scopesOf(linter.ID)

	meta := []string{}
	if impact := linterImpact(linter.ID); impact != "" {
		meta = append(meta, "default impact: "+impact)
	}

	meta = append(meta, scopeLines(scopes)...)

	printHeader(w, linter.ID, linter.Summary, meta)
	printParagraphs(w, linter.Description)
	printRuleList(w, linter)

	if linterImpact(linter.ID) != "" {
		printSection(w, "CONFIGURE", configSnippet(linter.ID, ""))
		printScopes(w, scopes)
	}

	printNotes(w, linter.Notes)
	printFooter(w, fmt.Sprintf("Run 'package doc %s --rule <rule>' for a single rule.", linter.ID))
}

// printRuleList writes the rules of a linter as an aligned three-column table of
// identifier, default severity and summary.
func printRuleList(w io.Writer, linter Linter) {
	fmt.Fprintln(w, bold("RULES"))
	fmt.Fprintln(w)

	idWidth, impactWidth := 0, 0

	for _, rule := range linter.Rules {
		idWidth = max(idWidth, len(rule.ID))
		impactWidth = max(impactWidth, len(ruleImpactLabel(linter.ID, rule)))
	}

	for _, rule := range linter.Rules {
		fmt.Fprintf(w, "%s%s  %s  %s\n",
			indent,
			pad(rule.ID, idWidth),
			dim(pad(ruleImpactLabel(linter.ID, rule), impactWidth)),
			rule.Summary)
	}

	fmt.Fprintln(w)
}

// printRule writes the full documentation of one rule of a linter.
func printRule(w io.Writer, linter Linter, rule Rule) {
	scopes := ruleScopesOf(linter.ID, rule.ID)

	impact := ruleImpactLine(linter.ID, rule)

	printHeader(w, linter.ID+" "+dim("·")+" "+rule.ID, rule.Summary,
		append([]string{impact}, scopeLines(scopes)...))
	printParagraphs(w, rule.Description)
	printBullets(w, "REPORTS", rule.Reports)
	printExample(w, rule.Example)

	if rule.Fix != "" {
		printSection(w, "FIX", wrap(rule.Fix, wrapWidth-cells(indent)))
	}

	if rule.Tunable {
		printSection(w, "CONFIGURE", configSnippet(linter.ID, rule.ID))
		printScopes(w, scopes)
	}

	printNotes(w, rule.Notes)
	printFooter(w, impactLegend)
}

// printHeader writes the title line of an entry — the identifier in bold followed
// by its summary — and one dimmed metadata line, such as severity or targets, below it.
func printHeader(w io.Writer, title, summary string, meta []string) {
	fmt.Fprintf(w, "%s%s  %s\n", indent, bold(title), summary)

	for _, line := range meta {
		fmt.Fprintf(w, "%s%s\n", indent, dim(line))
	}

	fmt.Fprintln(w)
}

// printParagraphs writes wrapped prose, one blank line between paragraphs.
func printParagraphs(w io.Writer, paragraphs []string) {
	for _, paragraph := range paragraphs {
		for _, line := range wrap(paragraph, wrapWidth-cells(indent)) {
			fmt.Fprintln(w, indent+line)
		}

		fmt.Fprintln(w)
	}
}

// printSection writes a titled block whose body lines are printed verbatim.
func printSection(w io.Writer, title string, body []string) {
	if len(body) == 0 {
		return
	}

	fmt.Fprintln(w, bold(title))
	fmt.Fprintln(w)

	for _, line := range body {
		fmt.Fprintln(w, indent+line)
	}

	fmt.Fprintln(w)
}

// printBullets writes a titled list, wrapping every entry with a hanging indent.
func printBullets(w io.Writer, title string, items []string) {
	if len(items) == 0 {
		return
	}

	fmt.Fprintln(w, bold(title))
	fmt.Fprintln(w)

	hanging := strings.Repeat(" ", cells(bullet))

	for _, item := range items {
		for i, line := range wrap(item, wrapWidth-cells(indent)-cells(bullet)) {
			prefix := bullet
			if i > 0 {
				prefix = hanging
			}

			fmt.Fprintln(w, indent+prefix+line)
		}
	}

	fmt.Fprintln(w)
}

// printExample writes the reported and accepted shapes of a rule, each labelled
// and printed verbatim so indentation-sensitive snippets survive.
func printExample(w io.Writer, example Example) {
	if len(example.Reported) == 0 && len(example.Accepted) == 0 {
		return
	}

	fmt.Fprintln(w, bold("EXAMPLE"))
	fmt.Fprintln(w)

	printExampleBlock(w, "reported", example.Reported)
	printExampleBlock(w, "accepted", example.Accepted)
}

// printExampleBlock writes one labelled half of an example.
func printExampleBlock(w io.Writer, label string, lines []string) {
	if len(lines) == 0 {
		return
	}

	fmt.Fprintln(w, indent+dim(label))

	for _, line := range lines {
		fmt.Fprintln(w, indent+indent+line)
	}

	fmt.Fprintln(w)
}

// printNotes writes the caveats of an entry, if it has any.
func printNotes(w io.Writer, notes []string) {
	printBullets(w, "NOTES", notes)
}

// printFooter writes the dimmed closing hint of an entry.
func printFooter(w io.Writer, text string) {
	for _, line := range wrap(text, wrapWidth-cells(indent)) {
		fmt.Fprintln(w, indent+dim(line))
	}
}

// printScopes writes the .pkglint.yaml keys the configuration snippet applies to,
// listing only the scopes the entry is actually processed in.
func printScopes(w io.Writer, scopes lint.TypeScopes) {
	fmt.Fprintln(w, indent+dim("scopes: "+strings.Join(configPathsOf(scopes), ", ")))
	fmt.Fprintln(w)
}

// configSnippet returns a .pkglint.yaml excerpt that sets the severity of a
// linter or, when ruleID is set, of that single rule.
func configSnippet(linterID, ruleID string) []string {
	lines := []string{
		"<scope>:",
		"  linters:",
		"    " + linterID + ":",
	}

	if ruleID == "" {
		return append(lines, "      impact: warn")
	}

	return append(lines,
		"      rules:",
		"        "+ruleID+":",
		"          impact: warn")
}

// ruleImpactLabel returns the severity shown for a rule in a linter's rule list.
// A rule that is neither tunable nor fixed in code follows its linter, which is
// stated instead of a level.
func ruleImpactLabel(linterID string, rule Rule) string {
	if rule.Tunable {
		return ruleImpact(linterID, rule.ID)
	}

	if rule.Impact != nil {
		return rule.Impact.String()
	}

	return "linter"
}

// ruleImpactLine returns the severity line shown in a rule's header. It spells out
// which of the three severity sources governs the rule: its own configurable
// setting, a level fixed in code, or the severity of its linter.
func ruleImpactLine(linterID string, rule Rule) string {
	if rule.Tunable {
		return "default impact: " + ruleImpact(linterID, rule.ID)
	}

	if rule.Impact != nil {
		return "impact: not tunable, always " + rule.Impact.String()
	}

	return fmt.Sprintf("impact: not tunable, always the one of the %s linter", linterID)
}

// pad right-pads str with spaces to width, leaving longer strings untouched.
func pad(str string, width int) string {
	if cells(str) >= width {
		return str
	}

	return str + strings.Repeat(" ", width-cells(str))
}

// wrap breaks text into lines no wider than width, splitting on spaces. A word
// wider than width keeps its own line rather than being cut.
func wrap(text string, width int) []string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}

	lines := make([]string, 0, len(words))
	line := words[0]

	for _, word := range words[1:] {
		if cells(line)+1+cells(word) > width {
			lines = append(lines, line)
			line = word

			continue
		}

		line += " " + word
	}

	return append(lines, line)
}

// cells returns the number of terminal columns str occupies. Documentation text
// carries em dashes, bullets and cyrillic examples, so counting bytes would
// wrap and pad those lines short.
func cells(str string) int {
	return utf8.RuneCountInString(str)
}
