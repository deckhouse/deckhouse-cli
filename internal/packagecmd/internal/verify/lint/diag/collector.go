package diag

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/fatih/color"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint"
)

// Collector is a scoped handle over a shared error store. Each With* call returns
// a shallow copy with updated context fields; all copies share the same *store,
// so findings from every linter/rule scope land in one collection.
type Collector struct {
	store    *store
	current  context
	maxLevel *lint.Level
}

// context carries the diagnostic coordinates stamped onto every finding emitted
// through this Collector handle.
type context struct {
	packageID string
	linterID  string
	ruleID    string
	objectID  string

	rootPath   string
	path       string
	lineNumber int
	value      any
}

// NewCollector returns a root Collector with an empty store and maxLevel defaulting to Error.
func NewCollector() *Collector {
	return &Collector{
		store:    new(store),
		maxLevel: lint.Error.Ptr(),
	}
}

// CopyOption is a functional option applied to a shallow copy of a Collector.
// Use the package-level constructors (LinterID, RuleID, Path, etc.) to build options.
type CopyOption func(*Collector)

// With returns a shallow copy of r with all opts applied.
// The copy shares the same underlying store, so findings still land in one collection.
func (r *Collector) With(opts ...CopyOption) *Collector {
	if r.store == nil {
		r.store = new(store)
	}

	c := *r
	for _, opt := range opts {
		opt(&c)
	}

	return &c
}

// MaxLevel caps finding severity to level — downgrades errors to warnings when
// a rule's configured impact is lower than what the rule itself reports.
func MaxLevel(level *lint.Level) CopyOption {
	return func(r *Collector) {
		if level == nil {
			return
		}

		if r.maxLevel != nil && *r.maxLevel < *level {
			return
		}

		r.maxLevel = level
	}
}

// PackageID sets the packageID context field.
func PackageID(packageID string) CopyOption {
	return func(r *Collector) { r.current.packageID = packageID }
}

// LinterID sets the linterID context field.
func LinterID(linterID string) CopyOption {
	return func(r *Collector) { r.current.linterID = linterID }
}

// RuleID sets the ruleID context field.
func RuleID(ruleID string) CopyOption {
	return func(r *Collector) { r.current.ruleID = ruleID }
}

// ObjectID sets the objectID context field.
func ObjectID(objectID string) CopyOption {
	return func(r *Collector) { r.current.objectID = objectID }
}

// Value sets the offending value context field.
func Value(value any) CopyOption {
	return func(r *Collector) { r.current.value = value }
}

// Path sets the path context field.
func Path(path string) CopyOption {
	return func(r *Collector) { r.current.path = path }
}

// RootPath sets the package root path. Relative paths from rules are joined with
// this root when findings are rendered, so the user sees a full clickable location.
func RootPath(rootPath string) CopyOption {
	return func(r *Collector) { r.current.rootPath = rootPath }
}

// LineNumber sets the lineNumber context field.
func LineNumber(lineNumber int) CopyOption {
	return func(r *Collector) { r.current.lineNumber = lineNumber }
}

// Warn records a warning-level finding with the current context.
func (r *Collector) Warn(str string, args ...any) {
	r.commit(fmt.Sprintf(str, args...), lint.Warn)
}

// Error records an error-level finding with the current context.
func (r *Collector) Error(str string, args ...any) {
	r.commit(fmt.Sprintf(str, args...), lint.Error)
}

func (r *Collector) commit(str string, level lint.Level) {
	if r.store == nil {
		r.store = new(store)
	}

	// Downgrade level if the rule's configured impact is lower than requested.
	if r.maxLevel != nil && *r.maxLevel < level {
		level = *r.maxLevel
	}

	r.store.addLog(errLog{
		context: r.current,
		message: str,
		level:   level,
	})
}

// HasErrors reports whether any finding at Error level was committed to the store.
func (r *Collector) HasErrors() bool {
	if r.store == nil {
		return false
	}

	for _, log := range r.store.getLogs() {
		if log.level == lint.Error {
			return true
		}
	}

	return false
}

// Print writes all collected findings to stdout, most-severe first, followed by a
// one-line summary. Ignored findings are skipped unless showIgnored is true;
// warnings are skipped if hideWarns is true. Counts in the summary always reflect
// the full set, regardless of filtering.
func (r *Collector) Print(showIgnored, hideWarns bool) {
	logs := r.store.getLogs()
	if len(logs) == 0 {
		return
	}

	slices.SortFunc(logs, func(a, b errLog) int {
		return cmp.Or(
			cmp.Compare(b.level, a.level),
			cmp.Compare(a.packageID, b.packageID),
			cmp.Compare(a.linterID, b.linterID),
			cmp.Compare(a.ruleID, b.ruleID),
			cmp.Compare(a.path, b.path),
			cmp.Compare(a.lineNumber, b.lineNumber),
		)
	})

	var (
		errCount, warnCount, ignoredCount int
		shown                             int
	)

	for _, log := range logs {
		switch log.level {
		case lint.Error:
			errCount++
		case lint.Warn:
			warnCount++

			if hideWarns {
				continue
			}
		case lint.Ignored:
			ignoredCount++

			if !showIgnored {
				continue
			}
		}

		if shown > 0 {
			renderSeparator(os.Stdout)
		}

		renderFinding(os.Stdout, log)

		shown++
	}

	if shown > 0 {
		fmt.Fprintln(os.Stdout)
	}

	renderSummary(os.Stdout, errCount, warnCount, ignoredCount)
}

// styleSpec holds the glyph and color applied to a finding based on its severity.
type styleSpec struct {
	glyph string
	color *color.Color
}

// levelStyles maps severity levels to their visual presentation.
var levelStyles = map[lint.Level]styleSpec{
	lint.Error:   {glyph: "✖", color: color.New(color.FgRed)},
	lint.Warn:    {glyph: "⚠", color: color.New(color.FgYellow)},
	lint.Ignored: {glyph: "·", color: color.New(color.Faint)},
}

// styleFor returns the styleSpec for level, falling back to the Error style.
func styleFor(level lint.Level) styleSpec {
	if s, ok := levelStyles[level]; ok {
		return s
	}

	return levelStyles[lint.Error]
}

// renderFinding writes a single finding: the coloured message with severity
// glyph and inline linter/rule tag, then one `→` bullet per metadata point.
func renderFinding(w io.Writer, log errLog) {
	style := styleFor(log.level)
	dim := color.New(color.Faint).SprintFunc()

	header := fmt.Sprintf("%s %s",
		style.color.Sprint(style.glyph),
		style.color.Sprint(strings.TrimSpace(log.message)))

	if id := identifierOf(log); id != "" {
		header += "  " + dim("["+id+"]")
	}

	fmt.Fprintln(w, header)

	arrow := dim("→")

	if loc := locationOf(log); loc != "" {
		fmt.Fprintf(w, "  %s path:    %s\n", arrow, loc)
	}

	if log.objectID != "" && log.objectID != log.packageID {
		fmt.Fprintf(w, "  %s object:  %s\n", arrow, log.objectID)
	}

	if log.value != nil {
		fmt.Fprintf(w, "  %s value:   %v\n", arrow, log.value)
	}

	if log.packageID != "" && log.packageID != log.objectID {
		fmt.Fprintf(w, "  %s package: %s\n", arrow, log.packageID)
	}
}

// renderSeparator writes the between-findings horizontal rule.
func renderSeparator(w io.Writer) {
	dim := color.New(color.Faint).SprintFunc()

	fmt.Fprintln(w)
	fmt.Fprintln(w, dim(strings.Repeat("-", 40)))
	fmt.Fprintln(w)
}

// locationOf returns the relativized `path[:line]` for a finding, joining rootPath
// with rule-relative paths first. Empty when no path is available — the caller
// suppresses the location line in that case.
func locationOf(log errLog) string {
	if log.path == "" {
		return ""
	}

	loc := strings.TrimSpace(log.path)
	if log.rootPath != "" && !filepath.IsAbs(loc) {
		loc = filepath.Join(log.rootPath, loc)
	}

	loc = relativize(loc)

	if log.lineNumber > 0 {
		return fmt.Sprintf("%s:%d", loc, log.lineNumber)
	}

	return loc
}

// relativize returns loc made relative to the current working directory when the
// result stays at or below CWD (no leading `..`). Otherwise loc is returned as-is.
func relativize(loc string) string {
	if !filepath.IsAbs(loc) {
		return loc
	}

	cwd, err := os.Getwd()
	if err != nil {
		return loc
	}

	rel, err := filepath.Rel(cwd, loc)
	if err != nil || strings.HasPrefix(rel, "..") {
		return loc
	}

	return rel
}

// identifierOf joins linter and rule IDs as `linter/rule`, tolerating an empty ruleID.
func identifierOf(log errLog) string {
	switch {
	case log.linterID != "" && log.ruleID != "":
		return log.linterID + "/" + log.ruleID
	case log.linterID != "":
		return log.linterID
	default:
		return log.ruleID
	}
}

// renderSummary writes a one-line tally; segments are coloured only when their count is non-zero.
func renderSummary(w io.Writer, errs, warns, ignored int) {
	red := color.New(color.FgRed).SprintfFunc()
	yellow := color.New(color.FgYellow).SprintfFunc()
	faint := color.New(color.Faint).SprintfFunc()

	var parts []string

	errLabel := fmt.Sprintf("%d %s", errs, plural(errs, "error", "errors"))
	if errs > 0 {
		errLabel = red("%s", errLabel)
	}

	parts = append(parts, errLabel)

	warnLabel := fmt.Sprintf("%d %s", warns, plural(warns, "warning", "warnings"))
	if warns > 0 {
		warnLabel = yellow("%s", warnLabel)
	}

	parts = append(parts, warnLabel)

	if ignored > 0 {
		parts = append(parts, faint("%d ignored", ignored))
	}

	fmt.Fprintln(w, strings.Join(parts, ", "))
}

// plural returns singular when n == 1, otherwise plural.
func plural(n int, singular, plural string) string {
	if n == 1 {
		return singular
	}

	return plural
}
