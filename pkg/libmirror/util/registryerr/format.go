/*
Copyright 2024 Flant JSC

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

package registryerr

import (
	"os"
	"strings"

	"golang.org/x/term"
)

// ANSI escape codes for terminal color output.
const (
	ansiReset  = "\033[0m"
	ansiRed    = "\033[31m"
	ansiYellow = "\033[33m"
	ansiCyan   = "\033[36m"
	ansiBold   = "\033[1m"
)

// textStyler controls whether styled output uses ANSI codes or plain text.
type textStyler struct {
	enabled bool
}

// style wraps text with ANSI codes when enabled, returns plain text otherwise.
func (t textStyler) style(text string, ansiCodes ...string) string {
	if !t.enabled {
		return text
	}
	return strings.Join(ansiCodes, "") + text + ansiReset
}

// Semantic text styles used by Diagnostic.Format().
func (t textStyler) danger(s string) string { return t.style(s, ansiBold, ansiRed) } // error labels
func (t textStyler) header(s string) string { return t.style(s, ansiBold) }          // category name
func (t textStyler) hint(s string) string   { return t.style(s, ansiCyan) }          // arrows, solutions
func (t textStyler) warn(s string) string   { return t.style(s, ansiYellow) }        // possible causes

// newTextStyler returns a textStyler configured for the current environment.
// Colors are enabled when stderr is a TTY, unless overridden by NO_COLOR or FORCE_COLOR.
func newTextStyler() textStyler {
	if os.Getenv("NO_COLOR") != "" {
		return textStyler{}
	}
	return textStyler{
		enabled: os.Getenv("FORCE_COLOR") != "" || term.IsTerminal(int(os.Stderr.Fd())),
	}
}

// Diagnostic is a classified registry error with user-friendly diagnostics.
// It implements the error interface so it can propagate up the call chain and be
// printed once at the top level, avoiding double output.
type Diagnostic struct {
	Category    string   // e.g. "DNS resolution failed for 'registry.example.com'"
	OriginalErr error    // the underlying error from go-containerregistry or net
	Causes      []string // "Possible causes" shown to the user
	Solutions   []string // "How to fix" shown to the user
}

// Error returns a plain-text representation suitable for logging and error wrapping.
// Use Format() for user-facing colored terminal output.
func (d *Diagnostic) Error() string {
	return d.Category + ": " + d.OriginalErr.Error()
}

// Unwrap returns the original error so errors.Is/errors.As work through the diagnostic.
func (d *Diagnostic) Unwrap() error {
	return d.OriginalErr
}

// Format returns the formatted diagnostic string with colors if stderr is a TTY.
//
// Example:
//
//	error: Network connection failed to 127.0.0.1:443
//	  ╰─▶ dial tcp 127.0.0.1:443: connect: connection refused
//
//	  Possible causes:
//	    * Network connectivity issues or no internet connection
//
//	  How to fix:
//	    * Check your network connection and internet access
func (d *Diagnostic) Format() string {
	t := newTextStyler()

	var b strings.Builder
	b.WriteString("\n" + t.danger("error") + t.header(": "+d.Category) + "\n")
	b.WriteString(t.hint("  ╰─▶ ") + d.OriginalErr.Error() + "\n\n")

	if len(d.Causes) > 0 {
		b.WriteString(t.warn("  Possible causes:") + "\n")
		for _, cause := range d.Causes {
			b.WriteString("    * " + cause + "\n")
		}
		b.WriteString("\n")
	}

	if len(d.Solutions) > 0 {
		b.WriteString(t.hint("  How to fix:") + "\n")
		for _, solution := range d.Solutions {
			b.WriteString("    * " + solution + "\n")
		}
	}

	b.WriteString("\n")
	return b.String()
}
