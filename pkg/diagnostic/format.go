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

package diagnostic

import (
	"errors"
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

// Semantic text styles used by HelpfulError.Format().
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

// Format returns the formatted diagnostic string with colors if stderr is a TTY.
//
//	error: Network connection failed to 127.0.0.1:443
//	  ╰─▶ pull from registry
//	    ╰─▶ validate platform access
//	      ╰─▶ dial tcp 127.0.0.1:443
//	        ╰─▶ connect: connection refused
//
//	  Possible causes:
//	    * Network connectivity issues or no internet connection
//
//	  How to fix:
//	    * Check your network connection and internet access
func (e *HelpfulError) Format() string {
	t := newTextStyler()

	var b strings.Builder
	b.WriteString("\n" + t.danger("error") + t.header(": "+e.Category) + "\n")
	if e.OriginalErr != nil {
		chain := unwrapChain(e.OriginalErr)
		for i, msg := range chain {
			indent := strings.Repeat("  ", i)
			b.WriteString("  " + indent + t.hint("╰─▶ ") + msg + "\n")
		}
	}
	b.WriteString("\n")

	if len(e.Causes) > 0 {
		b.WriteString(t.warn("  Possible causes:") + "\n")
		for _, cause := range e.Causes {
			b.WriteString("    * " + cause + "\n")
		}
		b.WriteString("\n")
	}

	if len(e.Solutions) > 0 {
		b.WriteString(t.hint("  How to fix:") + "\n")
		for _, solution := range e.Solutions {
			b.WriteString("    * " + solution + "\n")
		}
	}

	b.WriteString("\n")
	return b.String()
}

// unwrapChain walks errors.Unwrap() and extracts each level's unique context.
// For "a: b: c" wrapped via fmt.Errorf("%w"), returns ["a", "b", "c"].
func unwrapChain(err error) []string {
	var chain []string

	for err != nil {
		inner := errors.Unwrap(err)
		if inner == nil {
			chain = append(chain, err.Error())
			break
		}

		full := err.Error()
		innerText := inner.Error()
		context := strings.TrimSuffix(full, ": "+innerText)
		if context == full {
			// Can't extract prefix cleanly - use full message and stop
			chain = append(chain, full)
			break
		}

		chain = append(chain, context)
		err = inner
	}

	return chain
}
