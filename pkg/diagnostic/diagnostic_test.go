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
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHelpfulError_Error_PlainText(t *testing.T) {
	diag := &HelpfulError{
		Category:    "Network connection failed",
		OriginalErr: errors.New("connection refused"),
		Suggestions: []Suggestion{
			{Cause: "cause", Solutions: []string{"fix"}},
		},
	}

	errStr := diag.Error()
	assert.Equal(t, "Network connection failed: connection refused", errStr)
	assert.NotContains(t, errStr, "\033[")
	assert.NotContains(t, errStr, "cause")
}

func TestHelpfulError_Unwrap(t *testing.T) {
	originalErr := io.EOF
	diag := &HelpfulError{
		Category:    "Test",
		OriginalErr: originalErr,
	}
	require.True(t, errors.Is(diag, originalErr))
}

func TestHelpfulError_Format_NoColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	diag := &HelpfulError{
		Category:    "Network connection failed",
		OriginalErr: errors.New("test"),
		Suggestions: []Suggestion{
			{Cause: "cause1", Solutions: []string{"fix1"}},
		},
	}

	output := diag.Format()
	assert.NotContains(t, output, "\033[")
	assert.Contains(t, output, "Network connection failed")
	assert.Contains(t, output, "cause1")
	assert.Contains(t, output, "fix1")
}

func TestHelpfulError_Format_Structure(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	diag := &HelpfulError{
		Category:    "Network connection failed",
		OriginalErr: errors.New("connection refused"),
		Suggestions: []Suggestion{
			{Cause: "Network down", Solutions: []string{"Check network"}},
			{Cause: "Firewall blocking", Solutions: []string{"Check firewall"}},
		},
	}

	output := diag.Format()
	assert.Contains(t, output, "error: Network connection failed")
	assert.Contains(t, output, "connection refused")
	assert.Contains(t, output, "Network down")
	assert.Contains(t, output, "Check network")
	assert.Contains(t, output, "Firewall blocking")
	assert.Contains(t, output, "Check firewall")
}

func TestHelpfulError_Error_NilOriginalErr(t *testing.T) {
	diag := &HelpfulError{Category: "Something failed"}
	assert.Equal(t, "Something failed", diag.Error())
	assert.Nil(t, diag.Unwrap())
}

func TestHelpfulError_Format_NilOriginalErr(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	diag := &HelpfulError{
		Category: "Something failed",
		Suggestions: []Suggestion{
			{Cause: "Unknown", Solutions: []string{"Try again"}},
		},
	}

	output := diag.Format()
	assert.Contains(t, output, "Something failed")
	assert.Contains(t, output, "Try again")
	assert.NotContains(t, output, "╰─▶")
}

func TestHelpfulError_Format_NoCausesNoSolutions(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	diag := &HelpfulError{
		Category:    "Something failed",
		OriginalErr: errors.New("oops"),
	}

	output := diag.Format()
	assert.Contains(t, output, "Something failed")
	assert.Contains(t, output, "oops")
	assert.NotContains(t, output, "Possible causes")
	assert.NotContains(t, output, "How to fix")
}

func TestHelpfulError_Format_ForceColor(t *testing.T) {
	t.Setenv("FORCE_COLOR", "1")
	t.Setenv("NO_COLOR", "")

	diag := &HelpfulError{
		Category:    "Test error",
		OriginalErr: errors.New("test"),
		Suggestions: []Suggestion{
			{Cause: "cause1", Solutions: []string{"fix1"}},
		},
	}

	output := diag.Format()
	assert.True(t, strings.Contains(output, "\033["))
}
