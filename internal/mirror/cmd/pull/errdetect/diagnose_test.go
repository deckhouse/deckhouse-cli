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

package errdetect

import (
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/pkg/diagnostic"
)

func TestDiagnose_Nil(t *testing.T) {
	assert.Nil(t, Diagnose(nil))
}

func TestDiagnose_Unclassified(t *testing.T) {
	assert.Nil(t, Diagnose(errors.New("some random error")))
}

func TestDiagnose_AlreadyClassified(t *testing.T) {
	first := Diagnose(io.EOF)
	require.NotNil(t, first)
	assert.Nil(t, Diagnose(first))
}

func TestDiagnose_AllCategories(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		category string
	}{
		{"EOF", io.EOF, categoryEOF},
		{"TLS", fmt.Errorf("reg: %w", x509.UnknownAuthorityError{}), categoryTLS},
		{"Auth401", &transport.Error{StatusCode: http.StatusUnauthorized}, categoryAuth401},
		{"Auth403", &transport.Error{StatusCode: http.StatusForbidden}, categoryAuth403},
		{"RateLimit", &transport.Error{StatusCode: http.StatusTooManyRequests}, categoryRateLimit},
		{"Server500", &transport.Error{StatusCode: http.StatusInternalServerError}, categoryServerError},
		{"DiskFull", fmt.Errorf("write bundle: %w", syscall.ENOSPC), categoryDiskFull},
		{"Permission", fmt.Errorf("create file: %w", os.ErrPermission), categoryPermission},
		{"ImageNotFound", errors.New("MANIFEST_UNKNOWN: not found"), categoryImageNotFound},
		{"RepoNotFound", errors.New("NAME_UNKNOWN: repo"), categoryRepoNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diag := Diagnose(tt.err)
			require.NotNil(t, diag)
			assert.Contains(t, diag.Category, tt.category)
		})
	}
}

func TestDiagnose_PullSpecificAuth(t *testing.T) {
	diag := Diagnose(&transport.Error{StatusCode: http.StatusUnauthorized})
	require.NotNil(t, diag)

	solutions := allSolutions(diag)
	assert.Contains(t, solutions, "--license")
	assert.Contains(t, solutions, "--source-login")
	assert.NotContains(t, solutions, "--registry-login")
	assert.NotContains(t, solutions, "--registry-password")
}

func TestDiagnose_DiskFull(t *testing.T) {
	diag := Diagnose(fmt.Errorf("write bundle: %w", syscall.ENOSPC))
	require.NotNil(t, diag)
	assert.Equal(t, categoryDiskFull, diag.Category)
	assert.NotEmpty(t, diag.Suggestions)
}

func TestDiagnose_PermissionDenied(t *testing.T) {
	diag := Diagnose(fmt.Errorf("create file: %w", os.ErrPermission))
	require.NotNil(t, diag)
	assert.Equal(t, categoryPermission, diag.Category)
	assert.NotEmpty(t, diag.Suggestions)
}

func allSolutions(diag *diagnostic.HelpfulError) string {
	var parts []string
	for _, s := range diag.Suggestions {
		parts = append(parts, s.Solutions...)
	}
	return strings.Join(parts, " ")
}

func TestDiagnose_NoUnsupportedOCI(t *testing.T) {
	assert.Nil(t, Diagnose(errors.New("MANIFEST_INVALID: vnd.aquasec.trivy")))
}

func TestDiagnose_Unwrap(t *testing.T) {
	diag := Diagnose(io.EOF)
	require.NotNil(t, diag)

	var helpErr *diagnostic.HelpfulError
	require.True(t, errors.As(diag, &helpErr))
	assert.True(t, errors.Is(diag, io.EOF))
}

// --- DiagnoseConstraintParseError tests ---

func TestDiagnoseConstraintParseError_NilError(t *testing.T) {
	assert.Nil(t, DiagnoseConstraintParseError(nil, "include-module", "console@"))
}

func TestDiagnoseConstraintParseError_UnrelatedError(t *testing.T) {
	err := errors.New("something completely different")
	assert.Nil(t, DiagnoseConstraintParseError(err, "include-module", "console@"))
}

func TestDiagnoseConstraintParseError_MalformedConstraintIsNotDiagnosed(t *testing.T) {
	// A constraint that is present but invalid is the user's own typo.
	_, err := modules.NewFilter([]string{"console@^^1.0"}, modules.FilterTypeWhitelist)
	require.Error(t, err)
	assert.Nil(t, DiagnoseConstraintParseError(err, "include-module", "console@^^1.0"))
}

func TestDiagnoseConstraintParseError_EmptyConstraintButNoAtSuffix(t *testing.T) {
	// No raw value ends with '@', so the constraint was not cut off by the
	// shell. Return nil and let the caller use its own message.
	assert.Nil(t, DiagnoseConstraintParseError(modules.ErrEmptyConstraint, "include-module", "console"))
}

func TestDiagnoseConstraintParseError_RealFilterError(t *testing.T) {
	// shell-redirect scenario:
	//   --include-module console@>=1.43.2  (no quotes)
	// Shell hands cobra "console@" and redirects "=1.43.2" to a file.
	// The error must be recognised as produced by modules.NewFilter, not as a
	// string this package spells out for itself.
	_, err := modules.NewFilter([]string{"console@"}, modules.FilterTypeWhitelist)
	require.Error(t, err)

	diag := DiagnoseConstraintParseError(err, "include-module", "console@")
	require.NotNil(t, diag)
	assert.Equal(t, categoryEmptyConstraint, diag.Category)
	assert.True(t, errors.Is(diag, modules.ErrEmptyConstraint), "original error must stay reachable via errors.Is")
	require.NotEmpty(t, diag.Suggestions)
	assert.Contains(t, diag.Suggestions[0].Cause, `"console@"`, "cause should quote the value the parser rejected")

	solutions := allSolutions(diag)
	assert.Contains(t, solutions, "--include-module", "solution should name the affected flag")
	assert.Contains(t, solutions, `"console@>=1.43.2"`, "quotes go around the whole value, not just the constraint")
}

func TestDiagnoseConstraintParseError_NamesTheBrokenEntry(t *testing.T) {
	// Several --include-module entries; only one ends with '@'.
	diag := DiagnoseConstraintParseError(modules.ErrEmptyConstraint, "include-module", "cert-manager@^1.0.0", "console@ ", "ingress-nginx@~2.0.0")
	require.NotNil(t, diag)
	assert.Equal(t, categoryEmptyConstraint, diag.Category)
	assert.Contains(t, diag.Suggestions[0].Cause, `"console@"`, "cause should name the broken entry, trimmed, not the intact ones")
}

func TestDiagnoseConstraintParseError_CausesAreSingleLine(t *testing.T) {
	// Format() indents only the first line of a cause.
	diag := DiagnoseConstraintParseError(modules.ErrEmptyConstraint, "include-module", "console@")
	require.Len(t, diag.Suggestions, 2, "both the shell and the empty-variable cause are offered")

	for _, s := range diag.Suggestions {
		assert.NotContains(t, s.Cause, "\n", "cause must stay on one line")
	}
}

func TestDiagnoseConstraintParseError_FlagNameAppearsInSolution(t *testing.T) {
	for _, flagName := range []string{"exclude-module", "include-package", "exclude-package"} {
		diag := DiagnoseConstraintParseError(modules.ErrEmptyConstraint, flagName, "foo@")
		require.NotNil(t, diag)

		solutions := allSolutions(diag)
		assert.Contains(t, solutions, "--"+flagName, "solution text should reference the flag that was passed")
		assert.NotContains(t, solutions, "--include-module", "no solution may hardcode a different flag")
	}
}

// --- DiagnosePlatformConstraintParseError tests ---

func TestDiagnosePlatformConstraintParseError_NilError(t *testing.T) {
	assert.Nil(t, DiagnosePlatformConstraintParseError(nil, "./bundle"))
}

func TestDiagnosePlatformConstraintParseError_ConstraintTypoIsNotDiagnosed(t *testing.T) {
	// A malformed but path-less value is the user's own typo.
	for _, value := range []string{"1.64.", "^^1.0", ">=1.64 <=x"} {
		_, err := modules.ParseVersionConstraint(value)
		require.Error(t, err, "value %q should not parse", value)
		assert.Nil(t, DiagnosePlatformConstraintParseError(err, value), "value %q is not a path", value)
	}
}

func TestLooksLikePath(t *testing.T) {
	for _, v := range []string{"./bundle", "../bundle", "/tmp/bundle", "~/bundle", `.\bundle`, `..\bundle`, " ./bundle "} {
		assert.True(t, looksLikePath(v), "%q is a path", v)
	}

	// Constraint operators only. The tilde constraint is the trap: ~1.65.0 is
	// semver shorthand, not a home-relative path.
	for _, v := range []string{"~1.65.0", "^1.65.0", ">=1.64 <=1.68", "=v1.65.3", "1.65.0", "", "  "} {
		assert.False(t, looksLikePath(v), "%q is a constraint", v)
	}
}

func TestDiagnosePlatformConstraintParseError_BundlePathSwallowed(t *testing.T) {
	// shell-redirect scenario:
	//   --include-platform >=1.64 ./bundle  (no quotes)
	// Shell redirects ">=1.64" into a file, so the flag takes the bundle path.
	for _, path := range []string{"./bundle", "../bundle", "/tmp/bundle", "~/bundle"} {
		_, err := modules.ParseVersionConstraint(path)
		require.Error(t, err)

		diag := DiagnosePlatformConstraintParseError(err, path)
		require.NotNil(t, diag, "path-shaped value %q should be diagnosed", path)
		assert.Equal(t, categoryPathConstraint, diag.Category)
		assert.True(t, errors.Is(diag, err), "original error must be accessible via errors.Is")
		assert.Contains(t, diag.Suggestions[0].Cause, path, "cause should quote the value that landed in the flag")
		assert.Contains(t, allSolutions(diag), "--include-platform")
	}
}
