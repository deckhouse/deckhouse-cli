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
		{"DiskFull", fmt.Errorf("write temp: %w", syscall.ENOSPC), categoryDiskFull},
		{"Permission", fmt.Errorf("open bundle: %w", os.ErrPermission), categoryPermission},
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

func TestDiagnose_PushSpecificAuth(t *testing.T) {
	diag := Diagnose(&transport.Error{StatusCode: http.StatusUnauthorized})
	require.NotNil(t, diag)

	solutions := allSolutions(diag)
	assert.Contains(t, solutions, "--registry-login")
	assert.Contains(t, solutions, "--registry-password")
	assert.NotContains(t, solutions, "--license")
	assert.NotContains(t, solutions, "--source-login")
}

func TestDiagnose_DiskFull(t *testing.T) {
	diag := Diagnose(fmt.Errorf("write temp: %w", syscall.ENOSPC))
	require.NotNil(t, diag)
	assert.Equal(t, categoryDiskFull, diag.Category)
	assert.NotEmpty(t, diag.Suggestions)
}

func TestDiagnose_PermissionDenied(t *testing.T) {
	diag := Diagnose(fmt.Errorf("open bundle: %w", os.ErrPermission))
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

func TestDiagnose_Unwrap(t *testing.T) {
	diag := Diagnose(io.EOF)
	require.NotNil(t, diag)

	var helpErr *diagnostic.HelpfulError
	require.True(t, errors.As(diag, &helpErr))
	assert.True(t, errors.Is(diag, io.EOF))
}
