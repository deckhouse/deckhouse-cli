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

// End-to-end counterpart of TestNewRootCommand_WritesNothingToStdout
// (cmd/d8/root_test.go): the same invariant, checked on the built binary
// instead of on NewRootCommand in-process.
//
// Both are needed. The unit test guards every CI run and fails fast on a
// regression in the command tree. This one is the only check that covers the
// artifact users actually get: build tags, linked-in libraries and anything
// that writes to file descriptor 1 without going through this package's
// logger.
//
// The regression (v0.22.9, commit cf9b345, ticket 503982, 12 Nov 2025): a
// kubeconfig issued by user-authn authenticates through an `exec` block, so
// client-go runs `d8` as a subprocess and decodes its *stdout* as an
// ExecCredential object (the payload comes from `d8 login get-token`,
// cmd/commands/login.go). In v0.22.8 startup printed
//
//	{"level":"warn","logger":"d8","msg":"Failed to read plugins directory",
//	 "error":"open /opt/deckhouse/lib/deckhouse-cli/plugins: no such file or directory"}
//
// to that same stdout, so client-go saw two JSON documents and every `d8 k`
// died with `decoding stdout: couldn't get version/kind; json parse error:
// invalid character '{' after top-level value`. The customer first noticed
// the stray line in the output of `d8 --version`, which is why that command
// is the probe here: its stdout is exactly one known line.
package auth

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// binEnv points the test at a specific binary, overriding the lookup below.
// Useful for checking a release artifact: D8_BIN=/path/to/d8 go test ...
const binEnv = "D8_BIN"

// repoRoot is the repository root relative to this package: go test runs with
// the package directory as the working directory.
const repoRoot = "../../.."

// TestD8Stdout_CleanWhenPluginsDirUnreadable runs the built binary with the
// plugins directory pointed at a path that can be neither created nor read -
// the field condition - and requires stdout to hold nothing but the output of
// the command that was asked for.
func TestD8Stdout_CleanWhenPluginsDirUnreadable(t *testing.T) {
	cmd := exec.Command(d8Binary(t), "--version")
	cmd.Env = append(os.Environ(),
		"DECKHOUSE_CLI_PATH="+unreadablePluginsPath(t),
		// Pin the level this test is about. At debug level the CLI does still
		// print JSON to stdout (cmd/d8/root.go: dkplog defaults to os.Stdout
		// and no WithOutput is passed) - a separate, still-open defect that
		// dkplog.WithOutput(os.Stderr) would close.
		"LOG_LEVEL=",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()

	// Asserted before the exit status: stdout cleanliness is the invariant,
	// and a binary that also fails for another reason must not hide it.
	assert.NotContains(t, stdout.String(), `{"level":`,
		"a JSON log line on stdout breaks kubeconfig exec authorization: client-go "+
			"decodes this stream as an ExecCredential; stdout:\n%s", stdout.String())

	require.NoError(t, runErr, "d8 --version must succeed; stderr:\n%s", stderr.String())

	lines := strings.Split(strings.TrimSuffix(stdout.String(), "\n"), "\n")
	require.Len(t, lines, 1,
		"stdout must carry exactly one line, the version output; got:\n%s", stdout.String())
	assert.Regexp(t, `^d8 version \S+$`, lines[0],
		"the only line on stdout must be the version output")
}

// d8Binary locates the binary under test: the D8_BIN override first, then the
// two places the Taskfile and testing/e2e/plugins/test.sh put builds. When
// none exists the test skips rather than fails - a plain `go test ./...` on a
// clean checkout has no binary to check, and the in-process unit test in
// cmd/d8 is what guards CI.
func d8Binary(t *testing.T) string {
	t.Helper()

	if fromEnv := os.Getenv(binEnv); fromEnv != "" {
		return fromEnv
	}

	candidates := []string{
		filepath.Join(repoRoot, "bin", "d8"),
		filepath.Join(repoRoot, "build", runtime.GOOS+"-"+runtime.GOARCH, "bin", "d8"),
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}

	t.Skipf("no d8 binary in %v; build one (task build:dev) or set %s", candidates, binEnv)

	return ""
}

// unreadablePluginsPath returns a path that neither os.MkdirAll nor
// os.ReadDir can succeed on, because its parent is a regular file (ENOTDIR).
// That reproduces the field's absent /opt/deckhouse/lib/deckhouse-cli for any
// user, including root - where a merely missing path would just be created.
func unreadablePluginsPath(t *testing.T) string {
	t.Helper()

	blocker := filepath.Join(t.TempDir(), "blocker")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))

	return filepath.Join(blocker, "deckhouse-cli")
}
