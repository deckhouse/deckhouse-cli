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

package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
)

// TestNewRootCommand_WritesNothingToStdout is the regression test for the
// v0.22.9 fix (commit cf9b345, ticket 503982, 12 Nov 2025).
//
// A kubeconfig issued by user-authn authenticates through an `exec` block:
// client-go runs `d8` as a subprocess and decodes its *stdout* as an
// ExecCredential object (the payload comes from `d8 login get-token`,
// cmd/commands/login.go). In v0.22.8 startup printed
//
//	{"level":"warn","logger":"d8","msg":"Failed to read plugins directory",
//	 "error":"open /opt/deckhouse/lib/deckhouse-cli/plugins: no such file or directory"}
//
// to that same stdout, so client-go saw two JSON documents and every `d8 k`
// died with `decoding stdout: couldn't get version/kind; json parse error:
// invalid character '{' after top-level value`.
//
// The invariant: building the command tree writes nothing to stdout, even
// when the plugins directory cannot be read. The plugins directory is read
// during construction (root.go:97 registerCommands -> installedPlugins ->
// layout.ResolveInstalled), and root.go:67 builds the logger that used to
// carry the message - so this test covers both halves of the failure.
func TestNewRootCommand_WritesNothingToStdout(t *testing.T) {
	// NewRootCommand overwrites this package-level global from the
	// environment (root.go:92-95).
	restore := flags.DeckhousePluginsDir
	t.Cleanup(func() { flags.DeckhousePluginsDir = restore })

	t.Setenv(flags.EnvPluginsDir, unreadablePluginsPath(t))

	// Pin the level this test is about. root.go:67 takes it from the
	// environment, and a developer with LOG_LEVEL=debug exported would
	// otherwise get a red test for a different, still-open defect: at debug
	// level root.go:257 does print JSON to stdout, because dkplog defaults to
	// os.Stdout (deckhouse/pkg/log logger.go:110) and nothing here passes
	// WithOutput. That one is fixed by dkplog.WithOutput(os.Stderr), not by
	// this test.
	t.Setenv("LOG_LEVEL", "")

	stdout := captureStdout(t, func() {
		NewRootCommand()
	})

	assert.Empty(t, stdout,
		"CLI startup must leave stdout untouched: anything here is prepended to "+
			"the ExecCredential JSON and breaks kubeconfig exec authorization")
}

// TestCaptureStdout_SeesLoggerOutput is the sensitivity control. dkplog
// defaults to os.Stdout (deckhouse/pkg/log logger.go:110) and nothing in this
// repository passes WithOutput, so a logger built the way root.go:67 builds
// one lands in the capture. Without this check the test above could pass
// because the capture is broken rather than because stdout is clean.
func TestCaptureStdout_SeesLoggerOutput(t *testing.T) {
	stdout := captureStdout(t, func() {
		// Same construction as root.go:67, then the call that broke v0.22.8.
		dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn)).
			Named("d8").
			Warn("Failed to read plugins directory")
	})

	assert.Contains(t, stdout, `"msg":"Failed to read plugins directory"`,
		"the capture must see a log line written the way v0.22.8 wrote it")
}

// captureStdout swaps os.Stdout for a pipe, runs fn, and returns everything
// fn wrote there. The swap has to happen before fn builds any logger: dkplog
// resolves os.Stdout once, at logger construction.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	reader, writer, err := os.Pipe()
	require.NoError(t, err)

	saved := os.Stdout
	os.Stdout = writer

	// Drain concurrently so fn cannot block on a full pipe buffer.
	captured := make(chan string, 1)
	go func() {
		var sb strings.Builder
		_, _ = io.Copy(&sb, reader)
		captured <- sb.String()
	}()

	defer func() {
		os.Stdout = saved
		_ = reader.Close()
	}()

	fn()

	require.NoError(t, writer.Close())

	return <-captured
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
