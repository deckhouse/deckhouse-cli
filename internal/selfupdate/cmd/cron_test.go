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

package selfupdatecmd

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/autoupdate"
)

func TestCronCommandPrintsCrontabInstructions(t *testing.T) {
	cmd := newCronCommand()

	var buf bytes.Buffer

	cmd.SetOut(&buf)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Execute())

	out := buf.String()

	assert.Contains(t, out, ") | crontab -", "carries a ready-to-paste crontab line")
	assert.Contains(t, out, ` cli update >>"$HOME/.cache/deckhouse-cli/auto-update.log" 2>&1`)
	assert.Regexp(t, `\b[0-9]{1,2} 6 \* \* \*`, out, "daily schedule with a random minute at 06")
	assert.Contains(t, out, d8PathForCron(), "the crontab line carries the stable binary path")
	assert.Contains(t, out, "Verify:")
	assert.Contains(t, out, "Disable:")
	assert.Contains(t, out, autoupdate.EnvDisableAutoUpdate, "mentions the plugin auto-update opt-out")
}

func TestD8PathForCronUsesArg0WhenPathlike(t *testing.T) {
	prev := os.Args[0]
	t.Cleanup(func() { os.Args[0] = prev })

	path := filepath.Join(t.TempDir(), "d8")
	os.Args[0] = path

	assert.Equal(t, path, d8PathForCron())
}

func TestD8PathForCronLooksUpBareNameWithoutResolvingSymlinks(t *testing.T) {
	prev := os.Args[0]
	t.Cleanup(func() { os.Args[0] = prev })

	// A PATH entry that is a symlink (exactly how a store-managed install looks):
	// the returned path must stay the symlink, never its target - a resolved path
	// would pin the cron job to the version that happened to be current.
	dir := t.TempDir()
	target := filepath.Join(dir, "real-binary")
	require.NoError(t, os.WriteFile(target, []byte("#!/bin/sh\n"), 0o755))

	link := filepath.Join(dir, "d8")
	require.NoError(t, os.Symlink(target, link))

	t.Setenv("PATH", dir)

	os.Args[0] = "d8"

	assert.Equal(t, link, d8PathForCron(), "the PATH symlink must not be resolved into its target")
}
