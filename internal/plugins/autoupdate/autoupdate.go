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

// Package autoupdate schedules the background auto-update of installed plugins.
// It is invoked from the root command hook after ordinary commands. It owns its
// own throttle marker and detached spawn - the only background process it starts
// is the ordinary, visible `d8 plugins update all`, which the root hook gates out
// of further background work so it can never recurse.
package autoupdate

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

const (
	// EnvDisableAutoUpdate turns off the background plugin auto-update. It is an
	// independent per-mechanism switch: it has no effect on the d8 self-update
	// notice, and the self-update switch has no effect here.
	EnvDisableAutoUpdate = "D8_DISABLE_PLUGIN_AUTO_UPDATE"

	// pluginUpdateCheckTTL bounds how often the background plugin update may run, so
	// ordinary use does not spawn an update on every invocation.
	pluginUpdateCheckTTL = 6 * time.Hour

	markerFile = "plugin-update-check.json"
)

// ScheduleBackgroundUpdate spawns a detached `d8 plugins update all` to bring the
// installed plugins to their newest cluster-compatible version within the current
// major, at most once per pluginUpdateCheckTTL. It never blocks the command the
// user ran and is silent (the child's stdio is discarded). It is skipped when
// disabled by env, on Windows, when the last check is still fresh, or when no
// plugins are installed.
func ScheduleBackgroundUpdate(logger *dkplog.Logger, pluginDir string) {
	if os.Getenv(EnvDisableAutoUpdate) != "" || runtime.GOOS == "windows" {
		logger.Debug("background plugin update disabled by env or platform")

		return
	}

	path, err := markerPath()
	if err != nil {
		logger.Debug("skipping background plugin update: no marker path", dkplog.Err(err))

		return
	}

	// The throttle marker is checked before anything else: it is one small file
	// read, while the installed-plugins check below scans plugin directories -
	// that cost must not be paid on every invocation inside the TTL window.
	if m, err := loadMarker(path); err == nil && !m.isStale() {
		logger.Debug("background plugin update throttled (checked recently)")

		return
	}

	// Nothing installed - nothing to update; do not fork a child. Also do not
	// stamp: the first command after a future install should fire immediately,
	// not wait out a TTL started when there was nothing to update.
	if !hasInstalledPlugins(pluginDir) {
		logger.Debug("background plugin update skipped: no installed plugins")

		return
	}

	// Stamp the check time BEFORE spawning so a burst of commands forks at most one
	// child, and a child that cannot reach the cluster still backs off for the full
	// TTL even if its own run fails. If the marker cannot be persisted the throttle
	// is broken (every invocation would fork a child) - fail closed, never storm.
	if err := saveMarker(path, marker{CheckedAt: time.Now()}); err != nil {
		logger.Debug("skipping background plugin update: cannot write throttle marker", dkplog.Err(err))

		return
	}

	if err := spawnDetached("plugins", "update", "all"); err != nil {
		logger.Debug("could not start background plugin update", dkplog.Err(err))
	}
}

// hasInstalledPlugins reports whether at least one plugin is installed, so the
// background update is not forked when there is nothing to update. It checks both
// the configured dir and the home fallback (~/.deckhouse-cli): a non-root user on a
// root-owned deploy dir installs into the fallback via ensureInstallRoot, but the
// foreground process never applies that fallback, so the configured dir alone would
// look empty and the background update would silently never fire.
func hasInstalledPlugins(pluginDir string) bool {
	if layout.RootHasInstall(pluginDir) {
		return true
	}

	if fallback, err := layout.HomeFallbackPath(); err == nil && fallback != pluginDir {
		return layout.RootHasInstall(fallback)
	}

	return false
}

// spawnDetached starts `d8 plugins update all` as a detached background child and
// releases the handle, so the parent exits immediately while the orphan finishes.
// No recursion guard is set on the child: the spawned command is a plain `plugins`
// invocation, which the root hook gates out of all background work (it cannot spawn
// a grandchild). The child inherits the environment but not the parent's flags.
func spawnDetached(arg ...string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}

	cmd := exec.Command(exe, arg...)

	// werf's process-exterminator would SIGINT this orphan ~1s after the
	// short-lived parent exits (and may terminate docker containers); disable it.
	cmd.Env = append(os.Environ(), "WERF_ENABLE_PROCESS_EXTERMINATOR=0")

	if err := cmd.Start(); err != nil {
		return err
	}

	return cmd.Process.Release()
}

// marker records the last background plugin-update check so the throttle survives
// across invocations. It tracks only the timestamp - the work itself (installing
// newer versions) lives on disk.
type marker struct {
	CheckedAt time.Time `json:"checked_at"`
}

func (m marker) isStale() bool {
	return time.Since(m.CheckedAt) > pluginUpdateCheckTTL
}

// markerPath returns the per-user marker path <UserCacheDir>/deckhouse-cli/<file>.
// The user cache dir keeps it writable regardless of where d8 is installed (the
// deploy dir is usually root-owned).
func markerPath() (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("locate user cache dir: %w", err)
	}

	return filepath.Join(dir, "deckhouse-cli", markerFile), nil
}

func loadMarker(path string) (marker, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return marker{}, err
	}

	var m marker
	if err := json.Unmarshal(data, &m); err != nil {
		return marker{}, fmt.Errorf("parse %s: %w", filepath.Base(path), err)
	}

	return m, nil
}

// saveMarker writes the marker atomically (temp file + rename in the same dir), so
// a concurrent reader or a racing background writer never observes a torn file.
func saveMarker(path string, m marker) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create marker dir: %w", err)
	}

	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal marker: %w", err)
	}

	tmp, err := os.CreateTemp(dir, markerFile+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp marker: %w", err)
	}

	defer func() { _ = os.Remove(tmp.Name()) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()

		return fmt.Errorf("write temp marker: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp marker: %w", err)
	}

	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replace marker: %w", err)
	}

	return nil
}
