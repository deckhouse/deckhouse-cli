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

package selfupdate

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Masterminds/semver/v3"
)

// EnvDisableUpdateNotify turns off the d8 self-update notice and its background
// refresh. It is an independent per-mechanism switch: it has no effect on the
// plugin auto-update (which has its own D8_DISABLE_PLUGIN_AUTO_UPDATE).
const EnvDisableUpdateNotify = "D8_DISABLE_UPDATE_NOTIFY"

// NotifyIfUpdateAvailable writes a one-line notice to w when the cached latest
// stable version is newer than current. Best-effort and silent on any problem
// (missing/empty cache, non-semver current, disabled via env), so it never
// affects the command the user actually ran.
func NotifyIfUpdateAvailable(w io.Writer, current string) {
	if os.Getenv(EnvDisableUpdateNotify) != "" {
		return
	}

	path, err := cachePath()
	if err != nil {
		return
	}

	cache, err := loadCache(path)
	if err != nil || cache.LatestVersion == "" {
		return
	}

	if !isNewer(current, cache.LatestVersion) {
		return
	}

	fmt.Fprintf(w, "\nA newer deckhouse-cli is available: %s (current %s). Run 'd8 cli update' to upgrade.\n",
		cache.LatestVersion, current)
}

// isNewer reports whether latest is a stable semver strictly greater than current.
// A non-semver current (a dev build) returns false to avoid nagging developers.
func isNewer(current, latest string) bool {
	latestVersion, err := semver.NewVersion(latest)
	if err != nil {
		return false
	}

	currentVersion, err := semver.NewVersion(current)
	if err != nil {
		return false
	}

	return currentVersion.LessThan(latestVersion)
}

// RefreshDue reports whether the notice cache is stale enough to refresh (and the
// notice is not disabled). Cheap: one small file read, no cluster access - so it is
// safe to call from the root hook after every command.
func RefreshDue() bool {
	if os.Getenv(EnvDisableUpdateNotify) != "" {
		return false
	}

	path, err := cachePath()
	if err != nil {
		return false
	}

	// A missing cache (never checked) is due.
	cache, err := loadCache(path)

	return err != nil || cache.isStale()
}

// MarkChecked stamps the cache checked_at now, preserving latest_version, so a
// refresh that then fails to reach the cluster still backs off for the full TTL
// instead of retrying on the next command.
func MarkChecked() {
	path, err := cachePath()
	if err != nil {
		return
	}

	cache, _ := loadCache(path)
	cache.CheckedAt = time.Now()
	_ = saveCache(path, cache)
}

// RefreshCache performs a live version check and writes the cache. checked_at is
// recorded and the last known latest version is preserved on a transient failure.
// The caller owns the timeout.
func RefreshCache(ctx context.Context, updater *Updater, current string) error {
	path, err := cachePath()
	if err != nil {
		return err
	}

	cache, _ := loadCache(path)
	cache.CheckedAt = time.Now()

	if latest, _, err := updater.LatestVersion(ctx, current); err == nil {
		cache.LatestVersion = latest
	}

	return saveCache(path, cache)
}
