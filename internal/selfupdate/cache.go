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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// updateCheckTTL is how long a cached check is considered fresh, bounding how
// often the synchronous notice refresh contacts the proxy. It is deliberately
// long: the refresh runs in the foreground, so it must be rare.
const updateCheckTTL = 24 * time.Hour

const updateCheckFile = "update-check.json"

// updateCheckCache records the last availability check so the notice can be shown
// from disk without contacting the cluster on every invocation.
type updateCheckCache struct {
	CheckedAt     time.Time `json:"checked_at"`
	LatestVersion string    `json:"latest_version"`
}

func (c updateCheckCache) isStale() bool {
	return time.Since(c.CheckedAt) > updateCheckTTL
}

// cachePath returns the per-user cache path <UserCacheDir>/deckhouse-cli/<file>.
// The user cache dir keeps it writable regardless of where d8 is installed.
func cachePath() (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("locate user cache dir: %w", err)
	}

	return filepath.Join(dir, "deckhouse-cli", updateCheckFile), nil
}

func loadCache(path string) (updateCheckCache, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return updateCheckCache{}, err
	}

	var cache updateCheckCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return updateCheckCache{}, fmt.Errorf("parse %s: %w", filepath.Base(path), err)
	}

	return cache, nil
}

// saveCache writes the cache atomically (temp file + rename in the same dir), so a
// concurrent reader never observes a torn file. Missing parent dirs are created.
func saveCache(path string, cache updateCheckCache) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}

	data, err := json.Marshal(cache)
	if err != nil {
		return fmt.Errorf("marshal cache: %w", err)
	}

	tmp, err := os.CreateTemp(dir, updateCheckFile+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp cache: %w", err)
	}

	defer func() { _ = os.Remove(tmp.Name()) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()

		return fmt.Errorf("write temp cache: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp cache: %w", err)
	}

	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replace cache: %w", err)
	}

	return nil
}
