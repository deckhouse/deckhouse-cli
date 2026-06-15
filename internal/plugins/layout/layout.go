/*
Copyright 2025 Flant JSC

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

// Package layout centralizes the on-disk filesystem layout used by the
// d8 plugins subsystem: directory names, suffixes, and helpers that build
// concrete paths from the install root.
//
// The install root is the on-disk directory under which the subsystem keeps
// everything it owns - installed plugin binaries (<root>/plugins/...) and
// cached contracts (<root>/cache/contracts/...). It is supplied by the
// caller (typically the plugins.Manager directory, sourced from --plugins-dir, with
// default /opt/deckhouse/lib/deckhouse-cli, or ~/.deckhouse-cli as fallback).
//
// Callers should use the builder functions (PluginDir, BinaryPath, ...) for
// full paths; the directory-name segments are package-private.
package layout

import (
	"fmt"
	"os"
	"path"
	"strconv"
)

const (
	pluginsDirName   = "plugins"
	cacheDirName     = "cache"
	contractsDirName = "contracts"
	currentLinkName  = "current"
	lockFileSuffix   = ".lock"
	ContractFileExt  = ".json"
	homeFallbackDir  = ".deckhouse-cli"
	VersionDirPrefix = "v"
)

// PluginsRoot returns <installRoot>/plugins.
func PluginsRoot(installRoot string) string {
	return path.Join(installRoot, pluginsDirName)
}

// PluginDir returns <installRoot>/plugins/<pluginName>.
func PluginDir(installRoot, pluginName string) string {
	return path.Join(installRoot, pluginsDirName, pluginName)
}

// VersionDir returns <installRoot>/plugins/<pluginName>/v<majorVersion>.
func VersionDir(installRoot, pluginName string, majorVersion int) string {
	return path.Join(installRoot, pluginsDirName, pluginName, VersionDirPrefix+strconv.Itoa(majorVersion))
}

// BinaryPath returns <installRoot>/plugins/<pluginName>/v<majorVersion>/<pluginName>.
func BinaryPath(installRoot, pluginName string, majorVersion int) string {
	return path.Join(installRoot, pluginsDirName, pluginName, VersionDirPrefix+strconv.Itoa(majorVersion), pluginName)
}

// CurrentLinkPath returns <installRoot>/plugins/<pluginName>/current - the symlink to the
// currently active binary version.
func CurrentLinkPath(installRoot, pluginName string) string {
	return path.Join(installRoot, pluginsDirName, pluginName, currentLinkName)
}

// ContractsDir returns <installRoot>/cache/contracts.
func ContractsDir(installRoot string) string {
	return path.Join(installRoot, cacheDirName, contractsDirName)
}

// ContractFile returns <installRoot>/cache/contracts/<pluginName>.json.
func ContractFile(installRoot, pluginName string) string {
	return path.Join(installRoot, cacheDirName, contractsDirName, pluginName+ContractFileExt)
}

// HomeFallbackPath returns ~/.deckhouse-cli - the fallback install root used
// when the default /opt path is not writable.
func HomeFallbackPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to determine user home directory: %w", err)
	}

	return path.Join(home, homeFallbackDir), nil
}

// InstallLockPath returns <installRoot>/plugins/<pluginName>/install.lock - one
// lock per plugin (not per major): installs of different majors still contend
// on the shared `current` symlink and contract cache, so they must serialize.
func InstallLockPath(installRoot, pluginName string) string {
	return path.Join(installRoot, pluginsDirName, pluginName, "install"+lockFileSuffix)
}

// RootHasInstall reports whether <root>/plugins holds at least one installed
// plugin - a directory with a `current` symlink. Requiring the symlink (not just
// any subdir) means a leftover empty v<major> dir from a failed install does not
// count as an install. The cache dir is a sibling, never miscounted.
func RootHasInstall(root string) bool {
	entries, err := os.ReadDir(PluginsRoot(root))
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		if _, err := os.Lstat(CurrentLinkPath(root, entry.Name())); err == nil {
			return true
		}
	}

	return false
}
