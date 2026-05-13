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
// caller (typically pc.pluginDirectory, sourced from --plugins-dir, with
// default /opt/deckhouse/lib/deckhouse-cli, or ~/.deckhouse-cli as fallback).
//
// Callers should use the builder functions (PluginDir, BinaryPath, ...)
// for full paths. The raw segment constants are exposed for the rare cases
// where only a single directory or extension name is needed.
package layout

import (
	"fmt"
	"os"
	"path"
	"strconv"
)

const (
	PluginsDirName   = "plugins"
	CacheDirName     = "cache"
	ContractsDirName = "contracts"
	CurrentLinkName  = "current"
	LockFileSuffix   = ".lock"
	ContractFileExt  = ".json"
	HomeFallbackDir  = ".deckhouse-cli"
	VersionDirPrefix = "v"
)

// PluginsRoot returns <installRoot>/plugins.
func PluginsRoot(installRoot string) string {
	return path.Join(installRoot, PluginsDirName)
}

// PluginDir returns <installRoot>/plugins/<pluginName>.
func PluginDir(installRoot, pluginName string) string {
	return path.Join(installRoot, PluginsDirName, pluginName)
}

// VersionDir returns <installRoot>/plugins/<pluginName>/v<majorVersion>.
func VersionDir(installRoot, pluginName string, majorVersion int) string {
	return path.Join(installRoot, PluginsDirName, pluginName, VersionDirPrefix+strconv.Itoa(majorVersion))
}

// BinaryPath returns <installRoot>/plugins/<pluginName>/v<majorVersion>/<pluginName>.
func BinaryPath(installRoot, pluginName string, majorVersion int) string {
	return path.Join(installRoot, PluginsDirName, pluginName, VersionDirPrefix+strconv.Itoa(majorVersion), pluginName)
}

// LockPath returns <installRoot>/plugins/<pluginName>/v<majorVersion>/<pluginName>.lock.
func LockPath(installRoot, pluginName string, majorVersion int) string {
	return path.Join(installRoot, PluginsDirName, pluginName, VersionDirPrefix+strconv.Itoa(majorVersion), pluginName+LockFileSuffix)
}

// CurrentLinkPath returns <installRoot>/plugins/<pluginName>/current - the symlink to the
// currently active binary version.
func CurrentLinkPath(installRoot, pluginName string) string {
	return path.Join(installRoot, PluginsDirName, pluginName, CurrentLinkName)
}

// ContractsDir returns <installRoot>/cache/contracts.
func ContractsDir(installRoot string) string {
	return path.Join(installRoot, CacheDirName, ContractsDirName)
}

// ContractFile returns <installRoot>/cache/contracts/<pluginName>.json.
func ContractFile(installRoot, pluginName string) string {
	return path.Join(installRoot, CacheDirName, ContractsDirName, pluginName+ContractFileExt)
}

// HomeFallbackPath returns ~/.deckhouse-cli - the fallback install root used
// when the default /opt path is not writable.
func HomeFallbackPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to determine user home directory: %w", err)
	}
	return path.Join(home, HomeFallbackDir), nil
}
