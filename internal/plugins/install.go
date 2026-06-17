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

package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/fatih/color"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/lockfile"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

const (
	// pluginProbeTimeout bounds running a plugin binary with --version (the
	// post-install smoke test, the already-installed check, and reading the installed
	// version). A version probe is a sub-second call; this is only a hang guard.
	pluginProbeTimeout = 10 * time.Second

	// installLockStaleAfter is how old a lock file may get before it is treated as
	// orphaned (a prior install was hard-killed before its deferred release ran).
	// Installs take seconds, so an hour-old lock is certainly stale.
	installLockStaleAfter = 1 * time.Hour

	// stagedBinarySuffix marks the temp path a new binary is downloaded to before
	// it is verified and atomically swapped in place of the live one.
	stagedBinarySuffix = ".new"
)

// pluginNameLayout matches a valid plugin name: a single lowercase OCI path
// component. Anything else cannot name a published plugin and, used unvalidated,
// would build filesystem paths outside the plugins root ("..", "a/b") or alter
// registry routes.
var pluginNameLayout = regexp.MustCompile(`^[a-z0-9]+(?:[._-][a-z0-9]+)*$`)

// ValidatePluginName guards every user-supplied plugin name before it reaches
// MkdirAll / RemoveAll / registry paths.
func ValidatePluginName(name string) error {
	if !pluginNameLayout.MatchString(name) {
		return fmt.Errorf("invalid plugin name %q", name)
	}

	return nil
}

type installOptions struct {
	version                 string
	majorVersion            int
	resolvePluginsConflicts bool
	force                   bool
}

type InstallOption func(*installOptions)

func InstallWithForce() InstallOption {
	return func(opts *installOptions) {
		opts.force = true
	}
}

func InstallWithMajorVersion(majorVersion int) InstallOption {
	return func(opts *installOptions) {
		opts.majorVersion = majorVersion
	}
}

func InstallWithVersion(version string) InstallOption {
	return func(opts *installOptions) {
		opts.version = version
	}
}

func InstallWithResolvePluginsConflicts() InstallOption {
	return func(opts *installOptions) {
		opts.resolvePluginsConflicts = true
	}
}

// InstallPlugin installs or switches to a plugin version.
// Steps: select the version, validate requirements, lay out
// plugins/<name>/v<major>, swap the binary in, point `current` at it, cache
// the contract.
// Tune behaviour with the InstallWith* options (version, major, force,
// resolve-conflicts). With no options it installs the newest cluster-compatible
// version within the installed major.
func (m *Manager) InstallPlugin(ctx context.Context, pluginName string, opts ...InstallOption) error {
	if err := ValidatePluginName(pluginName); err != nil {
		return err
	}

	var installVersion *semver.Version

	options := &installOptions{
		majorVersion: -1,
	}

	for _, opt := range opts {
		opt(options)
	}

	// A major given explicitly via --use-major may move in any direction; only
	// the implicit selection below gets the downgrade guard.
	explicitMajor := options.majorVersion >= 0

	if options.version != "" {
		var err error

		installVersion, err = semver.NewVersion(options.version)
		if err != nil {
			return fmt.Errorf("failed to parse version: %w", err)
		}

		return m.installPlugin(ctx, pluginName, installVersion, options.resolvePluginsConflicts, options.force)
	}

	versions, err := m.service.ListPluginTags(ctx, pluginName)
	if err != nil {
		return fmt.Errorf("failed to list plugin tags: %w", err)
	}

	// Policy: an update stays within the installed major version unless the user
	// explicitly crosses majors with --use-major; a fresh install has no major to
	// pin and considers all majors.
	if options.majorVersion < 0 {
		options.majorVersion = m.inheritInstalledMajor(pluginName)
	}

	if options.majorVersion >= 0 {
		versions = m.filterMajorVersion(versions, options.majorVersion)
		if len(versions) == 0 {
			return fmt.Errorf("no versions found for major version: %d", options.majorVersion)
		}
	}

	// Requirements-aware selection: newest version compatible with this cluster.
	installVersion, err = m.selectLatestCompatible(ctx, pluginName, versions)
	if err != nil {
		if options.majorVersion >= 0 {
			return fmt.Errorf("%w (search was limited to major %d; pass --use-major to consider another major)",
				err, options.majorVersion)
		}

		return err
	}

	// Downgrade guard for the implicit path: when the newest tag's contract is
	// temporarily unreadable, selection falls back to an older version - that must
	// not silently downgrade a newer installed plugin. Explicit --version /
	// --use-major bypass this by design.
	if !explicitMajor && m.installedIsNewerThan(pluginName, installVersion) {
		fmt.Printf("Selected %s is older than the installed version; keeping the installed version "+
			"(use --version to downgrade explicitly).\n", installVersion.Original())

		return nil
	}

	return m.installPlugin(ctx, pluginName, installVersion, options.resolvePluginsConflicts, options.force)
}

// inheritInstalledMajor returns the major version to pin an implicit update to:
// the major of the installed plugin, or -1 (consider all majors) for a fresh
// install. The major is read from disk (the `current` symlink), not by running
// the binary, so a broken binary cannot drop the pin and let an implicit update
// cross a major.
func (m *Manager) inheritInstalledMajor(pluginName string) int {
	installed, _ := m.checkInstalled(pluginName)
	if !installed {
		return -1
	}

	major, ok := m.installedMajorFromDisk(pluginName)
	if !ok {
		m.logger.Warn("could not determine the installed major version; considering all majors (pass --use-major to constrain)",
			slog.String("plugin", pluginName))

		return -1
	}

	m.logger.Debug("staying within the installed major version (use --use-major to cross majors)",
		slog.String("plugin", pluginName), slog.Int("major", major))

	return major
}

// installedIsNewerThan reports whether the installed plugin is strictly newer
// than the selected version. An unreadable installed version (not installed,
// broken binary) reports false so install/repair proceeds.
func (m *Manager) installedIsNewerThan(pluginName string, selected *semver.Version) bool {
	current, err := m.getInstalledPluginVersion(pluginName)
	if err != nil {
		return false
	}

	return selected.LessThan(current)
}

// pluginPaths bundles the filesystem locations an install operates on.
// Created once by preparePluginDirs and threaded through the install pipeline.
type pluginPaths struct {
	pluginDir   string // <plugin-dir>/plugins/<name>
	versionDir  string // <plugin-dir>/plugins/<name>/v<major>
	binaryPath  string // <plugin-dir>/plugins/<name>/v<major>/<name>
	lockPath    string // <plugin-dir>/plugins/<name>/install.lock - one per plugin, not per major: installs of different majors still contend on `current` and the contract cache
	currentLink string // <plugin-dir>/plugins/<name>/current
}

// installPlugin orchestrates the install pipeline. Each step delegates to a
// focused helper below. The order matters:
//   - validate before switch
//   - stage and smoke-test before swapping the live binary
//   - cache the contract before linking
func (m *Manager) installPlugin(ctx context.Context, pluginName string, version *semver.Version, resolvePluginsConflicts, force bool) error {
	paths, err := m.preparePluginDirs(pluginName, version)
	if err != nil {
		return err
	}

	release, err := m.acquireInstallLock(paths.lockPath)
	if err != nil {
		return err
	}
	defer release()

	// One binary probe, reused by both checks below: the probe EXECUTES the
	// installed binary (--version), and the install lock guarantees the file
	// cannot change between the two uses.
	alreadyAtVersion := !force && m.pluginAlreadyAtVersion(ctx, paths.binaryPath, version)

	// Already current at the selected version: no switch happens, so nothing to do
	// (execution is still gated by the pre-run requirement check). --force re-pulls.
	if alreadyAtVersion && m.isCurrentBinary(paths) {
		fmt.Printf("Plugin '%s' is already at %s, nothing to do (use --force to reinstall).\n", pluginName, version.Original())

		return nil
	}

	// We are about to (re)point `current`. Validate requirements BEFORE the switch
	// (ADR: check requirements before switching) - this also covers the relink path
	// below, so a switch to an already-installed version is never made blindly.
	plugin, err := m.fetchAndDisplayContract(ctx, pluginName, version)
	if err != nil {
		return err
	}

	if err := m.validateAndResolveConflicts(ctx, plugin, resolvePluginsConflicts); err != nil {
		return err
	}

	// Selected version already on disk (a different major/version is current):
	// repoint the symlink, no re-download. Requirements were validated just above.
	// --force instead falls through to a full re-pull (corrupted on-disk binary,
	// republished tag).
	//
	// Cache the contract BEFORE flipping the link. A failure in between then
	// over-enforces the new contract on the old binary (fails closed), rather than
	// running the new binary gated by the old contract.
	if alreadyAtVersion {
		if err := m.cacheContract(pluginName, plugin); err != nil {
			return err
		}

		if err := m.linkCurrent(paths); err != nil {
			return err
		}

		fmt.Printf("Switched plugin '%s' to the already-installed %s.\n", pluginName, version.Original())

		return nil
	}

	// Download to a sibling staged path and verify it BEFORE touching the live
	// binary: the installed plugin keeps working (and `current` never dangles) for
	// the whole download, and a failed/partial download leaves no trace behind.
	stagedPath := paths.binaryPath + stagedBinarySuffix
	// Clean up the staged binary on any failure; a no-op once it is renamed into place.
	defer func() { _ = os.Remove(stagedPath) }()

	if err := m.downloadAndExtract(ctx, pluginName, version, stagedPath); err != nil {
		return err
	}

	// Reject a corrupt/incompatible binary before it replaces anything (ADR
	// safe-update: smoke-check --version before the switch).
	if err := m.smokeTestPlugin(ctx, pluginName, stagedPath); err != nil {
		return err
	}

	// Atomically replace the live binary (rename over it), so `current` never
	// points at a missing file. On failure the original is untouched.
	if err := os.Rename(stagedPath, paths.binaryPath); err != nil {
		return fmt.Errorf("install new binary: %w", err)
	}

	if err := m.cacheContract(pluginName, plugin); err != nil {
		return err
	}

	if err := m.linkCurrent(paths); err != nil {
		return err
	}

	fmt.Printf("✓ Plugin '%s' successfully installed!\n", pluginName)

	return nil
}

// preparePluginDirs creates plugins/<name>/v<major> on disk and returns the
// paths derived from <pluginName, version> used by the rest of the pipeline.
func (m *Manager) preparePluginDirs(pluginName string, version *semver.Version) (pluginPaths, error) {
	major := int(version.Major())
	paths := pluginPaths{
		pluginDir:   layout.PluginDir(m.pluginDirectory, pluginName),
		versionDir:  layout.VersionDir(m.pluginDirectory, pluginName, major),
		binaryPath:  layout.BinaryPath(m.pluginDirectory, pluginName, major),
		lockPath:    layout.InstallLockPath(m.pluginDirectory, pluginName),
		currentLink: layout.CurrentLinkPath(m.pluginDirectory, pluginName),
	}

	if err := os.MkdirAll(paths.pluginDir, 0755); err != nil {
		return pluginPaths{}, fmt.Errorf("failed to create plugin directory: %w", err)
	}

	if err := os.MkdirAll(paths.versionDir, 0755); err != nil {
		return pluginPaths{}, fmt.Errorf("failed to create plugin version directory: %w", err)
	}

	return paths, nil
}

// acquireInstallLock serializes installs of the plugin. A lock orphaned by a
// hard-killed install (older than installLockStaleAfter) is reclaimed: a Ctrl-C'd
// or crashed install leaves such orphans, so recover instead of failing forever.
// The caller must invoke the returned release func when finished (typically via
// defer).
func (m *Manager) acquireInstallLock(lockFilePath string) (func(), error) {
	release, err := lockfile.Acquire(lockFilePath, installLockStaleAfter, func(age time.Duration) {
		m.logger.Warn("reclaiming a stale plugin install lock",
			slog.String("lock", lockFilePath), slog.Duration("age", age))
	})

	if errors.Is(err, lockfile.ErrLocked) {
		return nil, fmt.Errorf("plugin is locked by: %s", lockFilePath)
	}

	if err != nil {
		return nil, err
	}

	return release, nil
}

// fetchAndDisplayContract pulls the contract for <pluginName, version> from
// the registry and prints the installing-plugin banner.
func (m *Manager) fetchAndDisplayContract(ctx context.Context, pluginName string, version *semver.Version) (*internal.Plugin, error) {
	tag := version.Original()

	// Cyan-bold key labels. fatih/color drops ANSI on a non-TTY and under NO_COLOR.
	key := color.New(color.FgCyan, color.Bold)

	fmt.Printf("%s %s\n", key.Sprint("Installing plugin:"), pluginName)
	fmt.Printf("%s %s\n", key.Sprint("Tag:"), tag)

	plugin, err := m.PluginContract(ctx, pluginName, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get plugin contract: %w", err)
	}

	fmt.Printf("%s %s %s\n", key.Sprint("Plugin:"), printable(plugin.Name), printable(plugin.Version))
	fmt.Printf("%s %s\n", key.Sprint("Description:"), printable(plugin.Description))

	return plugin, nil
}

// printable strips terminal control characters from untrusted contract text, so
// a malicious image cannot smuggle ANSI escapes into the user's terminal.
func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if (r < 32 && r != '\n' && r != '\t') || r == 127 {
			return -1
		}

		return r
	}, s)
}

// validateAndResolveConflicts runs validateRequirements; if requirements are
// not satisfied and resolvePluginsConflicts is true, attempts to fix them
// recursively; otherwise returns an error.
func (m *Manager) validateAndResolveConflicts(ctx context.Context, plugin *internal.Plugin, resolvePluginsConflicts bool) error {
	m.logger.Debug("validating requirements", slog.String("plugin", plugin.Name))

	failedConstraints, err := m.validateRequirements(ctx, plugin)
	if err != nil {
		return fmt.Errorf("failed to validate requirements: %w", err)
	}

	if len(failedConstraints) > 0 && !resolvePluginsConflicts {
		return failedConstraints.helpfulError(
			fmt.Sprintf("plugin %q has unsatisfied requirements", plugin.Name), true)
	}

	if len(failedConstraints) > 0 && resolvePluginsConflicts {
		if err := m.resolvePluginConflicts(ctx, failedConstraints); err != nil {
			return fmt.Errorf("failed to resolve conflicts: %w", err)
		}

		// Resolution installs the latest cluster-compatible dependency, which may
		// still fail the requiring plugin's constraint. Re-validate so a partial
		// resolution surfaces here, not as a blocked plugin at first run.
		remaining, err := m.validateRequirements(ctx, plugin)
		if err != nil {
			return fmt.Errorf("failed to validate requirements: %w", err)
		}

		if len(remaining) > 0 {
			return remaining.helpfulError(
				fmt.Sprintf("plugin %q still has unsatisfied requirements after --resolve-plugins-conflicts", plugin.Name), false)
		}
	}

	return nil
}

// pluginVersionProbe runs the binary at binaryPath with "--version" (falling back
// to "version") and returns its stdout. It succeeds if either invocation exits
// cleanly; the returned error on failure carries a tail of the binary's stderr so
// a smoke-test failure (missing shared lib, panic, wrong arch) is diagnosable.
func pluginVersionProbe(ctx context.Context, binaryPath string) ([]byte, error) {
	if output, err := exec.CommandContext(ctx, binaryPath, "--version").Output(); err == nil {
		return output, nil
	}

	output, err := exec.CommandContext(ctx, binaryPath, "version").Output()
	if err != nil {
		return nil, fmt.Errorf("run plugin binary: %w%s", err, stderrTail(err))
	}

	return output, nil
}

// stderrTail returns a short parenthesized tail of an *exec.ExitError's captured
// stderr (populated by Cmd.Output), or "" - turning "exit status 1" into something
// actionable.
func stderrTail(err error) string {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || len(exitErr.Stderr) == 0 {
		return ""
	}

	const maxLen = 200

	msg := strings.TrimSpace(string(exitErr.Stderr))
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "..."
	}

	return fmt.Sprintf(" (stderr: %s)", msg)
}

// pluginBinaryVersion probes the binary and parses its reported version as semver.
func pluginBinaryVersion(ctx context.Context, binaryPath string) (*semver.Version, error) {
	output, err := pluginVersionProbe(ctx, binaryPath)
	if err != nil {
		return nil, err
	}

	version, err := semver.NewVersion(strings.TrimSpace(string(output)))
	if err != nil {
		return nil, fmt.Errorf("parse plugin version %q: %w", strings.TrimSpace(string(output)), err)
	}

	return version, nil
}

// installedMajorFromDisk returns the major the plugin's `current` symlink points
// at, read from the on-disk v<major> directory WITHOUT running the binary - so a
// broken/hung binary cannot lose the major pin and let an implicit update
// silently cross a major.
func (m *Manager) installedMajorFromDisk(pluginName string) (int, bool) {
	target, err := os.Readlink(layout.CurrentLinkPath(m.pluginDirectory, pluginName))
	if err != nil {
		return 0, false
	}

	// target: <root>/plugins/<name>/v<major>/<name> - the parent dir is v<major>.
	major, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(filepath.Dir(target)), layout.VersionDirPrefix))
	if err != nil {
		return 0, false
	}

	return major, true
}

// pluginAlreadyAtVersion reports whether the major's binary is already the selected
// version, so an install/update is a no-op (idempotency; see ADR safe update
// "skip if already installed").
func (m *Manager) pluginAlreadyAtVersion(ctx context.Context, binaryPath string, version *semver.Version) bool {
	if _, err := os.Stat(binaryPath); err != nil {
		return false
	}

	ctx, cancel := context.WithTimeout(ctx, pluginProbeTimeout)
	defer cancel()

	installed, err := pluginBinaryVersion(ctx, binaryPath)
	if err != nil {
		return false
	}

	return installed.Equal(version)
}

// isCurrentBinary reports whether the `current` symlink already points at this
// major's binary, so an already-installed selected version can be distinguished
// between "nothing to do" and "needs a symlink switch".
func (m *Manager) isCurrentBinary(paths pluginPaths) bool {
	target, err := os.Readlink(paths.currentLink)
	if err != nil {
		return false
	}

	abs, err := filepath.Abs(paths.binaryPath)
	if err != nil {
		return false
	}

	return target == abs
}

// smokeTestPlugin verifies the freshly extracted binary runs cleanly when asked
// for its version. A corrupt or incompatible artifact (wrong arch, missing libs,
// crash) is rejected before it is linked as current.
// It requires only a clean exit, not parseable version output: a plugin that
// prints a human-readable banner is not rejected. Version parsing is reserved
// for the version-comparison paths.
func (m *Manager) smokeTestPlugin(ctx context.Context, pluginName, binaryPath string) error {
	ctx, cancel := context.WithTimeout(ctx, pluginProbeTimeout)
	defer cancel()

	if _, err := pluginVersionProbe(ctx, binaryPath); err != nil {
		return fmt.Errorf("installed %q binary failed its smoke test: %w", pluginName, err)
	}

	return nil
}

// downloadAndExtract pulls the plugin image tag and writes the embedded
// binary to <binaryPath> (a staged sibling of the final install path).
func (m *Manager) downloadAndExtract(ctx context.Context, pluginName string, version *semver.Version, binaryPath string) error {
	tag := version.Original()

	fmt.Printf("Installing to: %s\n", strings.TrimSuffix(binaryPath, stagedBinarySuffix))
	fmt.Println("Downloading and extracting plugin...")

	if err := m.service.ExtractPlugin(ctx, pluginName, tag, binaryPath); err != nil {
		return fmt.Errorf("failed to extract plugin: %w", err)
	}

	return nil
}

// linkCurrent (re)points <pluginDir>/current to the freshly installed binary
// using an absolute target path. The new link is built aside and renamed over
// `current`: rename is atomic, so a concurrent plugin exec always sees either
// the old or the new target - never a missing link (a plain remove+symlink has
// exactly that window).
func (m *Manager) linkCurrent(paths pluginPaths) error {
	absPath, err := filepath.Abs(paths.binaryPath)
	if err != nil {
		return fmt.Errorf("failed to compute absolute path: %w", err)
	}

	staged := paths.currentLink + stagedBinarySuffix
	_ = os.Remove(staged)

	if err := os.Symlink(absPath, staged); err != nil {
		return fmt.Errorf("failed to create symlink: %w", err)
	}

	if err := os.Rename(staged, paths.currentLink); err != nil {
		_ = os.Remove(staged)

		return fmt.Errorf("failed to create symlink: %w", err)
	}

	return nil
}

// cacheContract writes the plugin contract JSON to
// <plugin-dir>/cache/contracts/<name>.json for later lookups by
// validatePluginConflicts and `d8 plugins list`. The write is atomic (temp +
// rename): the runtime gate reads this file lock-free before every plugin run,
// and a torn contract would hard-block the plugin until a --force reinstall.
func (m *Manager) cacheContract(pluginName string, plugin *internal.Plugin) error {
	dir := layout.ContractsDir(m.pluginDirectory)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create contract directory: %w", err)
	}

	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)

	if err := enc.Encode(service.DomainToContract(plugin)); err != nil {
		return fmt.Errorf("failed to cache contract: %w", err)
	}

	tmp, err := os.CreateTemp(dir, pluginName+layout.ContractFileExt+".tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create temp contract file: %w", err)
	}

	// Removed if the rename below does not consume it (errors / partial write).
	defer func() { _ = os.Remove(tmp.Name()) }()

	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()

		return fmt.Errorf("failed to write contract file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close contract file: %w", err)
	}

	// CreateTemp makes the file 0600; the cached contract is world-readable data.
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return fmt.Errorf("failed to chmod contract file: %w", err)
	}

	if err := os.Rename(tmp.Name(), layout.ContractFile(m.pluginDirectory, pluginName)); err != nil {
		return fmt.Errorf("failed to cache contract: %w", err)
	}

	return nil
}

func (m *Manager) filterMajorVersion(versions []string, majorVersion int) []string {
	res := make([]string, 0, 1)

	for _, ver := range versions {
		version, err := semver.NewVersion(ver)
		if err != nil {
			continue
		}

		if version.Major() == uint64(majorVersion) {
			res = append(res, ver)
		}
	}

	return res
}

func (m *Manager) resolvePluginConflicts(ctx context.Context, failedConstraints failedConstraints) error {
	for pluginName := range failedConstraints {
		m.logger.Debug("resolving plugin conflict", slog.String("plugin", pluginName))

		err := m.InstallPlugin(ctx, pluginName, InstallWithResolvePluginsConflicts())
		if err != nil {
			return fmt.Errorf("failed to install plugin: %w", err)
		}
	}

	return nil
}
