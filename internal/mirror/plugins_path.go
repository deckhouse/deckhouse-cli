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

package mirror

import (
	"fmt"
	"path"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// PluginsPathReport describes where d8 CLI plugins are written by push.
//
// CLI artifacts live at the registry root above the edition segment:
// registry.example.com/deckhouse/deckhouse-cli/plugins, never
// registry.example.com/deckhouse/ee/deckhouse-cli/plugins. Pull reads them
// from there, and the in-cluster registry-packages-proxy looks them up there
// by cutting the edition off the cluster's imagesRepo. Push applies the same
// rule to its target, so the other components (platform, modules, packages,
// installer, security) stay under the target while plugins go one level up.
type PluginsPathReport struct {
	// Edition is the edition segment cut from the end of the push target,
	// e.g. "ee" for a target ".../deckhouse/ee". Empty when the target has
	// no edition segment, in which case Root equals Target.
	Edition string
	// Target is the push target as given by the user, e.g.
	// "registry.example.com/deckhouse/ee".
	Target string
	// Root is the registry root plugins are written under: Target without
	// Edition, e.g. "registry.example.com/deckhouse".
	Root string
	// Path is the plugins catalog, "<Root>/deckhouse-cli/plugins". Plugin
	// images go to "<Path>/<name>", discovery tags to Path itself.
	Path string
}

// pluginsRootPath returns the target path CLI artifacts are written under:
// targetPath without its trailing edition segment, plus that segment.
//
// The root always keeps at least one path segment. A target like
// "registry.example.com/ee" names a project that happens to be called "ee",
// not an edition of a Deckhouse repository, so it is left alone. The
// registry-packages-proxy applies the same rule when it looks the artifacts
// up, and both sides must agree on the result.
func pluginsRootPath(targetPath string) (string, string) {
	root, edition := registryservice.GetEditionFromRegistryPath(targetPath)
	if edition == pkg.NoEdition {
		return targetPath, ""
	}

	if len(pkgclient.PathToSegments(root)) == 0 {
		return targetPath, ""
	}

	return root, edition.String()
}

func newPluginsPathReport(target, root, edition string) PluginsPathReport {
	return PluginsPathReport{
		Edition: edition,
		Target:  target,
		Root:    root,
		Path:    path.Join(root, internal.D8CLISegment, internal.D8PluginsSegment),
	}
}

// Moved reports whether plugins go above the push target, i.e. an edition
// segment was cut off it.
func (p PluginsPathReport) Moved() bool {
	return p.Edition != ""
}

// Notice is the single-line form for the push log, printed before the first
// plugin write so the user sees where plugins go and why it is not the target.
func (p PluginsPathReport) Notice() string {
	if !p.Moved() {
		return fmt.Sprintf("CLI plugins go to %s", p.Path)
	}

	return fmt.Sprintf("CLI plugins go to %s (registry root above the %q edition of %s; registry-packages-proxy looks for them there)",
		p.Path, p.Edition, p.Target)
}

// PluginsRootError is a failed write to the CLI plugins root when that root
// sits above the push target (see PluginsPathReport). It carries the paths so
// the diagnostic can say where the write went and why, instead of leaving the
// user with a bare registry error for a path they never typed.
type PluginsRootError struct {
	// Repo is the repository the write went to, e.g.
	// "registry.example.com/deckhouse/deckhouse-cli/plugins/postgresql-mgr".
	Repo string
	// Report describes the plugins root relative to the push target.
	Report PluginsPathReport
	// Err is the underlying push error.
	Err error
}

func (e *PluginsRootError) Error() string {
	return e.Err.Error()
}

func (e *PluginsRootError) Unwrap() error {
	return e.Err
}
