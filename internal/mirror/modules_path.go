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
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// ModulesPathReport describes where modules were mirrored, so the pull/push
// summary can warn when the path was moved off the standard layout via
// --modules-path-suffix. Only the modules path is configurable today, so it is
// the only component the summary tracks.
type ModulesPathReport struct {
	// Moved is true when the modules path differs from the standard "modules"
	// segment (i.e. --modules-path-suffix took effect).
	Moved bool
	// Root is the registry base repo the modules path is rooted at: the edition
	// root (pull) or the target repo (push), without the modules segment.
	Root string
	// Path is the full registry path modules were read from (pull) or written to
	// (push).
	Path string
	// DefaultPath is the standard modules path, shown as a "default: <path>" hint
	// next to a moved path.
	DefaultPath string
}

// BuildModulesPathReport resolves the modules registry path for the summary.
//
// root is the edition root (pull) or the target repo (push). modulesPathSuffix
// is the raw --modules-path-suffix value; it is normalized the same way as the
// real push/pull path so the summary matches where images actually go.
func BuildModulesPathReport(root, modulesPathSuffix string) ModulesPathReport {
	root = strings.TrimSuffix(root, "/")

	modulesPath := registryservice.NormalizeModulesPath(modulesPathSuffix)

	return ModulesPathReport{
		Moved:       modulesPath != internal.ModulesSegment,
		Root:        root,
		Path:        path.Join(root, modulesPath),
		DefaultPath: path.Join(root, internal.ModulesSegment),
	}
}

// Warning is the single-line form of the moved-modules-path warning, for the
// start of the pull/push log: it names the path modules go through and the
// standard path they would go through by default. Empty for a standard path.
//
// The same warning closes the run as a multi-line block in the summary (see
// summaryui.WriteModulesPathWarning), where the module count is known.
func (m ModulesPathReport) Warning() string {
	if !m.Moved {
		return ""
	}

	return fmt.Sprintf(
		"Modules use a non-default path (--modules-path-suffix): %s (default: %s)",
		m.Path, m.DefaultPath,
	)
}
