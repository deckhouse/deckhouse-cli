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
	"path"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// RegistryPathRow is one component's location in the registry, for the summary
// layout section.
type RegistryPathRow struct {
	// Label is the component name, e.g. "Platform", "Modules".
	Label string
	// Path is the full registry path where the component lives.
	Path string
	// NonDefault marks a path the user moved off the standard layout. Only the
	// modules path is configurable today (via --modules-path-suffix), so only it
	// can be non-default; the model stays general for future configurable paths.
	NonDefault bool
	// DefaultPath is the standard path for this component. Filled only when
	// NonDefault, so the summary can show "default: <path>".
	DefaultPath string
}

// RegistryLayout is where each mirrored component resolves in the registry, for
// display in the pull/push summary. Root is the edition root on pull and the
// target repo on push.
type RegistryLayout struct {
	Root        string
	Rows        []RegistryPathRow
	HasOverride bool
}

// BuildRegistryLayout maps every mirrored component to its registry path.
//
// root is the edition root (pull) or the target repo (push). modulesPathSuffix
// is the raw --modules-path-suffix value; it is normalized the same way as the
// real push/pull path so the summary matches where images actually go. Only the
// modules path can differ from the standard layout, so only it is flagged
// NonDefault.
//
// installerPath is the full registry path of the installer images, which live
// outside the edition root; pass "" to omit the Installer row (push does not
// create an installer repo).
func BuildRegistryLayout(root, modulesPathSuffix, installerPath string) RegistryLayout {
	root = strings.TrimSuffix(root, "/")

	modulesPath := registryservice.NormalizeModulesPath(modulesPathSuffix)
	modulesNonDefault := modulesPath != internal.ModulesSegment

	rows := []RegistryPathRow{
		{Label: "Platform", Path: root},
		{
			Label:       "Modules",
			Path:        path.Join(root, modulesPath),
			NonDefault:  modulesNonDefault,
			DefaultPath: path.Join(root, internal.ModulesSegment),
		},
		{Label: "Security", Path: path.Join(root, internal.SecuritySegment)},
		{Label: "Packages", Path: path.Join(root, internal.PackagesSegment)},
	}

	if installerPath != "" {
		rows = append(rows, RegistryPathRow{Label: "Installer", Path: installerPath})
	}

	return RegistryLayout{
		Root:        root,
		Rows:        rows,
		HasOverride: modulesNonDefault,
	}
}
