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

package plugins

import (
	"context"

	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
)

// ModuleInBundle is one mirrored module with the exact versions selected into
// the bundle. The pull orchestrator builds this list from the modules phase
// stats and hands it to the plugins phase.
type ModuleInBundle struct {
	Name     string
	Versions []*semver.Version
}

// ResolveInput is everything the resolver knows about the bundle being built.
type ResolveInput struct {
	// Modules are the modules mirrored into this bundle with their versions.
	// Empty when modules were not mirrored: nothing is auto-selected then and
	// only explicit --include-plugin entries are resolved.
	Modules []ModuleInBundle
	// PlatformVersions are the mirrored Deckhouse platform versions. Empty
	// when the platform phase was skipped; the contract's deckhouse
	// constraint is then not checked.
	PlatformVersions []*semver.Version
	// Filter carries --include-plugin expressions. Explicit picks are
	// additive: they are pulled on top of the module-driven selection.
	Filter *modules.Filter
	// Builtins are d8 built-in command names (e.g. delivery-kit, package)
	// that satisfy a same-named plugin dependency by presence. They are
	// never pulled.
	Builtins map[string]struct{}
}

// ReasonKind classifies why a plugin version is in the bundle.
type ReasonKind int

const (
	// ReasonModule marks a plugin required by a mirrored module.
	// Reason.Subject is the module name.
	ReasonModule ReasonKind = iota
	// ReasonDependency marks a mandatory dependency of another selected
	// plugin. Reason.Subject is "<dependent>@<version>".
	ReasonDependency
	// ReasonExplicit marks a plugin named by --include-plugin.
	// Reason.Subject is the flag expression.
	ReasonExplicit
)

// Reason is one provenance edge of a selected plugin version: who needed it
// and under which constraint. The summary renders these edges as the
// per-module plugin tree.
type Reason struct {
	Kind    ReasonKind
	Subject string
	// Constraint is the requirement constraint that created the edge,
	// empty when none was declared.
	Constraint string
}

// SelectedVersion is one plugin version to mirror, with its provenance.
type SelectedVersion struct {
	Version  *semver.Version
	Contract *internal.Plugin
	Reasons  []Reason
}

// PluginToMirror is the resolver's verdict for one plugin.
type PluginToMirror struct {
	Name string
	// Versions to pull, newest first. Several versions appear when different
	// bundled module versions need different plugin versions.
	Versions []SelectedVersion
}

// SkippedPlugin is a plugin the resolver considered and dropped, with the
// reason spelled out for the summary (e.g. `requires module "postgresql"
// >=2.0.0, bundle has v1.4.1`).
type SkippedPlugin struct {
	Name   string
	Reason string
}

// Resolution is the resolver's full output.
type Resolution struct {
	// Plugins to mirror, sorted by name for deterministic output.
	Plugins []PluginToMirror
	Skipped []SkippedPlugin
	// Warnings are advisories that do not block the pull (e.g. an explicitly
	// included plugin whose required module is not in the bundle).
	Warnings []string
}

// Resolver picks which plugin versions belong to the bundle.
type Resolver interface {
	Resolve(ctx context.Context, in ResolveInput) (*Resolution, error)
}
