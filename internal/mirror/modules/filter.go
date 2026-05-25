/*
Copyright 2024 Flant JSC

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

package modules

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

type FilterType int

const (
	FilterTypeWhitelist FilterType = iota
	FilterTypeBlacklist
)

// Filter for modules by black and whitelists. Maps module names to minimal versions of these modules to be pulled.
// By default, this is a whitelist filter, but that can be changed via SetType.
type Filter struct {
	_type   FilterType
	modules map[string]VersionConstraint
	logger  params.Logger
}

func NewFilter(filterExpressions []string, filterType FilterType) (*Filter, error) {
	if filterType != FilterTypeWhitelist && filterType != FilterTypeBlacklist {
		return nil, fmt.Errorf("unknown filter type: %v", filterType)
	}

	filter := &Filter{
		_type:   filterType,
		modules: make(map[string]VersionConstraint),
	}
	if len(filterExpressions) == 0 {
		// Empty filter matches any module
		filter._type = FilterTypeBlacklist
		return filter, nil
	}

	for _, filterExpr := range filterExpressions {
		moduleName, versionStr, hasVersion := strings.Cut(strings.TrimSpace(filterExpr), "@")

		moduleName = strings.TrimSpace(moduleName)
		if moduleName == "" {
			return nil, fmt.Errorf("Malformed filter expression %q: empty module name", filterExpr)
		}

		if _, moduleRedeclared := filter.modules[moduleName]; moduleRedeclared {
			return nil, fmt.Errorf("Malformed filter expression: module %s is declared multiple times", moduleName)
		}

		if !hasVersion {
			constraint, _ := NewSemanticVersionConstraint(">=0.0.0")
			filter.modules[moduleName] = constraint

			continue
		}

		constraint, err := parseVersionConstraint(versionStr)
		if err != nil {
			return nil, err
		}

		filter.modules[moduleName] = constraint
	}

	return filter, nil
}

func (f *Filter) UseLogger(logger params.Logger) *Filter {
	f.logger = logger
	return f
}

func (f *Filter) Match(mod *Module) bool {
	_, moduleInList := f.modules[mod.Name]
	if f._type == FilterTypeWhitelist {
		return moduleInList
	}

	return !moduleInList
}

func (f *Filter) Len() int { return len(f.modules) }

func (f *Filter) GetConstraint(moduleName string) (VersionConstraint, bool) {
	constraint, found := f.modules[moduleName]
	return constraint, found
}

// IsWhitelist reports whether the filter is operating in whitelist mode.
// It exists so the modules service can take alternate code paths that
// only make sense when a finite, user-supplied module list is available
// (e.g. proxy-registry probing, which has no module catalog to enumerate).
func (f *Filter) IsWhitelist() bool {
	return f._type == FilterTypeWhitelist
}

// ModuleNames returns the names registered with the filter in
// deterministic insertion-agnostic order (sorted). For a whitelist
// filter this is exactly the set of modules the user named with
// --include-module; for a blacklist filter it is the set the user
// asked to exclude.
//
// The slice is freshly allocated so callers may mutate it freely.
func (f *Filter) ModuleNames() []string {
	names := make([]string, 0, len(f.modules))
	for name := range f.modules {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// ParseVersionConstraint turns a user-supplied constraint string into a
// VersionConstraint. The syntax mirrors the `module-name@<constraint>` body
// accepted by --include-module so any consumer (modules filter, platform
// --include-platform, future call sites) speaks the same dialect:
//
//   - "=v1.2.3"           → exact tag (no channel propagation)
//   - "=v1.2.3+stable"    → exact tag pinned to the named release channel
//   - ">=1.2.0 <=1.3.0"   → semver range with inclusive anchors
//   - "^1.2.0", "~1.2.0"  → semver shorthand
//   - "1.2.0"             → implicit caret (^1.2.0), kept for backward compat
//
// An empty or whitespace-only input is rejected so callers see a clear error
// instead of silently producing a no-op constraint.
func ParseVersionConstraint(v string) (VersionConstraint, error) {
	return parseVersionConstraint(v)
}

func parseVersionConstraint(v string) (VersionConstraint, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, fmt.Errorf("empty constraint")
	}

	switch v[0] {
	// has user defined constraint (nothing to do)
	case '=', '>', '<', '~', '^':
	default:
		// version without contraint (add ^ for backward compatibility)
		v = "^" + v
	}

	// exact-match: "=1.2.3" or "=1.2.3+stable"
	if v[0] == '=' {
		return parseExact(v[1:])
	}
	// semver constraint
	return parseSemver(v)
}

func parseExact(body string) (VersionConstraint, error) {
	// exact match, console@=v1.38.1 -> registry.deckhouse.io/deckhouse/ce/modules/console:v1.38.1
	tag, ch, _ := strings.Cut(body, "+")
	if tag == "" {
		return nil, fmt.Errorf("empty tag in %q", body)
	}

	if ch != "" {
		if internal.ChannelIsValid(ch) {
			return NewExactTagConstraintWithChannel(tag, ch), nil
		}
	}

	return NewExactTagConstraint(tag), nil
}

func parseSemver(v string) (VersionConstraint, error) {
	// semver match, console@~1.38.1 = registry.deckhouse.io/deckhouse/ce/modules/console:v1.38.x
	c, err := NewSemanticVersionConstraint(v)
	if err != nil {
		return nil, fmt.Errorf("invalid semver %q: %w", v, err)
	}

	return c, nil
}

func (f *Filter) ShouldMirrorReleaseChannels(moduleName string) bool {
	constraint, hasConstraint := f.modules[moduleName]
	if hasConstraint && constraint.IsExact() {
		return false
	}

	return true
}

// VersionsToMirror resolves module constraints from --include-module into concrete tags to pull.
// Returns nil when no explicit version tags should be added for this module.
//
// For semver constraints (caret, tilde, ranges) only the highest patch in each
// (major, minor) bucket that satisfies the constraint is returned. This mirrors
// the platform-level discovery rule (filterOnlyLatestPatches in
// internal/mirror/platform/platform.go) and avoids pulling N redundant patches
// per minor when the user wires a single module pin like `module@v1.6.0`.
//
// Anchor exception: versions named with an inclusive boundary operator (`>=`
// or `<=`) are always restored to the result if they exist in the registry.
// `>=1.40.0` literally encodes "v1.40.0 OR newer" — the user named v1.40.0
// by hand and we MUST honour that even when a newer patch (v1.40.1) exists
// in the same minor. Caret (`^`) and tilde (`~`) are syntactic shorthand
// for a range; their lower bounds are NOT anchors.
//
// Exact-tag constraints (`module@=vX.Y.Z`) bypass this filter — when the user
// asks for a specific tag they get exactly that tag.
//
// Channel snapshot versions (alpha/beta/early-access/stable/rock-solid) are
// merged into the pull list outside this method, so an older patch that a
// channel still points at remains reachable through the channel snapshot even
// when filterOnlyLatestPatches drops it from the constraint set.
func (f *Filter) VersionsToMirror(mod *Module) []string {
	constraint, hasConstraint := f.modules[mod.Name]
	if !hasConstraint {
		return nil
	}

	if constraint.IsExact() {
		exact, isExactTag := constraint.(*ExactTagConstraint)
		if !isExactTag {
			return nil
		}

		return []string{exact.Tag()}
	}

	semverConstraint, isSemver := constraint.(*SemanticVersionConstraint)
	if !isSemver {
		return nil
	}

	matched := make([]*semver.Version, 0)

	for _, v := range mod.Versions() {
		if semverConstraint.Match(v) {
			matched = append(matched, v)
		}
	}

	selected := filterOnlyLatestPatches(matched)
	selected = restoreInclusiveAnchors(selected, matched, semverConstraint.Anchors())

	tags := make([]string, 0, len(selected))
	for _, v := range selected {
		tags = append(tags, "v"+v.String())
	}

	return tags
}

// restoreInclusiveAnchors re-introduces any anchor versions (named via >=/<=
// in the user's constraint string) that were dropped by filterOnlyLatestPatches.
//
// An anchor is only restored when it is present in `available` (i.e. the
// constraint actually matched it against the registry's tag list). This guards
// against two failure modes:
//   - silently widening the constraint by appending versions the user
//     contradicted in another sub-constraint;
//   - emitting tags the registry doesn't have.
//
// The function preserves the order of `selected` and appends restored anchors
// at the end. Duplicates are de-duplicated by version equality.
func restoreInclusiveAnchors(selected, available []*semver.Version, anchors []*semver.Version) []*semver.Version {
	if len(anchors) == 0 {
		return selected
	}

	availableByKey := make(map[string]*semver.Version, len(available))
	for _, v := range available {
		if v == nil {
			continue
		}

		availableByKey[v.String()] = v
	}

	selectedKeys := make(map[string]struct{}, len(selected))
	for _, v := range selected {
		selectedKeys[v.String()] = struct{}{}
	}

	for _, anchor := range anchors {
		key := anchor.String()
		if _, already := selectedKeys[key]; already {
			continue
		}

		registryVersion, isAvailable := availableByKey[key]
		if !isAvailable {
			continue
		}

		selected = append(selected, registryVersion)
		selectedKeys[key] = struct{}{}
	}

	return selected
}

// filterOnlyLatestPatches keeps a single highest-patch version for every
// (major, minor) bucket. It is the modules-package counterpart of the same
// helper used by platform release discovery, kept private to avoid an
// import cycle and to make it easy for both packages to evolve independently.
func filterOnlyLatestPatches(versions []*semver.Version) []*semver.Version {
	type majorMinor struct {
		major uint64
		minor uint64
	}

	latest := make(map[majorMinor]*semver.Version, len(versions))
	for _, v := range versions {
		if v == nil {
			continue
		}

		key := majorMinor{major: v.Major(), minor: v.Minor()}

		current, ok := latest[key]
		if !ok || v.GreaterThan(current) {
			latest[key] = v
		}
	}

	result := make([]*semver.Version, 0, len(latest))
	for _, v := range latest {
		result = append(result, v)
	}

	return result
}
