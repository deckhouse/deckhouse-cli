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
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	dkpclient "github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/errmatch"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	pluginlayout "github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/requirements"
)

// maxResolveDepth caps plugin dependency recursion, same as the install-time
// planner: deeper chains are a contract-authoring error, not a real graph.
const maxResolveDepth = 16

// resolver implements Resolver: for every bundled version of every mirrored
// module it picks the newest plugin version the bundle satisfies, resolves
// transitive mandatory plugin dependencies, and applies --include-plugin on
// top. Content problems (no compatible version, broken published contract)
// skip a plugin with a recorded reason; transport errors fail the whole
// resolution. An explicitly included plugin that cannot be resolved is an
// error, not a skip.
type resolver struct {
	catalog Catalog
	logger  *dkplog.Logger
}

// NewResolver creates a resolver over the given catalog.
func NewResolver(catalog Catalog, logger *dkplog.Logger) Resolver {
	return &resolver{catalog: catalog, logger: logger}
}

func (r *resolver) Resolve(ctx context.Context, in ResolveInput) (*Resolution, error) {
	st := &resolveState{
		catalog:  r.catalog,
		in:       in,
		bundle:   make(map[moduleName][]*semver.Version, len(in.Modules)),
		selected: make(selectionSet),
		warnings: newWarningLog(),
		logger:   r.logger,
	}

	for _, module := range in.Modules {
		st.bundle[module.Name] = module.Versions
	}

	if len(st.bundle) > 0 {
		if err := st.resolveAuto(ctx); err != nil {
			return nil, err
		}
	}

	if err := st.resolveExplicit(ctx); err != nil {
		return nil, err
	}

	return st.result(), nil
}

// resolveState is the working state of one Resolve call.
type resolveState struct {
	catalog Catalog
	in      ResolveInput
	// bundle maps a mirrored module to its bundled versions.
	bundle map[moduleName][]*semver.Version

	selected selectionSet
	skipped  []SkippedPlugin
	warnings *warningLog

	logger *dkplog.Logger
}

// selectionSet accumulates the resolution outcome: which versions of which
// plugins enter the bundle, with provenance on every version.
type selectionSet map[pluginName]map[versionTag]*SelectedVersion

// commit adds a plugin version to the set; when the version is already
// selected, only the provenance edge is appended.
func (s selectionSet) commit(name pluginName, version *semver.Version, contract *internal.Plugin, reason Reason) {
	byVersion, ok := s[name]
	if !ok {
		byVersion = make(map[versionTag]*SelectedVersion)
		s[name] = byVersion
	}

	tag := version.Original()

	sv, ok := byVersion[tag]
	if !ok {
		sv = &SelectedVersion{Version: version, Contract: contract}
		byVersion[tag] = sv
	}

	addReason(sv, reason)
}

// at returns the selection stored under exact (name, tag), nil if absent.
func (s selectionSet) at(name pluginName, tag versionTag) *SelectedVersion {
	return s[name][tag]
}

// version is at() keyed by a parsed version.
func (s selectionSet) version(name pluginName, version *semver.Version) *SelectedVersion {
	return s.at(name, version.Original())
}

// warningLog collects user-facing warnings in emission order, deduplicated.
type warningLog struct {
	seen     map[string]struct{}
	messages []string
}

func newWarningLog() *warningLog {
	return &warningLog{seen: make(map[string]struct{})}
}

func (w *warningLog) add(msg string) {
	if _, dup := w.seen[msg]; dup {
		return
	}

	w.seen[msg] = struct{}{}
	w.messages = append(w.messages, msg)
}

// resolveAuto selects plugins for the bundle's modules: every plugin in the
// catalog whose contract names a mirrored module is paired with each bundled
// version of that module.
func (st *resolveState) resolveAuto(ctx context.Context) error {
	names, err := st.catalog.PluginNames(ctx)
	if err != nil {
		if isNotPublished(err) {
			// The source registry has no plugins catalog (older registries,
			// self-hosted mirrors). Nothing to auto-select; explicit includes
			// still resolve against their own repositories.
			st.logger.Debug("The registry has no plugins catalog, skipping plugin auto-selection")

			return nil
		}

		return fmt.Errorf("list plugins catalog: %w", err)
	}

	sort.Strings(names)

	for _, name := range names {
		// The catalog is registry data. A malformed entry cannot name a
		// published plugin, so it is dropped rather than failing the pull.
		if err := pluginlayout.ValidatePluginName(name); err != nil {
			st.logger.Debug(fmt.Sprintf("Skipping catalog entry: %v", err))

			continue
		}

		if err := st.resolveAutoPlugin(ctx, name); err != nil {
			return err
		}
	}

	return nil
}

func (st *resolveState) resolveAutoPlugin(ctx context.Context, name string) error {
	versions, err := st.catalog.PluginVersions(ctx, name)
	if err != nil {
		if isNotPublished(err) {
			// A name tag in the catalog index without a version repo behind it.
			st.logger.Debug(fmt.Sprintf("Plugin %q is in the catalog index but has no published versions", name))

			return nil
		}

		return err
	}

	if len(versions) == 0 {
		return nil
	}

	// Relevance is judged by the newest readable contract: it is the plugin's
	// current self-description. Older contracts may reference abandoned
	// module integrations and must not resurrect them.
	top, err := st.newestReadableContract(ctx, name, versions)
	if err != nil {
		return err
	}

	if top == nil {
		// Every published version carries a broken contract - relevance
		// cannot be judged. Surface the broken publication to the operator.
		st.skipped = append(st.skipped, SkippedPlugin{Name: name, Reason: "no readable contract in any published version"})

		return nil
	}

	triggers := triggeringModules(top, st.bundle)
	if len(triggers) == 0 {
		return nil
	}

	var failures []string

	for _, module := range triggers {
		for _, moduleVersion := range st.bundle[module] {
			failure, err := st.selectForModuleVersion(ctx, name, versions, module, moduleVersion)
			if err != nil {
				return err
			}

			if failure != "" {
				failures = append(failures, fmt.Sprintf("for module %s %s: %s", module, moduleVersion.Original(), failure))
			}
		}
	}

	if len(failures) == 0 {
		return nil
	}

	detail := strings.Join(failures, "; ")

	// A plugin that still got some version into the bundle is not skipped:
	// the leftover per-module-version failures become advisories.
	if len(st.selected[name]) > 0 {
		st.warnings.add(fmt.Sprintf("plugin %s: %s", name, detail))

		return nil
	}

	st.skipped = append(st.skipped, SkippedPlugin{Name: name, Reason: detail})

	return nil
}

// newestReadableContract walks versions newest first and returns the first
// contract that decodes. Versions with broken published contracts are passed
// over; nil means no version has a readable contract.
func (st *resolveState) newestReadableContract(ctx context.Context, name string, versions []*semver.Version) (*internal.Plugin, error) {
	for _, version := range versions {
		contract, err := st.catalog.Contract(ctx, name, version)
		if err != nil {
			if errors.Is(err, ErrInvalidContract) {
				continue
			}

			return nil, err
		}

		return contract, nil
	}

	return nil, nil
}

// selectForModuleVersion picks the newest plugin version compatible with one
// bundled module version and commits it (with its dependency closure). An
// empty failure return means a version was selected; a non-empty one carries
// the newest candidate's rejection reason.
func (st *resolveState) selectForModuleVersion(ctx context.Context, name string, versions []*semver.Version, module string, moduleVersion *semver.Version) (string, error) {
	var firstReject string

	for _, candidate := range versions {
		contract, err := st.catalog.Contract(ctx, name, candidate)
		if err != nil {
			if errors.Is(err, ErrInvalidContract) {
				noteReject(&firstReject, fmt.Sprintf("%s: broken published contract", candidate.Original()))

				continue
			}

			return "", err
		}

		why := st.pairingGate(contract, module, moduleVersion)
		if why != "" {
			noteReject(&firstReject, fmt.Sprintf("%s: %s", candidate.Original(), why))

			continue
		}

		_, declaredConstraint := moduleConstraint(contract, module)
		reason := Reason{Kind: ReasonModule, Subject: module, Constraint: declaredConstraint}

		// The same version may win for several module versions: the first win
		// already resolved its dependencies, later wins only add provenance.
		if sv := st.selected.version(name, candidate); sv != nil {
			addReason(sv, reason)

			return "", nil
		}

		delta := &selectionDelta{}

		why, err = st.resolveDeps(ctx, contract, delta, map[pluginName]bool{name: true}, []string{name + "@" + candidate.Original()}, 0, true)
		if err != nil {
			return "", err
		}

		if why != "" {
			noteReject(&firstReject, fmt.Sprintf("%s: %s", candidate.Original(), why))

			continue
		}

		st.selected.commit(name, candidate, contract, reason)
		st.applyDelta(delta)

		return "", nil
	}

	if firstReject == "" {
		firstReject = "no published versions"
	}

	return firstReject, nil
}

// resolveExplicit applies --include-plugin entries on top of the module-driven
// selection. Failures here are the user's explicit request going unmet, so
// they are errors, not skips.
//
// Exact pins are all committed before any dependency is resolved: a
// dependency the user pinned is then reused by every other explicit plugin,
// whatever the name order.
func (st *resolveState) resolveExplicit(ctx context.Context) error {
	// Only a whitelist filter carries --include-plugin names; anything else
	// (including the empty filter, which is a blacklist) has none.
	if st.in.Filter == nil || !st.in.Filter.IsWhitelist() || st.in.Filter.Len() == 0 {
		return nil
	}

	names := st.in.Filter.ModuleNames()

	for _, name := range names {
		if err := pluginlayout.ValidatePluginName(name); err != nil {
			return fmt.Errorf("--include-plugin %s: %w", name, err)
		}
	}

	var pins []explicitPin

	for _, name := range names {
		constraint, _ := st.in.Filter.GetConstraint(name)

		// Exact tag pins bypass the stable-version list, so pre-releases stay
		// reachable - the mirror analog of `d8 plugins install --version`.
		for _, exact := range modules.ExactConstraintsOf(constraint) {
			pin, err := st.pinExplicitExact(ctx, name, exact.Tag())
			if err != nil {
				return err
			}

			pins = append(pins, pin)
		}
	}

	for _, pin := range pins {
		if err := st.resolvePinDeps(ctx, pin); err != nil {
			return err
		}
	}

	for _, name := range names {
		constraint, _ := st.in.Filter.GetConstraint(name)

		if err := st.resolveExplicitRanges(ctx, name, constraint); err != nil {
			return err
		}
	}

	return nil
}

// explicitPin is an exactly pinned --include-plugin entry, committed to the
// selection with its dependencies still to be resolved.
type explicitPin struct {
	name     pluginName
	tag      versionTag
	contract *internal.Plugin
}

// resolveExplicitRanges resolves the semver ranges of one --include-plugin
// entry (or "newest stable" for a bare name).
func (st *resolveState) resolveExplicitRanges(ctx context.Context, name string, constraint modules.VersionConstraint) error {
	subject := "--include-plugin " + name

	// Nothing ranged left when the pin was exact-only.
	ranges := modules.SemverConstraintsOf(constraint)
	if constraint != nil && len(ranges) == 0 {
		return nil
	}

	versions, err := st.catalog.PluginVersions(ctx, name)
	if err != nil {
		if isNotPublished(err) {
			return fmt.Errorf("--include-plugin %s: plugin is not published under deckhouse-cli/plugins", name)
		}

		return err
	}

	if constraint == nil {
		return st.selectExplicitRanged(ctx, name, versions, nil, subject)
	}

	// Every semver range the user declared must be met on its own: repeated
	// --include-plugin entries OR-combine into disjoint ranges, and one
	// picked version must not silently swallow another range.
	for _, sub := range ranges {
		if err := st.selectExplicitRanged(ctx, name, versions, sub, subject); err != nil {
			return err
		}
	}

	return nil
}

// selectExplicitRanged picks the newest version matching one semver range (or
// any version when sub is nil) whose contract is readable and dependencies
// resolve. Bundle-gate violations warn: the user's explicit choice wins, and
// modules are never auto-added for a plugin.
func (st *resolveState) selectExplicitRanged(ctx context.Context, name pluginName, versions []*semver.Version, sub *modules.SemanticVersionConstraint, subject string) error {
	var rejections []string

	for _, candidate := range versions {
		if sub != nil && !sub.Match(candidate) {
			continue
		}

		contract, err := st.catalog.Contract(ctx, name, candidate)
		if err != nil {
			if errors.Is(err, ErrInvalidContract) {
				rejections = append(rejections, fmt.Sprintf("%s: broken published contract", candidate.Original()))

				continue
			}

			return err
		}

		delta := &selectionDelta{}

		why, err := st.resolveDeps(ctx, contract, delta, map[pluginName]bool{name: true}, []string{name + "@" + candidate.Original()}, 0, false)
		if err != nil {
			return err
		}

		if why != "" {
			rejections = append(rejections, fmt.Sprintf("%s: %s", candidate.Original(), why))

			continue
		}

		st.selected.commit(name, candidate, contract, Reason{Kind: ReasonExplicit, Subject: subject})
		st.applyDelta(delta)

		if gateWarn := st.bundleGate(contract, ""); gateWarn != "" {
			st.warnings.add(fmt.Sprintf("plugin %s@%s (explicitly included): %s; the target cluster must provide it", name, candidate.Original(), gateWarn))
		}

		return nil
	}

	if len(rejections) == 0 {
		return fmt.Errorf("--include-plugin %s: no published version matches the requested constraint; pin an exact version with %s@=vX.Y.Z if it is a pre-release", name, name)
	}

	return fmt.Errorf("--include-plugin %s: no suitable version: %s", name, strings.Join(rejections, "; "))
}

// pinExplicitExact reads the contract of an exactly pinned version and
// commits the version to the selection. Dependencies are resolved later by
// resolvePinDeps, once every pin is in.
func (st *resolveState) pinExplicitExact(ctx context.Context, name, tag string) (explicitPin, error) {
	version, err := semver.NewVersion(tag)
	if err != nil {
		return explicitPin{}, fmt.Errorf("--include-plugin %s: %q is not a semver version tag", name, tag)
	}

	contract, err := st.catalog.Contract(ctx, name, version)
	if err != nil {
		if isNotPublished(err) {
			return explicitPin{}, fmt.Errorf("--include-plugin %s: version %s is not published", name, tag)
		}

		// Shipping an exactly pinned image whose requirements cannot be read
		// would break air-gapped installs silently, so this is fatal.
		return explicitPin{}, fmt.Errorf("--include-plugin %s: %w", name, err)
	}

	st.selected.commit(name, version, contract, Reason{Kind: ReasonExplicit, Subject: "--include-plugin " + name, Constraint: "=" + tag})

	return explicitPin{name: name, tag: tag, contract: contract}, nil
}

// resolvePinDeps resolves the mandatory plugin dependencies of one exact pin.
// Unmet dependencies are the user's explicit request going unmet: an error.
func (st *resolveState) resolvePinDeps(ctx context.Context, pin explicitPin) error {
	delta := &selectionDelta{}

	why, err := st.resolveDeps(ctx, pin.contract, delta, map[pluginName]bool{pin.name: true}, []string{pin.name + "@" + pin.tag}, 0, false)
	if err != nil {
		return err
	}

	if why != "" {
		return fmt.Errorf("--include-plugin %s@=%s: unresolved dependencies: %s", pin.name, pin.tag, why)
	}

	st.applyDelta(delta)

	if gateWarn := st.bundleGate(pin.contract, ""); gateWarn != "" {
		st.warnings.add(fmt.Sprintf("plugin %s@%s (explicitly included): %s; the target cluster must provide it", pin.name, pin.tag, gateWarn))
	}

	return nil
}

// resolveDeps resolves the contract's mandatory plugin dependencies into
// delta, recursively. A non-empty reject reason means the candidate that
// declared these dependencies must be passed over; an error is operational
// and fails the resolution. enforceGate tells whether a dependency failing
// the bundle gate rejects it (auto-selected chains) or only warns (chains of
// an explicitly included plugin - the user's choice wins there).
func (st *resolveState) resolveDeps(ctx context.Context, contract *internal.Plugin, delta *selectionDelta, visited map[pluginName]bool, path []string, depth int, enforceGate bool) (string, error) {
	if depth > maxResolveDepth {
		return fmt.Sprintf("dependency chain deeper than %d: %s", maxResolveDepth, strings.Join(path, " -> ")), nil
	}

	for _, req := range contract.Requirements.Plugins.Mandatory {
		if _, builtin := st.in.Builtins[req.Name]; builtin {
			// Built-in d8 commands satisfy a same-named dependency by
			// presence; there is nothing to pull.
			continue
		}

		// The dependency name comes from a published contract - external
		// data with no grammar of its own - and becomes a registry route
		// and a filesystem path.
		if err := pluginlayout.ValidatePluginName(req.Name); err != nil {
			return fmt.Sprintf("dependency of %s: %v", path[len(path)-1], err), nil
		}

		if visited[req.Name] {
			return fmt.Sprintf("dependency cycle: %s -> %s", strings.Join(path, " -> "), req.Name), nil
		}

		var constraint *semver.Constraints

		if req.Constraint != "" {
			parsed, err := semver.NewConstraint(req.Constraint)
			if err != nil {
				return fmt.Sprintf("invalid constraint %q for dependency %q", req.Constraint, req.Name), nil
			}

			constraint = parsed
		}

		reason := Reason{Kind: ReasonDependency, Subject: path[len(path)-1], Constraint: req.Constraint}

		// Union-reuse: a version already picked for the bundle that satisfies
		// this constraint is shared instead of adding another one.
		if reused := st.findSatisfying(delta, req.Name, constraint); reused != "" {
			delta.reasons = append(delta.reasons, deltaReason{name: req.Name, version: reused, reason: reason})

			continue
		}

		reject, err := st.resolveDepFresh(ctx, req, constraint, reason, delta, visited, path, depth, enforceGate)
		if reject != "" || err != nil {
			return reject, err
		}
	}

	return "", nil
}

// resolveDepFresh picks the newest version of one dependency that satisfies
// the constraint, passes the bundle gate, and resolves its own dependencies.
func (st *resolveState) resolveDepFresh(ctx context.Context, req internal.PluginRequirement, constraint *semver.Constraints, reason Reason, delta *selectionDelta, visited map[pluginName]bool, path []string, depth int, enforceGate bool) (string, error) {
	if st.in.NoCatalog {
		// Nothing to pick a version from: only a pinned version could have
		// satisfied this dependency, and none did.
		return fmt.Sprintf("dependency %q (constraint %q): no pinned version satisfies it; with --proxy-registry pin it explicitly: --include-plugin %s@=<version>", req.Name, req.Constraint, req.Name), nil
	}

	versions, err := st.catalog.PluginVersions(ctx, req.Name)
	if err != nil {
		if isNotPublished(err) {
			return fmt.Sprintf("dependency %q is not published in the plugins catalog", req.Name), nil
		}

		return "", err
	}

	var firstReject string

	for _, version := range versions {
		if constraint != nil && !constraint.Check(version) {
			continue
		}

		contract, err := st.catalog.Contract(ctx, req.Name, version)
		if err != nil {
			if errors.Is(err, ErrInvalidContract) {
				noteReject(&firstReject, fmt.Sprintf("%s: broken published contract", version.Original()))

				continue
			}

			return "", err
		}

		if why := st.bundleGate(contract, ""); why != "" {
			if enforceGate {
				noteReject(&firstReject, fmt.Sprintf("%s: %s", version.Original(), why))

				continue
			}

			st.warnings.add(fmt.Sprintf("dependency %s@%s of an explicitly included plugin: %s; the target cluster must provide it", req.Name, version.Original(), why))
		}

		childPath := append(append(make([]string, 0, len(path)+1), path...), req.Name+"@"+version.Original())

		mark := delta.checkpoint()
		visited[req.Name] = true

		why, err := st.resolveDeps(ctx, contract, delta, visited, childPath, depth+1, enforceGate)

		delete(visited, req.Name)

		if err != nil {
			return "", err
		}

		if why != "" {
			delta.rollback(mark)
			noteReject(&firstReject, fmt.Sprintf("%s: %s", version.Original(), why))

			continue
		}

		delta.adds = append(delta.adds, deltaAdd{name: req.Name, version: version, contract: contract, reason: reason})

		return "", nil
	}

	detail := "no published version satisfies it"
	if firstReject != "" {
		detail = firstReject
	}

	return fmt.Sprintf("dependency %q (constraint %q): %s", req.Name, req.Constraint, detail), nil
}

// pairingGate checks one candidate contract against one bundled module
// version: the contract must integrate the module, the module version must
// satisfy the declared constraint, and the rest of the contract must be
// satisfiable by the bundle. Empty return means the gate passes.
func (st *resolveState) pairingGate(contract *internal.Plugin, module string, moduleVersion *semver.Version) string {
	declared, constraintStr := moduleConstraint(contract, module)
	if !declared {
		return fmt.Sprintf("does not integrate module %q", module)
	}

	if constraintStr != "" {
		constraint, err := semver.NewConstraint(constraintStr)
		if err != nil {
			return fmt.Sprintf("invalid constraint %q for module %q", constraintStr, module)
		}

		// Same normalization as install-time: CI/build markers on the module
		// version are stripped, so ">=1.0" matches "v1.2.3-dev".
		if !constraint.Check(requirements.NormalizedForConstraint(moduleVersion)) {
			return fmt.Sprintf("requires module %q %s", module, constraintStr)
		}
	}

	return st.bundleGate(contract, module)
}

// bundleGate checks the bundle-verifiable requirements of a contract, apart
// from the paired module (already checked by pairingGate; empty when there is
// no pairing). Kubernetes and noneOf requirements are cluster-side and are
// left to install-time validation. Empty return means the gate passes.
func (st *resolveState) bundleGate(contract *internal.Plugin, pairedModule string) string {
	for _, req := range contract.Requirements.Modules.Mandatory {
		if req.Name == pairedModule {
			continue
		}

		versions, inBundle := st.bundle[req.Name]
		if !inBundle || len(versions) == 0 {
			return fmt.Sprintf("requires module %q which is not in the bundle", req.Name)
		}

		if ok := anySatisfies(versions, req.Constraint); !ok {
			return fmt.Sprintf("requires module %q %s, bundle has %s", req.Name, req.Constraint, formatVersions(versions))
		}
	}

	// anyOf groups are checked in full even when they contain the paired
	// module: the pairing may have matched a weaker constraint from the
	// mandatory bucket, while the group's own member constraint is stricter.
	// When the pairing did satisfy the group's member, anyOfSatisfied is true
	// anyway (the paired version is one of the bundled ones).
	for _, group := range contract.Requirements.Modules.AnyOf {
		if !st.anyOfSatisfied(group) {
			return fmt.Sprintf("no module of group %q is in the bundle at a satisfying version", group.Name)
		}
	}

	if constraint := contract.Requirements.Deckhouse.Constraint; constraint != "" && len(st.in.PlatformVersions) > 0 {
		if ok := anySatisfies(st.in.PlatformVersions, constraint); !ok {
			return fmt.Sprintf("requires Deckhouse %q, bundle platform versions: %s", constraint, formatVersions(st.in.PlatformVersions))
		}
	}

	return ""
}

func (st *resolveState) anyOfSatisfied(group internal.ModuleGroup) bool {
	for _, member := range group.Modules {
		versions, inBundle := st.bundle[member.Name]
		if !inBundle || len(versions) == 0 {
			continue
		}

		if anySatisfies(versions, member.Constraint) {
			return true
		}
	}

	return false
}

// selectionDelta accumulates tentative dependency picks while one candidate
// version is evaluated. It is committed only when the whole candidate
// resolves; checkpoints let a failed dependency branch roll back its own
// additions without touching sibling branches.
type selectionDelta struct {
	adds    []deltaAdd
	reasons []deltaReason
}

type deltaAdd struct {
	name     pluginName
	version  *semver.Version
	contract *internal.Plugin
	reason   Reason
}

type deltaReason struct {
	name    pluginName
	version versionTag
	reason  Reason
}

type deltaMark struct {
	adds    int
	reasons int
}

func (d *selectionDelta) checkpoint() deltaMark {
	return deltaMark{adds: len(d.adds), reasons: len(d.reasons)}
}

func (d *selectionDelta) rollback(m deltaMark) {
	d.adds = d.adds[:m.adds]
	d.reasons = d.reasons[:m.reasons]
}

func (st *resolveState) applyDelta(delta *selectionDelta) {
	for _, add := range delta.adds {
		st.selected.commit(add.name, add.version, add.contract, add.reason)
	}

	for _, dr := range delta.reasons {
		if sv := st.selected.at(dr.name, dr.version); sv != nil {
			addReason(sv, dr.reason)
		}
	}
}

// findSatisfying returns the newest already-picked version of the plugin
// (committed or pending in delta) that satisfies the constraint, "" if none.
func (st *resolveState) findSatisfying(delta *selectionDelta, name pluginName, constraint *semver.Constraints) versionTag {
	var best *semver.Version

	consider := func(v *semver.Version) {
		if constraint != nil && !constraint.Check(v) {
			return
		}

		if best == nil || v.GreaterThan(best) {
			best = v
		}
	}

	for _, sv := range st.selected[name] {
		consider(sv.Version)
	}

	for _, add := range delta.adds {
		if add.name == name {
			consider(add.version)
		}
	}

	if best == nil {
		return ""
	}

	return best.Original()
}

// result assembles the final Resolution: plugins sorted by name, versions
// newest first, plus co-installation advisories over the selected set.
func (st *resolveState) result() *Resolution {
	res := &Resolution{Skipped: st.skipped}

	names := make([]string, 0, len(st.selected))
	for name := range st.selected {
		names = append(names, name)
	}

	sort.Strings(names)

	for _, name := range names {
		byVersion := st.selected[name]

		versions := make([]SelectedVersion, 0, len(byVersion))
		for _, sv := range byVersion {
			versions = append(versions, *sv)
		}

		sort.Slice(versions, func(i, j int) bool { return versions[i].Version.GreaterThan(versions[j].Version) })

		res.Plugins = append(res.Plugins, PluginToMirror{Name: name, Versions: versions})
	}

	for _, plugin := range res.Plugins {
		for _, sv := range plugin.Versions {
			st.advisories(plugin.Name, sv)
		}
	}

	res.Warnings = st.warnings.messages

	return res
}

// advisories emits non-blocking warnings about conditional requirements the
// bundle cannot satisfy. Conditional requirements never gate mirroring (the
// module or plugin may simply not be enabled on the target cluster), but the
// operator should know about the co-installation hazard.
func (st *resolveState) advisories(name string, sv SelectedVersion) {
	for _, req := range sv.Contract.Requirements.Modules.Conditional {
		versions, inBundle := st.bundle[req.Name]
		if !inBundle || len(versions) == 0 || req.Constraint == "" {
			continue
		}

		if !anySatisfies(versions, req.Constraint) {
			st.warnings.add(fmt.Sprintf("plugin %s@%s: module %q is in the bundle, but no bundled version satisfies its conditional constraint %q",
				name, sv.Version.Original(), req.Name, req.Constraint))
		}
	}

	for _, req := range sv.Contract.Requirements.Plugins.Conditional {
		byVersion := st.selected[req.Name]
		if len(byVersion) == 0 || req.Constraint == "" {
			continue
		}

		satisfied := false

		for _, other := range byVersion {
			if anySatisfies([]*semver.Version{other.Version}, req.Constraint) {
				satisfied = true

				break
			}
		}

		if !satisfied {
			st.warnings.add(fmt.Sprintf("plugins %s@%s and %s cannot be installed together: no bundled version of %q satisfies the conditional constraint %q",
				name, sv.Version.Original(), req.Name, req.Name, req.Constraint))
		}
	}
}

// triggeringModules returns the mirrored modules the contract names in its
// mandatory or anyOf requirements - the modules this plugin exists for.
// Conditional mentions do not trigger: install never auto-installs on a
// conditional, so mirror does not auto-pull on one either.
func triggeringModules(contract *internal.Plugin, bundle map[string][]*semver.Version) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, 1)

	add := func(name string) {
		if len(bundle[name]) == 0 {
			return
		}

		if _, dup := seen[name]; dup {
			return
		}

		seen[name] = struct{}{}
		out = append(out, name)
	}

	for _, req := range contract.Requirements.Modules.Mandatory {
		add(req.Name)
	}

	for _, group := range contract.Requirements.Modules.AnyOf {
		for _, member := range group.Modules {
			add(member.Name)
		}
	}

	sort.Strings(out)

	return out
}

// moduleConstraint reports whether the contract declares the module (in
// mandatory or anyOf) and the constraint it declares for it.
func moduleConstraint(contract *internal.Plugin, module string) (bool, string) {
	for _, req := range contract.Requirements.Modules.Mandatory {
		if req.Name == module {
			return true, req.Constraint
		}
	}

	for _, group := range contract.Requirements.Modules.AnyOf {
		for _, member := range group.Modules {
			if member.Name == module {
				return true, member.Constraint
			}
		}
	}

	return false, ""
}

func addReason(sv *SelectedVersion, reason Reason) {
	for _, existing := range sv.Reasons {
		if existing == reason {
			return
		}
	}

	sv.Reasons = append(sv.Reasons, reason)
}

// noteReject keeps the first (newest candidate's) rejection reason - the most
// useful one for the user, since newer versions are preferred.
func noteReject(dst *string, reason string) {
	if *dst == "" {
		*dst = reason
	}
}

// anySatisfies reports whether at least one version satisfies the constraint.
// An empty constraint is satisfied by anything; an unparseable one by nothing.
// Versions are normalized the same way install-time checks normalize them, so
// CI/build markers (e.g. "-dev") do not fail plain constraints.
func anySatisfies(versions []*semver.Version, constraintStr string) bool {
	if constraintStr == "" {
		return true
	}

	constraint, err := semver.NewConstraint(constraintStr)
	if err != nil {
		return false
	}

	for _, version := range versions {
		if constraint.Check(requirements.NormalizedForConstraint(version)) {
			return true
		}
	}

	return false
}

func formatVersions(versions []*semver.Version) string {
	parts := make([]string, 0, len(versions))
	for _, version := range versions {
		parts = append(parts, version.Original())
	}

	return strings.Join(parts, ", ")
}

// isNotPublished tells a missing repository or tag apart from other registry
// errors: absence is content information ("not published"), everything else
// is operational.
func isNotPublished(err error) bool {
	return errors.Is(err, dkpclient.ErrImageNotFound) || errmatch.IsRepoNotFound(err) || errmatch.IsImageNotFound(err)
}
