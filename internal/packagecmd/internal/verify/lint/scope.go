package lint

import "slices"

// Scope names one verification target. The three sources a package is verified from
// carry different files, so which linters and rules are processed is a property of the
// tool rather than of a package: .pkglint.yaml tunes severities and can silence a rule
// with an ignored impact, but it cannot switch one on or off. That membership is
// declared in code, alongside the linters themselves.
type Scope string

const (
	// ScopeStatic verifies a package directory on disk, i.e. the full source tree.
	ScopeStatic Scope = "static"
	// ScopeBundle verifies the bundle image, which carries the renderable package:
	// charts, templates, openapi, docs, metadata and a generated .helmignore.
	ScopeBundle Scope = "bundle"
	// ScopeRelease verifies the release image, which carries release metadata only:
	// version.json, package.yaml, changelog.yaml, openapi, docs and the icon.
	ScopeRelease Scope = "release"
)

// PackageType names a kind of package. It mirrors the type recorded in package.yaml
// without importing the package loader, which sits above this package in the layering.
// Conversion happens once, where the definition is read.
type PackageType string

const (
	// TypeApplication is a package deploying per-instance resources.
	TypeApplication PackageType = "Application"
	// TypeModule is a package deploying a cluster module, with hooks and CRDs.
	TypeModule PackageType = "Module"
)

// AllTypes lists every package type, for linters that apply to all of them.
var AllTypes = []PackageType{TypeApplication, TypeModule}

// Target identifies one verification pass: the kind of package being verified and the
// source it is read from. Linters and rules declare which targets they are processed in.
type Target struct {
	// Type is the kind of package, taken from package.yaml.
	Type PackageType
	// Scope is the source the package is read from.
	Scope Scope
}

// String renders the target as "<type>/<scope>" for diagnostics and errors.
func (t Target) String() string {
	return string(t.Type) + "/" + string(t.Scope)
}

// Scopes is a set of verification scopes.
type Scopes []Scope

// AllScopes lists every scope, for linters that apply to all of them.
var AllScopes = Scopes{ScopeStatic, ScopeBundle, ScopeRelease}

// Contains reports whether scope is a member of s.
func (s Scopes) Contains(scope Scope) bool {
	return slices.Contains(s, scope)
}

// TypeScopes declares, per package type, the scopes a linter or rule is processed in.
// A package type absent from the map is not processed at all, so adding a package type
// is a deliberate act: every linter has to opt into it.
type TypeScopes map[PackageType]Scopes

// Contains reports whether target is covered by t.
func (t TypeScopes) Contains(target Target) bool {
	return t[target.Type].Contains(target.Scope)
}

// EveryType applies the same scopes to every package type. Use it for linters and rules
// whose applicability depends only on which files a source carries, not on the kind of
// package; spell the map out instead when the two types genuinely differ.
func EveryType(scopes ...Scope) TypeScopes {
	ts := make(TypeScopes, len(AllTypes))
	for _, packageType := range AllTypes {
		ts[packageType] = scopes
	}

	return ts
}

// RuleScopes narrows individual rules to fewer targets than their linter runs in, keyed
// by rule ID. A rule absent from the map inherits its linter's targets, so only the
// exceptions need listing.
type RuleScopes map[string]TypeScopes

// Runs reports whether the rule is processed for target, falling back to the linter's
// own targets when the rule declares none of its own.
func (r RuleScopes) Runs(ruleID string, linterScopes TypeScopes, target Target) bool {
	if scopes, ok := r[ruleID]; ok {
		return scopes.Contains(target)
	}

	return linterScopes.Contains(target)
}
