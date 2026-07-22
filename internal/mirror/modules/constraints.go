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
	"regexp"

	"github.com/Masterminds/semver/v3"
)

type VersionConstraint interface {
	Match(version interface{}) bool
	IsExact() bool
	HasChannelAlias() bool
}

type SemanticVersionConstraint struct {
	constraint *semver.Constraints
	// anchors are versions explicitly named with an inclusive boundary
	// operator (>=, <=) in the user's constraint string. These versions
	// must round-trip through the latest-patch-per-minor filter — the user
	// has named them by hand, so dropping them in favor of a newer patch
	// in the same (major, minor) bucket would silently override an
	// explicit user choice.
	//
	// Only `>=`, `<=` (and their reversed forms `=>`, `=<`) contribute
	// anchors. Caret (`^`), tilde (`~`), and implicit constraints do not:
	// those operators are shorthand for a range and the patch filter is
	// free to collapse same-minor patches inside them.
	anchors []*semver.Version
	// lowerBound is the smallest version literal that appears in the
	// constraint string, regardless of which operator (^, ~, >=, =, etc.)
	// preceded it. It is intended for callers that cannot list registry
	// tags directly (proxy registries) and need a starting point to
	// enumerate (major, minor, patch) by incrementing from a known
	// version that the user has named.
	//
	// May be nil only when the constraint string contains no recognisable
	// version literal, which is rejected by NewSemanticVersionConstraint
	// before it reaches consumers.
	lowerBound *semver.Version
}

// anchorOpRegex captures version literals that follow an inclusive boundary
// operator. Exclusive bounds (>, <) and shorthand operators (^, ~) are
// intentionally excluded — see the anchors field doc above for the rationale.
//
// The version capture is deliberately permissive (any non-space, non-comma
// run); the result is re-validated through semver.NewVersion before being
// stored.
var anchorOpRegex = regexp.MustCompile(`(?:>=|<=|=>|=<)\s*([^\s,]+)`)

// versionLiteralRegex captures any version literal in the constraint string,
// regardless of the operator preceding it (or absence thereof). Used by
// LowerBound to find a sensible probing entry point when callers can't list
// tags directly (e.g. proxy registries) and need to enumerate versions by
// incrementing patch/minor/major starting from the constraint's lowest
// explicitly-named version.
var versionLiteralRegex = regexp.MustCompile(`v?(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:[-+][0-9A-Za-z.\-]+)?`)

func NewSemanticVersionConstraint(c string) (*SemanticVersionConstraint, error) {
	constraint, err := semver.NewConstraint(c)
	if err != nil {
		return nil, fmt.Errorf("invalid semantic version constraint %q: %w", c, err)
	}

	anchors, err := extractInclusiveAnchors(c)
	if err != nil {
		return nil, fmt.Errorf("invalid semantic version constraint %q: %w", c, err)
	}

	lower, err := extractLowerBound(c)
	if err != nil {
		return nil, fmt.Errorf("invalid semantic version constraint %q: %w", c, err)
	}

	return &SemanticVersionConstraint{
		constraint: constraint,
		anchors:    anchors,
		lowerBound: lower,
	}, nil
}

// NewImplicitVersionConstraint builds the constraint for a bare, operator-less
// version literal (e.g. `alb@0.4.0`). Historically this was implemented by
// prepending a caret (`^0.4.0`), but Masterminds' caret special-cases the 0.x
// major line: `^0.4.0` expands to `>=0.4.0 <0.5.0`, locking the MINOR instead
// of the major. In a catch-up mirror that silently drops every intermediate
// 0.x minor (0.5.*, 0.6.* …), leaving gaps that block sequential upgrades.
//
// We instead expand a bare version to `>=X.Y.Z <(major+1).0.0`, treating major
// 0 like any other major. For major >= 1 this is byte-for-byte equivalent to
// the previous caret behaviour (`^1.52.0` == `>=1.52.0 <2.0.0`); for major 0 it
// now spans the whole 0.x line from the named version upward.
//
// The synthesized `>=` lower bound is deliberately NOT registered as an anchor:
// the user wrote a bare version, not an explicit inclusive boundary, so the
// latest-patch-per-minor filter stays free to collapse same-minor patches (see
// the anchors field doc). This preserves the issue #220 behaviour.
func NewImplicitVersionConstraint(version string) (*SemanticVersionConstraint, error) {
	ver, err := semver.NewVersion(version)
	if err != nil {
		return nil, fmt.Errorf("invalid version %q: %w", version, err)
	}

	rangeStr := fmt.Sprintf(">= %s, < %d.0.0", ver.String(), ver.Major()+1)

	constraint, err := semver.NewConstraint(rangeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid implicit version constraint %q: %w", rangeStr, err)
	}

	return &SemanticVersionConstraint{
		constraint: constraint,
		anchors:    nil,
		lowerBound: ver,
	}, nil
}

// Anchors returns the versions explicitly named with an inclusive boundary
// operator (>=, <=). Callers must re-check membership against the constraint
// itself (Match) before consuming an anchor: an anchor that fails Match means
// the user wrote a contradictory constraint (e.g. `>=2.0.0 <1.0.0`) and we
// will not silently widen the range.
func (s *SemanticVersionConstraint) Anchors() []*semver.Version {
	return s.anchors
}

// LowerBound returns the smallest version literal found in the constraint
// string. This is the natural starting point for callers that enumerate
// (major, minor, patch) by probing the registry one tag at a time (proxy
// registry mode) — incrementing forward from a version the user has
// explicitly named avoids scanning from v0.0.0.
//
// Returns nil only when the constraint string contained no recognisable
// version literal. NewSemanticVersionConstraint refuses such constraints
// so consumers can treat a nil result as a programming error.
func (s *SemanticVersionConstraint) LowerBound() *semver.Version {
	return s.lowerBound
}

// extractLowerBound walks every version literal that appears in the
// constraint string (regardless of the operator preceding it) and returns
// the smallest one. We deliberately ignore operators here: the goal is a
// safe starting point for forward enumeration, not constraint semantics.
//
// Notes on operator-specific behaviour:
//   - `^X.Y.Z` / `~X.Y.Z` / implicit `X.Y.Z`: the only literal is X.Y.Z
//     and it is returned as-is.
//   - `>=A <=B` (or any range with two literals): the smaller of A and B
//     is returned so probing starts at the constraint's lower edge even
//     when the user wrote the bounds in a non-canonical order.
//   - `>A`: returned as A; the strict-greater semantics belong to Match,
//     not to the enumeration start point.
//
// Returns an error only on the (defensive) case where a literal that the
// regex extracted fails semver.NewVersion — which should be impossible
// because semver.NewConstraint already validated the string.
func extractLowerBound(constraintStr string) (*semver.Version, error) {
	matches := versionLiteralRegex.FindAllString(constraintStr, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("no version literal found in constraint")
	}

	var lowest *semver.Version

	for _, raw := range matches {
		v, err := semver.NewVersion(raw)
		if err != nil {
			// Defensive: the regex is permissive but constraint parsing
			// already accepted the string upstream. Surface a clear error
			// instead of silently dropping the literal.
			return nil, fmt.Errorf("version literal %q not parseable: %w", raw, err)
		}

		if lowest == nil || v.LessThan(lowest) {
			lowest = v
		}
	}

	if lowest == nil {
		return nil, fmt.Errorf("no parseable version literal in constraint")
	}

	return lowest, nil
}

// extractInclusiveAnchors finds every >=X / <=X literal in the constraint
// string and parses X with semver.NewVersion. Duplicates are removed.
// The returned slice is nil when no inclusive boundary literals are present.
func extractInclusiveAnchors(constraintStr string) ([]*semver.Version, error) {
	matches := anchorOpRegex.FindAllStringSubmatch(constraintStr, -1)
	if len(matches) == 0 {
		return nil, nil
	}

	seen := make(map[string]struct{}, len(matches))

	out := make([]*semver.Version, 0, len(matches))
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}

		raw := m[1]
		if _, dup := seen[raw]; dup {
			continue
		}

		seen[raw] = struct{}{}

		v, err := semver.NewVersion(raw)
		if err != nil {
			// The constraint already passed semver.NewConstraint, so this
			// only fires on programming errors in the regex (we'd extract
			// something that the constraint parser had accepted but the
			// version parser hadn't). Surface it as a real error.
			return nil, fmt.Errorf("anchor %q not a valid semver: %w", raw, err)
		}

		out = append(out, v)
	}

	return out, nil
}

func (s *SemanticVersionConstraint) HasChannelAlias() bool {
	return false
}

func (s *SemanticVersionConstraint) Match(version interface{}) bool {
	switch v := version.(type) {
	case *semver.Version:
		return s.constraint.Check(v)
	default:
		return false
	}
}

func (s *SemanticVersionConstraint) IsExact() bool {
	return false
}

type ExactTagConstraint struct {
	tag     string
	channel string
}

func (e *ExactTagConstraint) Tag() string {
	return e.tag
}

func (e *ExactTagConstraint) Channel() string {
	return e.channel
}

func NewExactTagConstraint(tag string) *ExactTagConstraint {
	return &ExactTagConstraint{tag: tag}
}

func NewExactTagConstraintWithChannel(tag string, channel string) *ExactTagConstraint {
	return &ExactTagConstraint{tag: tag, channel: channel}
}

func (e *ExactTagConstraint) Match(version interface{}) bool {
	switch v := version.(type) {
	case string:
		return e.tag == v
	default:
		return false
	}
}

func (e *ExactTagConstraint) IsExact() bool {
	return true
}

func (e *ExactTagConstraint) HasChannelAlias() bool {
	return e.channel != ""
}

// MultiConstraint is the OR-combination of several constraints declared for
// the same name. It is produced when a user repeats a name on the command
// line, e.g. `--include-package test@=v0.0.2 --include-package test@=v0.0.3`,
// so that all of the named versions are pulled instead of only the last one.
//
// A value Match-es when ANY sub-constraint matches. It is considered exact
// only when EVERY sub-constraint is exact (so a set of pinned tags keeps the
// "no release-channel discovery, no tag listing" fast paths), and it advertises
// a channel alias when ANY sub-constraint does.
type MultiConstraint struct {
	constraints []VersionConstraint
}

// Constraints returns the sub-constraints in declaration order.
func (m *MultiConstraint) Constraints() []VersionConstraint {
	return m.constraints
}

func (m *MultiConstraint) Match(version interface{}) bool {
	for _, c := range m.constraints {
		if c.Match(version) {
			return true
		}
	}

	return false
}

func (m *MultiConstraint) IsExact() bool {
	for _, c := range m.constraints {
		if !c.IsExact() {
			return false
		}
	}

	return true
}

func (m *MultiConstraint) HasChannelAlias() bool {
	for _, c := range m.constraints {
		if c.HasChannelAlias() {
			return true
		}
	}

	return false
}

// mergeConstraints OR-combines an already-registered constraint with an
// additional one declared for the same name, flattening nested
// MultiConstraints so repeated declarations stay a single flat list.
func mergeConstraints(existing, additional VersionConstraint) VersionConstraint {
	if multi, ok := existing.(*MultiConstraint); ok {
		multi.constraints = append(multi.constraints, additional)
		return multi
	}

	return &MultiConstraint{constraints: []VersionConstraint{existing, additional}}
}

// ExactConstraintsOf flattens a constraint into the exact-tag constraints it
// contains. A plain ExactTagConstraint yields itself; a MultiConstraint yields
// every exact sub-constraint; anything else yields nothing.
func ExactConstraintsOf(c VersionConstraint) []*ExactTagConstraint {
	switch t := c.(type) {
	case *ExactTagConstraint:
		return []*ExactTagConstraint{t}
	case *MultiConstraint:
		var out []*ExactTagConstraint
		for _, sub := range t.constraints {
			out = append(out, ExactConstraintsOf(sub)...)
		}

		return out
	default:
		return nil
	}
}

// SemverConstraintsOf flattens a constraint into the semantic-version
// constraints it contains, used by the proxy-registry probe which can only
// walk semver ranges.
func SemverConstraintsOf(c VersionConstraint) []*SemanticVersionConstraint {
	switch t := c.(type) {
	case *SemanticVersionConstraint:
		return []*SemanticVersionConstraint{t}
	case *MultiConstraint:
		var out []*SemanticVersionConstraint
		for _, sub := range t.constraints {
			out = append(out, SemverConstraintsOf(sub)...)
		}

		return out
	default:
		return nil
	}
}
