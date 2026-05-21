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
}

// anchorOpRegex captures version literals that follow an inclusive boundary
// operator. Exclusive bounds (>, <) and shorthand operators (^, ~) are
// intentionally excluded — see the anchors field doc above for the rationale.
//
// The version capture is deliberately permissive (any non-space, non-comma
// run); the result is re-validated through semver.NewVersion before being
// stored.
var anchorOpRegex = regexp.MustCompile(`(?:>=|<=|=>|=<)\s*([^\s,]+)`)

func NewSemanticVersionConstraint(c string) (*SemanticVersionConstraint, error) {
	constraint, err := semver.NewConstraint(c)
	if err != nil {
		return nil, fmt.Errorf("invalid semantic version constraint %q: %w", c, err)
	}

	anchors, err := extractInclusiveAnchors(c)
	if err != nil {
		return nil, fmt.Errorf("invalid semantic version constraint %q: %w", c, err)
	}

	return &SemanticVersionConstraint{
		constraint: constraint,
		anchors:    anchors,
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
