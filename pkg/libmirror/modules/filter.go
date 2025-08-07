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
	"strings"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

type FilterType int

const (
	FilterTypeWhitelist FilterType = iota
	FilterTypeBlacklist
)

var validChannels = map[string]struct{}{
	"alpha": {}, "beta": {}, "early-access": {}, "stable": {}, "rock-solid": {},
}

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
	// exac match, console@=1.38.1 = registry.deckhouse.io/deckhouse/ce/modules/console:v1.38.1
	tag, ch, _ := strings.Cut(body, "+")
	if tag == "" {
		return nil, fmt.Errorf("empty tag in %q", body)
	}
	if ch != "" {
		if _, ok := validChannels[ch]; ok {
			return NewExactTagConstraintWithChannel(tag, ch), nil
		}
	}
	return NewExactTagConstraint(tag), nil
}


func parseSemver(v string) (VersionConstraint, error) {
	// semver match, console@1.38.1 = registry.deckhouse.io/deckhouse/ce/modules/console:v1.38.5
	c, err := NewSemanticVersionConstraint(v)
	if err != nil {
		return nil, fmt.Errorf("invalid semver %q: %w", v, err)
	}
	return c, nil
}

func (f *Filter) ShouldMirrorReleaseChannels(moduleName string) bool {
    if c, ok := f.modules[moduleName]; ok && c.IsExact() {
        return false
    }
    return true
}

func (f *Filter) VersionsToMirror(mod *Module) []string {
	c, ok := f.modules[mod.Name]
	if !ok {
		return nil
	}

	if c.IsExact() {
		if e, ok := c.(*ExactTagConstraint); ok {
			return []string{e.Tag()}
		}
		return nil
	}

	sc, ok := c.(*SemanticVersionConstraint)
	if !ok {
		return nil
	}
	var tags []string
	for _, v := range mod.Versions() {
		if sc.Match(v) {
			tags = append(tags, "v"+v.String())
		}
	}
	return tags
}
