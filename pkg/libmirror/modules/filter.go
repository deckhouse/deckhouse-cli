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

	"github.com/Masterminds/semver/v3"

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
	modules map[string]*semver.Version
	logger  params.Logger
}

func NewFilter(filterExpressions []string, filterType FilterType) (*Filter, error) {
	if filterType != FilterTypeWhitelist && filterType != FilterTypeBlacklist {
		return nil, fmt.Errorf("unknown filter type: %v", filterType)
	}

	filter := &Filter{
		_type:   filterType,
		modules: make(map[string]*semver.Version),
	}
	if len(filterExpressions) == 0 {
		// Empty filter matches any module
		filter._type = FilterTypeBlacklist
		return filter, nil
	}

	for _, filterExpr := range filterExpressions {
		moduleName, moduleMinVersionString, validSplit := strings.Cut(strings.TrimSpace(filterExpr), "@")
		moduleName = strings.TrimSpace(moduleName)
		if moduleName == "" {
			return nil, fmt.Errorf("Malformed filter expression %q: empty module name", filterExpr)
		}
		if _, moduleRedeclared := filter.modules[moduleName]; moduleRedeclared {
			return nil, fmt.Errorf("Malformed filter expression: module %s is declared multiple times", moduleName)
		}
		if !validSplit {
			filter.modules[moduleName] = semver.New(0, 0, 0, "", "")
			continue
		}

		moduleMinVersion, err := semver.NewVersion(strings.TrimSpace(moduleMinVersionString))
		if err != nil {
			return nil, fmt.Errorf("Malformed filter expression %q: %w", filterExpr, err)
		}

		filter.modules[moduleName] = moduleMinVersion
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

func (f *Filter) GetMinimalVersion(moduleName string) (*semver.Version, bool) {
	v, found := f.modules[moduleName]
	return v, found
}

func (f *Filter) FilterReleases(mod *Module) {
	moduleMinVersion, hasMinVersion := f.modules[mod.Name]
	if !hasMinVersion {
		return
	}

	filteredReleases := make([]string, 0)
	for _, tag := range mod.Releases {
		v, err := semver.NewVersion(tag)
		if err != nil {
			if f.logger != nil {
				f.logger.DebugLn("Failed to parse module release tag as semver", tag, err.Error())
			}
			filteredReleases = append(filteredReleases, tag) // This is probably a release channel, so just leave it
			continue
		}

		if moduleMinVersion.GreaterThan(v) {
			continue
		}

		filteredReleases = append(filteredReleases, tag)
	}

	mod.Releases = filteredReleases
}
