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

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
)

// Filter maps module names to minimal versions of these modules to be pulled
type Filter struct {
	modules map[string]*semver.Version
	logger  contexts.Logger
}

func NewFilter(filterExpression string, logger contexts.Logger) (*Filter, error) {
	filter := &Filter{
		modules: make(map[string]*semver.Version),
		logger:  logger,
	}
	if filterExpression == "" {
		return filter, nil
	}

	filters := strings.Split(filterExpression, ";")
	for _, filterExpr := range filters {
		moduleName, moduleMinVersionString, validSplit := strings.Cut(strings.TrimSpace(filterExpr), "@")
		if !validSplit {
			logger.WarnF("Malformed filter %q is ignored: invalid filter syntax", filterExpr)
			continue
		}

		moduleName = strings.TrimSpace(moduleName)
		if moduleName == "" {
			return nil, fmt.Errorf("Malformed filter expression %q: empty module name", filterExpr)
		}
		if _, moduleRedeclared := filter.modules[moduleName]; moduleRedeclared {
			return nil, fmt.Errorf("Malformed filter expression: module %s is declared multiple times", moduleName)
		}

		moduleMinVersion, err := semver.NewVersion(strings.TrimSpace(moduleMinVersionString))
		if err != nil {
			return nil, fmt.Errorf("Malformed filter expression %q: %w", filterExpr, err)
		}

		filter.modules[moduleName] = moduleMinVersion
	}

	return filter, nil
}

func (f *Filter) MatchesFilter(mod *Module) bool {
	_, hasMinVersion := f.modules[mod.Name]
	if !hasMinVersion {
		return false
	}

	return true
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
			f.logger.DebugLn("Failed to parse module release tag as semver", tag, err.Error())
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
