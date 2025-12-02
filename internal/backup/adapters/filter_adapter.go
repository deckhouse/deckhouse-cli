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

package adapters

import (
	"github.com/deckhouse/deckhouse-cli/internal/backup/domain"
	"github.com/deckhouse/deckhouse-cli/internal/backup/usecase"
)

// ResourceFilterAdapter adapts infrastructure filters to usecase.ResourceFilter
type ResourceFilterAdapter struct {
	filter func(obj domain.K8sObject) bool
}

// NewResourceFilterAdapter creates a new filter adapter from a function
func NewResourceFilterAdapter(filter func(obj domain.K8sObject) bool) *ResourceFilterAdapter {
	return &ResourceFilterAdapter{filter: filter}
}

// NewResourceFilterFromWhitelist creates a filter adapter from the built-in whitelist
func NewResourceFilterFromWhitelist() *ResourceFilterAdapter {
	return &ResourceFilterAdapter{
		filter: func(obj domain.K8sObject) bool {
			// The whitelist filter uses domain.K8sObject interface
			// which provides all necessary information for filtering
			return defaultWhitelistFilter(obj)
		},
	}
}

// Matches implements usecase.ResourceFilter
func (a *ResourceFilterAdapter) Matches(obj domain.K8sObject) bool {
	if a.filter == nil {
		return true
	}
	return a.filter(obj)
}

// defaultWhitelistFilter is a placeholder that can be replaced with actual whitelist logic
// The actual implementation should be injected via constructor
func defaultWhitelistFilter(obj domain.K8sObject) bool {
	// Default: allow all objects
	// Actual filtering logic should be provided by the caller
	return true
}

// Compile-time check
var _ usecase.ResourceFilter = (*ResourceFilterAdapter)(nil)

