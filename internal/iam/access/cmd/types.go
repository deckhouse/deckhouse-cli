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

package access

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

// namespacedAccessLevelsOrdered is the source of truth for access levels
// allowed at namespace scope, listed from least to most privileged.
var namespacedAccessLevelsOrdered = []string{
	"User", "PrivilegedUser", "Editor", "Admin",
}

// clusterOnlyAccessLevelsOrdered are the levels that only make sense at
// cluster scope. Combined with namespacedAccessLevelsOrdered they form the
// full set of access levels accepted by `d8 iam access grant`.
var clusterOnlyAccessLevelsOrdered = []string{
	"ClusterEditor", "ClusterAdmin", "SuperAdmin",
}

// allAccessLevelsOrdered is the full ordered list of access levels in
// least-to-most privileged order. Used for completion and error messages
// so user-facing output stays in a predictable order.
var allAccessLevelsOrdered = append(
	append([]string(nil), namespacedAccessLevelsOrdered...),
	clusterOnlyAccessLevelsOrdered...,
)

// accessLevelOrder defines the hierarchy: higher index = more privileged.
// Built from allAccessLevelsOrdered so a single rename keeps everything in
// sync.
var accessLevelOrder = func() map[string]int {
	m := make(map[string]int, len(allAccessLevelsOrdered))
	for i, l := range allAccessLevelsOrdered {
		m[l] = i
	}
	return m
}()

var namespacedAccessLevels = sliceToSet(namespacedAccessLevelsOrdered)
var allAccessLevels = sliceToSet(allAccessLevelsOrdered)

func sliceToSet(s []string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, v := range s {
		m[v] = true
	}
	return m
}

// canonicalGrantSpec is the canonical representation of a grant for hashing
// and comparison. The typed Model/SubjectKind/ScopeType fields encode-decode
// to plain JSON strings (Go's encoding/json treats `type X string` as a
// string), so on-disk annotations stay byte-compatible with previous releases.
//
// LabelMatch is omitempty so cluster/all-namespaces/namespace specs hash to
// the exact same JSON they did before this field was added (keeping
// d8-managed object names stable across upgrades). It is only populated for
// ScopeLabels.
type canonicalGrantSpec struct {
	Model            iamtypes.AccessModel `json:"model"`
	SubjectKind      iamtypes.SubjectKind `json:"subjectKind"`
	SubjectRef       string               `json:"subjectRef"`
	SubjectPrincipal string               `json:"subjectPrincipal"`
	AccessLevel      string               `json:"accessLevel"`
	ScopeType        iamtypes.Scope       `json:"scopeType"`
	Namespaces       []string             `json:"namespaces,omitempty"`
	LabelMatch       map[string]string    `json:"labelMatch,omitempty"`
	AllowScale       bool                 `json:"allowScale"`
	PortForwarding   bool                 `json:"portForwarding"`
}

func (c *canonicalGrantSpec) JSON() (string, error) {
	cpy := *c
	if cpy.Namespaces != nil {
		sorted := make([]string, len(cpy.Namespaces))
		copy(sorted, cpy.Namespaces)
		sort.Strings(sorted)
		cpy.Namespaces = sorted
	}
	// encoding/json sorts map keys alphabetically, so LabelMatch is already
	// emitted deterministically — no extra normalisation needed here.
	data, err := json.Marshal(&cpy)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// normalizedGrant is the internal representation of a grant from any source.
type normalizedGrant struct {
	SourceKind      string // AuthorizationRule or ClusterAuthorizationRule (object kind)
	SourceName      string
	SourceNamespace string // only for AuthorizationRule

	SubjectKind      iamtypes.SubjectKind
	SubjectPrincipal string // email for users, name for groups

	AccessLevel    string
	AllowScale     bool
	PortForwarding bool

	ScopeType       iamtypes.Scope
	ScopeNamespaces []string // for namespace scope

	ManagedByD8 bool
}

func validateAccessLevel(level string, namespaced bool) error {
	if namespaced {
		if !namespacedAccessLevels[level] {
			return fmt.Errorf("access level %q is not valid for namespaced scope; valid levels: %s",
				level, strings.Join(namespacedAccessLevelsOrdered, ", "))
		}
		return nil
	}
	if !allAccessLevels[level] {
		return fmt.Errorf("invalid access level %q; valid levels: %s",
			level, strings.Join(allAccessLevelsOrdered, ", "))
	}
	return nil
}

func maxAccessLevel(levels []string) string {
	best := ""
	bestOrder := -1
	for _, l := range levels {
		if ord, ok := accessLevelOrder[l]; ok && ord > bestOrder {
			best = l
			bestOrder = ord
		}
	}
	return best
}

// canonicalGrantInput captures the minimal subset of grant/revoke options
// needed to expand a user-facing intent into one canonicalGrantSpec per
// concrete object. It is intentionally a flat value type so callers don't
// have to leak grantOpts/revokeOpts into shared helpers.
type canonicalGrantInput struct {
	SubjectKind      iamtypes.SubjectKind
	SubjectRef       string
	SubjectPrincipal string
	AccessLevel      string
	ScopeType        iamtypes.Scope
	Namespaces       []string
	LabelMatch       map[string]string
	AllowScale       bool
	PortForwarding   bool
}
