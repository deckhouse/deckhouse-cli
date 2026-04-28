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

// Access levels listed from least to most privileged. Used for validation,
// completion, and error messages — order is part of the user-facing output.
var namespacedAccessLevelsOrdered = []string{
	"User", "PrivilegedUser", "Editor", "Admin",
}

var clusterOnlyAccessLevelsOrdered = []string{
	"ClusterEditor", "ClusterAdmin", "SuperAdmin",
}

var allAccessLevelsOrdered = append(
	append([]string(nil), namespacedAccessLevelsOrdered...),
	clusterOnlyAccessLevelsOrdered...,
)

// accessLevelOrder maps level → privilege rank (higher = more privileged).
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
// and comparison. Typed string fields encode to plain JSON strings, so the
// on-disk annotation stays byte-compatible across releases. LabelMatch is
// omitempty so non-labels scopes hash exactly as before this field was added.
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
	// encoding/json sorts map keys, so LabelMatch is already deterministic.
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

// canonicalGrantInput is the flat value type shared by grant and revoke
// callers, so neither has to leak its full *Opts struct into shared helpers.
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
