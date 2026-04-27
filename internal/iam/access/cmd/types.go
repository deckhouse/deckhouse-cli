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

	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	userGVR = schema.GroupVersionResource{
		Group: "deckhouse.io", Version: "v1", Resource: "users",
	}
	groupGVR = schema.GroupVersionResource{
		Group: "deckhouse.io", Version: "v1alpha1", Resource: "groups",
	}
	authorizationRuleGVR = schema.GroupVersionResource{
		Group: "deckhouse.io", Version: "v1alpha1", Resource: "authorizationrules",
	}
	clusterAuthorizationRuleGVR = schema.GroupVersionResource{
		Group: "deckhouse.io", Version: "v1", Resource: "clusterauthorizationrules",
	}
)

// accessLevelOrder defines the hierarchy: higher index = more privileged.
var accessLevelOrder = map[string]int{
	"User":           0,
	"PrivilegedUser": 1,
	"Editor":         2,
	"Admin":          3,
	"ClusterEditor":  4,
	"ClusterAdmin":   5,
	"SuperAdmin":     6,
}

var namespacedAccessLevels = map[string]bool{
	"User": true, "PrivilegedUser": true, "Editor": true, "Admin": true,
}

var allAccessLevels = map[string]bool{
	"User": true, "PrivilegedUser": true, "Editor": true, "Admin": true,
	"ClusterEditor": true, "ClusterAdmin": true, "SuperAdmin": true,
}

const managedByLabel = "app.kubernetes.io/managed-by"
const managedByValue = "d8-cli"

// canonicalGrantSpec is the canonical representation of a grant for hashing and comparison.
type canonicalGrantSpec struct {
	Model            string   `json:"model"`
	SubjectKind      string   `json:"subjectKind"`
	SubjectRef       string   `json:"subjectRef"`
	SubjectPrincipal string   `json:"subjectPrincipal"`
	AccessLevel      string   `json:"accessLevel"`
	ScopeType        string   `json:"scopeType"`
	Namespaces       []string `json:"namespaces,omitempty"`
	AllowScale       bool     `json:"allowScale"`
	PortForwarding   bool     `json:"portForwarding"`
}

func (c *canonicalGrantSpec) JSON() (string, error) {
	cpy := *c
	if cpy.Namespaces != nil {
		sorted := make([]string, len(cpy.Namespaces))
		copy(sorted, cpy.Namespaces)
		sort.Strings(sorted)
		cpy.Namespaces = sorted
	}
	data, err := json.Marshal(&cpy)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// normalizedGrant is the internal representation of a grant from any source.
type normalizedGrant struct {
	SourceKind      string // AuthorizationRule or ClusterAuthorizationRule
	SourceName      string
	SourceNamespace string // only for AuthorizationRule

	SubjectKind      string // User or Group
	SubjectPrincipal string // email for users, name for groups

	AccessLevel    string
	AllowScale     bool
	PortForwarding bool

	ScopeType       string   // namespace, cluster, all-namespaces
	ScopeNamespaces []string // for namespace scope

	ManagedByD8 bool
}

func validateAccessLevel(level string, namespaced bool) error {
	if namespaced {
		if !namespacedAccessLevels[level] {
			valid := []string{"User", "PrivilegedUser", "Editor", "Admin"}
			return fmt.Errorf("access level %q is not valid for namespaced scope; valid levels: %s", level, strings.Join(valid, ", "))
		}
	} else {
		if !allAccessLevels[level] {
			valid := []string{"User", "PrivilegedUser", "Editor", "Admin", "ClusterEditor", "ClusterAdmin", "SuperAdmin"}
			return fmt.Errorf("invalid access level %q; valid levels: %s", level, strings.Join(valid, ", "))
		}
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
