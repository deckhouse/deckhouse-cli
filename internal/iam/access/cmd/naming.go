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
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/version"
)

// generateGrantName produces a deterministic name for a d8-managed grant object.
// Format: d8-access-<subjectKind>-<subjectRef>-<scope>-<level>-<hash8>
func generateGrantName(spec *canonicalGrantSpec) (string, error) {
	jsonStr, err := spec.JSON()
	if err != nil {
		return "", fmt.Errorf("canonical JSON: %w", err)
	}

	h := sha256.Sum256([]byte(jsonStr))
	hash8 := fmt.Sprintf("%x", h[:4])

	name := fmt.Sprintf("d8-access-%s-%s-%s-%s-%s",
		strings.ToLower(spec.SubjectKind),
		sanitizeNamePart(spec.SubjectRef),
		scopeNamePart(spec),
		strings.ToLower(spec.AccessLevel),
		hash8,
	)

	if len(name) > 253 {
		name = name[:253]
	}

	return name, nil
}

// grantLabels returns the standard labels for a d8-managed grant object.
func grantLabels(spec *canonicalGrantSpec) map[string]string {
	return map[string]string{
		managedByLabel:                     managedByValue,
		"deckhouse.io/access-model":        "current",
		"deckhouse.io/access-subject-kind": strings.ToLower(spec.SubjectKind),
		"deckhouse.io/access-scope":        spec.ScopeType,
	}
}

// grantAnnotations returns the standard annotations for a d8-managed grant object.
func grantAnnotations(spec *canonicalGrantSpec) (map[string]string, error) {
	jsonStr, err := spec.JSON()
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"deckhouse.io/access-subject-ref":        spec.SubjectRef,
		"deckhouse.io/access-subject-principal":  spec.SubjectPrincipal,
		"deckhouse.io/access-canonical-spec":     jsonStr,
		"deckhouse.io/access-created-by-version": version.Version,
	}, nil
}

func sanitizeNamePart(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "@", "-at-")
	s = strings.ReplaceAll(s, ".", "-")
	s = strings.ReplaceAll(s, "_", "-")
	if len(s) > 40 {
		s = s[:40]
	}
	s = strings.TrimRight(s, "-")
	return s
}

func scopeNamePart(spec *canonicalGrantSpec) string {
	switch spec.ScopeType {
	case "namespace":
		if len(spec.Namespaces) == 1 {
			return spec.Namespaces[0]
		}
		return "multi-ns"
	case "cluster":
		return "cluster"
	case "all-namespaces":
		return "all"
	default:
		return "unknown"
	}
}
