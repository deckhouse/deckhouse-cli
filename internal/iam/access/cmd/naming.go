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
	"sort"
	"strings"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
	"github.com/deckhouse/deckhouse-cli/internal/version"
)

// k8sNameMaxLen is the DNS-subdomain limit for a Kubernetes object name.
const k8sNameMaxLen = 253

// generateGrantName produces a deterministic name for a d8-managed grant object.
// Format: d8-access-<subjectKind>-<subjectRef>-<scope>-<level>-<hash8>
//
// The trailing hash8 is what guarantees uniqueness across different canonical
// specs that happen to share the same human-readable prefix (long emails,
// long namespace names, etc). It must survive truncation, so we trim the
// readable body and re-append the hash, never the other way around.
func generateGrantName(spec *canonicalGrantSpec) (string, error) {
	jsonStr, err := spec.JSON()
	if err != nil {
		return "", fmt.Errorf("canonical JSON: %w", err)
	}

	h := sha256.Sum256([]byte(jsonStr))
	hash8 := fmt.Sprintf("%x", h[:4])

	body := fmt.Sprintf("d8-access-%s-%s-%s-%s",
		strings.ToLower(string(spec.SubjectKind)),
		sanitizeNamePart(spec.SubjectRef),
		scopeNamePart(spec),
		strings.ToLower(spec.AccessLevel),
	)

	// Reserve room for "-<hash8>" so the unique suffix is never truncated.
	maxBody := k8sNameMaxLen - 1 - len(hash8)
	if len(body) > maxBody {
		body = strings.TrimRight(body[:maxBody], "-")
	}

	return body + "-" + hash8, nil
}

// grantLabels returns the standard labels for a d8-managed grant object.
// Typed enum values are converted to plain strings here because Kubernetes
// label values are strings.
func grantLabels(spec *canonicalGrantSpec) map[string]string {
	return map[string]string{
		iamtypes.LabelManagedBy:         iamtypes.ManagedByValueCLI,
		iamtypes.LabelAccessModel:       string(iamtypes.ModelCurrent),
		iamtypes.LabelAccessSubjectKind: strings.ToLower(string(spec.SubjectKind)),
		iamtypes.LabelAccessScope:       string(spec.ScopeType),
	}
}

// grantAnnotations returns the standard annotations for a d8-managed grant object.
func grantAnnotations(spec *canonicalGrantSpec) (map[string]string, error) {
	jsonStr, err := spec.JSON()
	if err != nil {
		return nil, err
	}
	return map[string]string{
		iamtypes.AnnotationAccessSubjectRef:       spec.SubjectRef,
		iamtypes.AnnotationAccessSubjectPrincipal: spec.SubjectPrincipal,
		iamtypes.AnnotationAccessCanonicalSpec:    jsonStr,
		iamtypes.AnnotationAccessCreatedByVersion: version.Version,
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
	case iamtypes.ScopeNamespace:
		if len(spec.Namespaces) == 1 {
			return spec.Namespaces[0]
		}
		return "multi-ns"
	case iamtypes.ScopeCluster:
		return "cluster"
	case iamtypes.ScopeAllNamespaces:
		return "all"
	case iamtypes.ScopeLabels:
		// Stable middle segment so the object name self-documents as
		// labels-scoped. Disambiguation against other label sets comes from
		// the trailing hash8 of the full canonical spec.
		keys := make([]string, 0, len(spec.LabelMatch))
		for k := range spec.LabelMatch {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			b.WriteString(k)
			b.WriteByte('=')
			b.WriteString(spec.LabelMatch[k])
			b.WriteByte(',')
		}
		h := sha256.Sum256([]byte(b.String()))
		return fmt.Sprintf("labels-%x", h[:3])
	default:
		return "unknown"
	}
}
