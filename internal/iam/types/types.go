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

// Package types holds the constants and GVRs shared across the iam
// subpackages (access, group, user). Anything more interesting than a
// constant or a GVR belongs in the consumer package.
package types

import "k8s.io/apimachinery/pkg/runtime/schema"

// GVRs of the deckhouse IAM resources.
var (
	UserGVR = schema.GroupVersionResource{
		Group: "deckhouse.io", Version: "v1", Resource: "users",
	}
	GroupGVR = schema.GroupVersionResource{
		Group: "deckhouse.io", Version: "v1alpha1", Resource: "groups",
	}
	AuthorizationRuleGVR = schema.GroupVersionResource{
		Group: "deckhouse.io", Version: "v1alpha1", Resource: "authorizationrules",
	}
	ClusterAuthorizationRuleGVR = schema.GroupVersionResource{
		Group: "deckhouse.io", Version: "v1", Resource: "clusterauthorizationrules",
	}
	UserOperationGVR = schema.GroupVersionResource{
		Group: "deckhouse.io", Version: "v1", Resource: "useroperations",
	}
)

// SubjectKind identifies the principal type that appears in
// spec.subjects[].kind on AuthorizationRule / ClusterAuthorizationRule and in
// Group.spec.members[].kind. It is a typed string so that internal struct
// fields and switch statements get compile-time protection against typos like
// "user" (lowercase) or unrelated kinds. Conversions to/from string only
// happen at unstructured.Unstructured map boundaries, where the API server
// requires plain strings.
type SubjectKind string

// Subject kinds. Note these are also the apiVersion-less object kinds for
// User / Group / ServiceAccount when referenced as principals.
const (
	KindUser           SubjectKind = "User"
	KindGroup          SubjectKind = "Group"
	KindServiceAccount SubjectKind = "ServiceAccount"
)

// Object kinds for the rule resources themselves. Kept as untyped strings
// because they are only ever used as literal values in unstructured maps and
// for ref formatting; introducing a separate "RuleKind" type would not catch
// any realistic bug today.
const (
	KindAuthorizationRule        = "AuthorizationRule"
	KindClusterAuthorizationRule = "ClusterAuthorizationRule"
	KindUserOperation            = "UserOperation"
)

// API versions matching the GVRs above. Kept in sync explicitly because
// unstructured.Unstructured needs apiVersion strings literally.
//
// The previous APIVersionUserAuthn / APIVersionUserAuthz names suggested a
// per-module split that does not actually exist in the API group: every
// resource here is under the deckhouse.io group, the only difference is the
// stability tier (v1 vs v1alpha1). The names below reflect that reality.
const (
	APIVersionDeckhouseV1       = "deckhouse.io/v1"       // User, ClusterAuthorizationRule, UserOperation
	APIVersionDeckhouseV1Alpha1 = "deckhouse.io/v1alpha1" // Group, AuthorizationRule
)

// Scope identifies how a grant maps onto cluster topology. Typed because it
// drives the choice between AuthorizationRule and ClusterAuthorizationRule
// and a wrong value here silently produces objects of the wrong kind.
type Scope string

const (
	ScopeNamespace     Scope = "namespace"
	ScopeCluster       Scope = "cluster"
	ScopeAllNamespaces Scope = "all-namespaces"
)

// AccessModel is the internal authorization model identifier persisted on
// managed grants. There is only one model today; bumping it is a deliberate
// breaking change. Typed for the same reason as Scope.
type AccessModel string

const ModelCurrent AccessModel = "current"

// Labels and annotations stamped on grant objects created by `d8 iam access grant`.
const (
	LabelManagedBy         = "app.kubernetes.io/managed-by"
	ManagedByValueCLI      = "d8-cli"
	LabelAccessModel       = "deckhouse.io/access-model"
	LabelAccessSubjectKind = "deckhouse.io/access-subject-kind"
	LabelAccessScope       = "deckhouse.io/access-scope"

	AnnotationAccessSubjectRef       = "deckhouse.io/access-subject-ref"
	AnnotationAccessSubjectPrincipal = "deckhouse.io/access-subject-principal"
	AnnotationAccessCanonicalSpec    = "deckhouse.io/access-canonical-spec"
	AnnotationAccessCreatedByVersion = "deckhouse.io/access-created-by-version"
)
