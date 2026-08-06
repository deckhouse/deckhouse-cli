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

package source

import (
	"strings"

	"k8s.io/apimachinery/pkg/types"
)

// SnapshotIdentity is the structural identity of a snapshot CR node itself
// (apiVersion/kind/namespace/name/uid). Every node has one, unlike SourceRefIdentity (the
// captured source object), which may be absent (root, manifest-only). It is the node's resume
// identity and the input to the archive collision discriminator. The readable archive
// directory name itself is NOT derived from it: layout stays source-name based via
// archive.NodeDirName (readable base = captured source name, fallback CR name); only the
// collision suffix uses a short hash of CanonicalSnapshotIdentity so two nodes sharing a
// source-name base never mix.
type SnapshotIdentity struct {
	APIVersion string
	Kind       string
	Namespace  string
	Name       string
	UID        types.UID
}

// canonicalSep joins identity components. NUL cannot appear in any Kubernetes identifier, so
// it is an injective separator; this also makes the result unusable as a filesystem path (by
// design — canonical keys are for comparison/resume/hashing, never directory names).
const canonicalSep = "\x00"

// CanonicalSnapshotIdentity returns an opaque, deterministic key for a snapshot node, used for
// resume matching and as the input to the archive collision discriminator (a short hash of it
// disambiguates two nodes that share a readable source-name directory base). It is NOT a
// filesystem path.
func CanonicalSnapshotIdentity(id SnapshotIdentity) string {
	return strings.Join([]string{id.APIVersion, id.Kind, id.Namespace, id.Name, string(id.UID)}, canonicalSep)
}

// CanonicalSourceIdentity returns an opaque, deterministic key for the captured source object
// (provenance). Same NUL-joined form as CanonicalSnapshotIdentity; not a path.
func CanonicalSourceIdentity(id SourceRefIdentity) string {
	return strings.Join([]string{id.APIVersion, id.Kind, id.Namespace, id.Name, id.UID}, canonicalSep)
}
