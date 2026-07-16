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
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/types"
)

// SnapshotIdentity is the structural identity of a snapshot CR node itself
// (apiVersion/kind/namespace/name/uid). Every node has one, unlike SourceRefIdentity (the
// captured source object), which may be absent (root, manifest-only). It is the basis for the
// resume key and the archive directory name.
type SnapshotIdentity struct {
	APIVersion string
	Kind       string
	Namespace  string
	Name       string
	UID        types.UID
}

// canonicalSep joins identity components. NUL cannot appear in any Kubernetes identifier, so
// it is an injective separator (no component can contain it), which is exactly why the result
// is unusable as a filesystem path — see ArchiveNodeDirName for names.
const canonicalSep = "\x00"

// CanonicalSnapshotIdentity returns an opaque, deterministic key for a snapshot node, used for
// comparison, checksums and the resume index. It is NOT a filesystem path: the NUL separator
// is illegal in a path component and apiVersion contains '/'. Use ArchiveNodeDirName for
// directory names.
func CanonicalSnapshotIdentity(id SnapshotIdentity) string {
	return strings.Join([]string{id.APIVersion, id.Kind, id.Namespace, id.Name, string(id.UID)}, canonicalSep)
}

// CanonicalSourceIdentity returns an opaque, deterministic key for the captured source object
// (provenance). Same NUL-joined form as CanonicalSnapshotIdentity; not a path.
func CanonicalSourceIdentity(id SourceRefIdentity) string {
	return strings.Join([]string{id.APIVersion, id.Kind, id.Namespace, id.Name, id.UID}, canonicalSep)
}

// dirNameHashLen is the length of the short hex discriminator appended to a directory name.
const dirNameHashLen = 8

// pathUnsafe matches any run of characters not allowed in a portable, readable path component.
var pathUnsafe = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// ArchiveNodeDirName returns a path-safe, human-readable, deterministic directory name for a
// snapshot node. The readable base is "<kind>-<name>" (sanitized); a short hash of the
// canonical identity is appended so distinct nodes whose bases sanitize to the same string
// stay unique. The UID feeds the hash, never replacing the readable base. The result never
// contains NUL or '/'.
func ArchiveNodeDirName(id SnapshotIdentity) string {
	sum := sha256.Sum256([]byte(CanonicalSnapshotIdentity(id)))
	short := hex.EncodeToString(sum[:])[:dirNameHashLen]

	base := sanitizePathComponent(strings.ToLower(id.Kind) + "-" + id.Name)
	if base == "" {
		return short
	}

	return base + "-" + short
}

// sanitizePathComponent replaces runs of unsafe characters with a single '-' and trims
// leading/trailing separators so the result is a valid, readable path component (or "").
func sanitizePathComponent(s string) string {
	return strings.Trim(pathUnsafe.ReplaceAllString(s, "-"), "-.")
}
