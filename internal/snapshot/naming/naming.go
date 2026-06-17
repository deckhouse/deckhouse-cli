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

// Package naming provides deterministic name derivation helpers shared between
// the snapshot tree builder and the data-plane exporter. Keeping the formulae
// here prevents an import cycle between internal/snapshot/source (tree) and
// internal/snapshot/exporter (data-plane).
package naming

import (
	"crypto/sha256"
	"encoding/hex"
)

// ShadowName returns a deterministic Kubernetes-safe name for the shadow pair
// (both the cluster-scoped shadow VolumeSnapshotContent and the namespaced
// shadow VolumeSnapshot share this name). The name is derived from the
// artifact VolumeSnapshotContent name so that resume across restarts is
// idempotent: the same artifact always maps to the same shadow pair name.
//
// The result is always exactly 22 characters ("d8-ss-" prefix + 16 hex
// digits encoding 8 bytes of sha256), well within the 63-char DNS label limit.
func ShadowName(artifactName string) string {
	h := sha256.Sum256([]byte(artifactName))

	return "d8-ss-" + hex.EncodeToString(h[:8])
}
