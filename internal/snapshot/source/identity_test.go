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
	"testing"
)

func TestCanonicalSnapshotIdentity_DeterministicAndDiscriminating(t *testing.T) {
	a := SnapshotIdentity{
		APIVersion: "state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Namespace:  "ns",
		Name:       "root",
		UID:        "uid-1",
	}

	if CanonicalSnapshotIdentity(a) != CanonicalSnapshotIdentity(a) {
		t.Fatal("CanonicalSnapshotIdentity must be deterministic")
	}

	// Only the UID differs — the canonical key must still differ.
	b := a
	b.UID = "uid-2"
	if CanonicalSnapshotIdentity(a) == CanonicalSnapshotIdentity(b) {
		t.Error("nodes differing only by UID must produce distinct canonical keys")
	}

	// The NUL separator makes the key injective (and unusable as a path).
	if !strings.Contains(CanonicalSnapshotIdentity(a), "\x00") {
		t.Error("canonical key must use the NUL separator")
	}
}

func TestCanonicalSourceIdentity_Discriminating(t *testing.T) {
	a := SourceRefIdentity{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "ns", Name: "disk-0", UID: "uid-1"}
	b := a
	b.UID = "uid-2"

	if CanonicalSourceIdentity(a) == CanonicalSourceIdentity(b) {
		t.Error("sources differing only by UID must produce distinct canonical keys")
	}
	if !strings.Contains(CanonicalSourceIdentity(a), "\x00") {
		t.Error("canonical source key must use the NUL separator")
	}
}
