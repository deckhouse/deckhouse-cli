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

package archive

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Archive root files.
const (
	fileArchive  = "archive.json"
	fileIndex    = "index.json"
	fileComplete = "COMPLETE"
)

// Archive directories.
const (
	dirIndexes = "indexes"
	dirObjects = "manifests/objects"
	dirData    = "data"
)

// JSONL index files under dirIndexes.
const (
	fileNodes   = "nodes.jsonl"
	fileObjects = "objects.jsonl"
)

// Blob naming: two-level sharding by digest prefix.
const (
	// blobShardLen is the number of hex chars per shard directory level.
	blobShardLen = 2
	// blobPrefixLen is the minimum digest length required for two shard levels.
	blobPrefixLen = blobShardLen * 2
	blobSuffix    = ".json.gz"
	blobPrefix    = "o-"
)

// Aggregated subresource API base path.
const apiBase = "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1"

// NodeID builds a stable identifier for a snapshot tree node.
// It combines kind and name with a "--" separator.
func NodeID(kind, name string) string {
	return kind + "--" + name
}

// BlobPath returns the relative path of the manifest blob for the given sha256 digest hex string.
// Blobs are sharded by two two-char hex prefixes to keep directory sizes manageable:
// manifests/objects/<aa>/<bb>/o-<fullDigest>.json.gz
func BlobPath(digest string) (string, error) {
	if len(digest) < blobPrefixLen {
		return "", fmt.Errorf("digest too short: %q", digest)
	}

	shard1 := digest[:blobShardLen]
	shard2 := digest[blobShardLen : blobShardLen*2]
	filename := blobPrefix + digest + blobSuffix

	return filepath.Join(dirObjects, shard1, shard2, filename), nil
}

// AggregatedPath returns the absolute URL path to the manifests subresource for one snapshot node.
func AggregatedPath(namespace, resource, name string) string {
	return strings.Join([]string{
		apiBase,
		"namespaces", namespace,
		resource, name,
		"manifests",
	}, "/")
}

// IsComplete reports whether the COMPLETE sentinel file is present in dir.
func IsComplete(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, fileComplete))

	return err == nil
}
