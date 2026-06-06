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
)

const (
	fileArchive  = "archive.json"
	fileIndex    = "index.json"
	fileComplete = "COMPLETE"
)

const (
	dirIndexes = "indexes"
	dirObjects = "manifests/objects"
	dirData    = "data"
)

const (
	fileNodes    = "nodes.jsonl"
	fileObjects  = "objects.jsonl"
	fileProgress = "progress.jsonl"
	fileVolumes  = "volumes.jsonl"
)

const (
	blobShardLen  = 2
	blobPrefixLen = blobShardLen * 2
	blobSuffix    = ".json.gz"
	blobPrefix    = "o-"
)

func NodeID(kind, name string) string {
	return kind + "--" + name
}

func BlobPath(digest string) (string, error) {
	if len(digest) < blobPrefixLen {
		return "", fmt.Errorf("digest too short: %q", digest)
	}

	shard1 := digest[:blobShardLen]
	shard2 := digest[blobShardLen : blobShardLen*2]
	filename := blobPrefix + digest + blobSuffix

	return filepath.Join(dirObjects, shard1, shard2, filename), nil
}

func IsComplete(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, fileComplete))

	return err == nil
}
