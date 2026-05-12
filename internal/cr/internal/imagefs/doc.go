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

// Package imagefs reads the filesystem of an OCI/Docker image as the merged
// view that a running container would see, and extracts it to disk with
// path-traversal and absolute-symlink-rewrite protection.
//
// Whiteouts (.wh.* markers and .wh..wh..opq opaque markers) are honored:
// files explicitly deleted in an upper layer are invisible in the result.
//
// NOT a Go fs.FS implementation - the API surface is purpose-built for the
// `d8 cr fs` subcommands (MergedFS / ReadFile / ExtractMerged).
package imagefs
