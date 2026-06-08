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

// Package restore implements the d8 snapshot restore command.
//
// Architecture overview:
//
//	RestoreSource (interface)
//	  └── ArchiveSource   — reads from a local directory (d8 snapshot download output)
//	  └── ClusterSource   — (future) calls GET .../snapshots/{name}/manifests-with-data-restoration
//
//	RestorePlan            — ordered list of ManifestOps + VolumeOps
//	Applier                — applies manifests via Server-Side Apply + editor on conflict
//	VolumeRestorer         — creates DataImport, uploads data, waits for Completed
package restore

// RestoreSource is the interface that wraps the plan-building logic for a
// restore source. The current implementation is ArchiveSource (local archive).
// A future ClusterSource will call the manifests-with-data-restoration endpoint
// and return pre-transformed manifests (PVC → VRR) with empty VolumeOps.
type RestoreSource interface {
	// Build returns a RestorePlan filtered by opts.
	Build(opts Options) (*RestorePlan, error)
}

// ArchiveSource builds a RestorePlan from a local snapshot archive directory.
type ArchiveSource struct {
	// ArchiveDir is the path to the local archive directory.
	ArchiveDir string
}

// Build implements RestoreSource for a local archive.
func (s ArchiveSource) Build(opts Options) (*RestorePlan, error) {
	return Build(s.ArchiveDir, opts)
}
