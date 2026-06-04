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

package mirror

import "time"

// ComponentStats is the per-category image accounting captured after a pull
// phase completes. The image count is "planned" in dry-run (download-list
// lengths) and "actual" in a real pull (OCI layout manifest counts).
type ComponentStats struct {
	// Skipped is true when the category was disabled via a Skip* option.
	Skipped bool
	// Attempted is true when the phase ran, even if it produced zero images.
	Attempted bool
	// Images is the number of image manifests (planned or actual).
	Images int
	// Versions are the resolved release versions that will be (or were) pulled,
	// e.g. ["v1.69.0"] or ["v1.71.7", "v1.72.3"]. Populated for the platform;
	// available in dry-run too, since version selection happens before download.
	Versions []string
	// Channels is the set of release channels mapped to Versions (platform only).
	Channels []string
}

// SecurityStats specializes ComponentStats for the trivy security databases.
type SecurityStats struct {
	// Skipped is true when --no-security-db was set.
	Skipped bool
	// Attempted is true when the security phase ran.
	Attempted bool
	// Available is false for editions without security databases (CE/BE/SE),
	// where securityDatabasesAvailable() returned false.
	Available bool
	// Databases is the number of databases pulled (real pull) or enqueued
	// (dry-run). At most AvailableDatabases.
	Databases int
	// AvailableDatabases is the size of the security database catalogue
	// (trivy-db, trivy-bdu, trivy-java-db, trivy-checks).
	AvailableDatabases int
}

// ModuleStat is one module's contribution to the pull.
type ModuleStat struct {
	Name   string
	Images int
	// VEX is how many of Images are VEX attestations (a subset of Images).
	VEX int
	// Versions are the resolved module versions that will be (or were) pulled,
	// e.g. ["v1.10.3", "v1.9.16"]. Available in dry-run too.
	Versions []string
}

// ModulesStats aggregates per-module image accounting.
type ModulesStats struct {
	// Skipped is true when modules were disabled and OnlyExtraImages is off.
	Skipped bool
	// Attempted is true when the modules phase ran.
	Attempted bool
	// OnlyExtraImages reflects the --only-extra-images mode.
	OnlyExtraImages bool
	// Modules holds the per-module breakdown, sorted by name.
	Modules []ModuleStat
	// TotalImages is the sum of images across all modules.
	TotalImages int
	// TotalVEX is the number of VEX attestations across all modules, a subset of
	// TotalImages.
	TotalVEX int
}

// PackageStat is one package's contribution to the pull.
type PackageStat struct {
	Name   string
	Images int
	// VEX is how many of Images are VEX attestations (a subset of Images).
	VEX int
	// Versions are the resolved package versions that will be (or were) pulled,
	// e.g. ["v1.45.2", "v1.44.0"]. Available in dry-run too.
	Versions []string
}

// PackagesStats aggregates per-package image accounting.
type PackagesStats struct {
	// Skipped is true when packages were disabled and OnlyExtraImages is off.
	Skipped bool
	// Attempted is true when the packages phase ran.
	Attempted bool
	// OnlyExtraImages reflects the --only-extra-images mode.
	OnlyExtraImages bool
	// Packages holds the per-package breakdown, sorted by name.
	Packages []PackageStat
	// TotalImages is the sum of images across all packages.
	TotalImages int
	// TotalVEX is the number of VEX attestations across all packages, a subset of
	// TotalImages.
	TotalVEX int
}

// BundleFile is one logical bundle artifact (platform.tar, installer.tar,
// security.tar, module-<name>.tar), possibly spread over .NNNN.chunk files.
type BundleFile struct {
	// Name is the logical artifact name, e.g. "module-foo.tar".
	Name string
	// Bytes is the total size across all chunks of the artifact.
	Bytes int64
	// Chunks is the number of .chunk files (0 for a single .tar artifact).
	Chunks int
}

// BundleStats is the on-disk artifact accounting collected after packing.
type BundleStats struct {
	Files      []BundleFile
	TotalBytes int64
}

// PullSummary is the complete end-of-pull accounting handed to the renderer.
type PullSummary struct {
	// DryRun reports whether this was a planning run with no downloads.
	DryRun bool
	// Cancelled marks a graceful interrupt (Ctrl+C); the summary reflects what
	// completed before it.
	Cancelled bool
	// Failed marks a hard-error abort (e.g. retries exhausted, checksum failure).
	// The summary still renders, in a FAILED state. A phase that never ran has a
	// zero-valued stat and renders "not pulled". Mutually exclusive with Cancelled.
	Failed bool
	// Edition is the source edition (e.g. "ce", "ee"), parsed from the source
	// registry path. Empty for a custom registry with no edition segment, in
	// which case the summary omits the Edition line.
	Edition string
	// Elapsed is the wall-clock duration of the pull, filled by the CLI.
	Elapsed time.Duration

	Platform  ComponentStats
	Installer ComponentStats
	Security  SecurityStats
	Modules   ModulesStats
	Packages  PackagesStats

	// Bundle is populated by the CLI from the bundle directory (real pull only).
	Bundle BundleStats
}
