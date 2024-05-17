/*
Copyright 2024 Flant JSC

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

package contexts

import (
	"github.com/Masterminds/semver/v3"
)

// PullContext holds data related to pending mirroring-from-registry operation.
type PullContext struct {
	BaseContext

	DoGOSTDigests   bool  // --gost-digest
	SkipModulesPull bool  // --no-modules
	BundleChunkSize int64 // Plain bytes

	// Only one of those 2 is filled at a single time or none at all.
	MinVersion      *semver.Version // --min-version
	SpecificVersion *semver.Version // --release
}
