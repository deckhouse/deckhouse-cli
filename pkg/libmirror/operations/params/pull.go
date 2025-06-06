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

package params

import (
	"github.com/Masterminds/semver/v3"
)

// PullParams holds data related to pending mirroring-from-registry operation.
type PullParams struct {
	BaseParams

	DoGOSTDigests         bool  // --gost-digest
	SkipPlatform          bool  // --no-platform
	SkipSecurityDatabases bool  // --no-security-db
	SkipModules           bool  // --no-modules
	BundleChunkSize       int64 // Plain bytes

	// Only one of those 2 is filled at a single time or none at all.
	SinceVersion *semver.Version // --since-version
	DeckhouseTag string          // --deckhouse-tag
}
