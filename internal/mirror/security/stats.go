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

package security

import (
	"github.com/google/go-containerregistry/pkg/v1/layout"

	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// securityDatabaseCount is the size of the security database catalogue:
// trivy-db, trivy-bdu, trivy-java-db, trivy-checks.
const securityDatabaseCount = 4

// SecurityStats is the security phase's accounting, mapped into the top-level
// summary by the pull orchestrator.
type SecurityStats struct {
	Attempted          bool
	Available          bool
	Databases          int
	AvailableDatabases int
}

// Stats returns accounting for the security phase. It reports whether security
// databases are available in the source edition; for available editions it
// reports planned counts in dry-run (enqueued database sets) and actual counts
// in a real pull (databases whose layout received at least one manifest,
// captured before packing in Service.pulledDatabases).
func (svc *Service) Stats() SecurityStats {
	stats := SecurityStats{
		Attempted:          true,
		Available:          svc.available,
		AvailableDatabases: securityDatabaseCount,
	}

	if !svc.available {
		return stats
	}

	if svc.options.DryRun {
		stats.Databases = len(svc.downloadList.Security)

		return stats
	}

	stats.Databases = svc.pulledDatabases

	return stats
}

// countPulledDatabases returns how many security database layouts received at
// least one image manifest. It must run before packing deletes the layout
// files (see bundle.Pack).
func (svc *Service) countPulledDatabases() int {
	count := 0

	for _, l := range svc.layout.Security {
		if l == nil {
			continue
		}

		if regimage.CountManifests([]layout.Path{l.Path()}) > 0 {
			count++
		}
	}

	return count
}
