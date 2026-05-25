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

package modules

import (
	"context"
	"errors"
	"fmt"

	"github.com/Masterminds/semver/v3"
)

// ProbeChecker reports whether a specific semver tag is served by the
// registry. The boolean MUST be false (with a nil error) when the tag is
// definitively absent — this is the signal the probe uses to decide that a
// patch series has ended. A non-nil error indicates a real failure
// (network, auth, ...) and aborts the probe.
//
// Callers integrating with a registry client typically translate the
// client's "image not found" sentinel to (false, nil) and propagate all
// other errors.
type ProbeChecker func(ctx context.Context, version *semver.Version) (bool, error)

// ErrProbeNoLowerBound is returned when a constraint without a parseable
// lower bound is handed to ProbeAvailableVersions. Callers should
// surface this to the user as "your constraint needs an explicit
// version anchor (e.g. ^1.65.0, ~1.65.0, or >=1.65.0)" rather than
// silently scanning from v0.0.0.
var ErrProbeNoLowerBound = errors.New("constraint has no parseable lower-bound version literal — probing requires an explicit starting point")

// ProbeAvailableVersions enumerates registry tags by walking semver
// versions starting from the constraint's lower bound. It exists for
// proxy/caching registries that do NOT expose the registry catalog API
// but DO serve manifests for tags they cache.
//
// Walk rules (intentionally identical to the user-facing description):
//
//  1. From the lower-bound (M, m, p), increment patch one step at a time.
//     Each step is tested with `check` and (when present and matching the
//     constraint) appended to the result. The patch series ends as soon
//     as a step does not exist OR does not satisfy the constraint.
//  2. When the patch series ends, advance one step to (M, m+1, 0) and
//     retry. If that exists, resume rule 1 from there; if not, fall
//     through to rule 3.
//  3. When the new-minor step also fails, advance to (M+1, 0, 0). If
//     that exists, resume rule 1 from there; if not, terminate the probe.
//
// The probe never invents a tag — every appended version was confirmed
// by the registry. It also never widens past the constraint: a version
// that the constraint does not accept is treated the same as a missing
// tag for rule-1 purposes, and the rule-2 / rule-3 jump points are
// also constraint-gated so the probe can terminate cleanly inside a
// bounded range like ">=1.64 <=1.68".
//
// Context cancellation is honoured between every probe step so a
// proxy registry that hangs on a single HEAD request does not block
// shutdown indefinitely.
func ProbeAvailableVersions(
	ctx context.Context,
	constraint *SemanticVersionConstraint,
	check ProbeChecker,
) ([]*semver.Version, error) {
	if constraint == nil {
		return nil, errors.New("probe requires a non-nil constraint")
	}

	if check == nil {
		return nil, errors.New("probe requires a non-nil checker")
	}

	start := constraint.LowerBound()
	if start == nil {
		return nil, ErrProbeNoLowerBound
	}

	major, minor, patch := start.Major(), start.Minor(), start.Patch()
	found := make([]*semver.Version, 0)

	// probeOne tests (major, minor, patch) against constraint+registry and,
	// on success, appends the version and advances patch by 1. The boolean
	// return ("advanced") tells the outer loop whether to keep walking
	// patches or to fall through to the minor/major lookahead.
	probeOne := func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		v := semver.MustParse(fmt.Sprintf("%d.%d.%d", major, minor, patch))
		if !constraint.Match(v) {
			return false, nil
		}

		exists, err := check(ctx, v)
		if err != nil {
			return false, err
		}

		if !exists {
			return false, nil
		}

		found = append(found, v)
		patch++

		return true, nil
	}

	for {
		// Rule 1: walk patches until one fails (or context is cancelled).
		for {
			advanced, err := probeOne()
			if err != nil {
				return nil, err
			}

			if !advanced {
				break
			}
		}

		// Rule 2: try the next minor with patch reset to 0. If it lands
		// on an existing+matching version, we resume rule 1 from the
		// patch right after it.
		minor++
		patch = 0

		advanced, err := probeOne()
		if err != nil {
			return nil, err
		}

		if advanced {
			continue
		}

		// Rule 3: rule 2 didn't pan out. Try the next major.
		major++
		minor = 0
		patch = 0

		advanced, err = probeOne()
		if err != nil {
			return nil, err
		}

		if advanced {
			continue
		}

		// Both lookaheads failed and we've already drained the patch
		// loop above — terminate.
		break
	}

	return found, nil
}
