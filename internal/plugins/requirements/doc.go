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

// Package requirements answers one question: does THIS cluster satisfy a
// plugin's cluster-side requirements (Kubernetes, Deckhouse and module
// versions)?
//
// It has two halves:
//
//   - ClusterState / LoadClusterState - a one-shot snapshot of the cluster
//     facts the checks need;
//   - Checker.Checks - the ordered, named validators that enforce a plugin
//     contract against a snapshot.
//
// A check failure is either a genuinely unmet requirement (IsUnmet reports
// true; version selection may then try an older version) or an operational
// error (malformed constraint, unreadable cluster fact) that must propagate.
//
// The Manager-side concerns stay in internal/plugins: building the Kubernetes
// clients from flags, caching the snapshot per command run, the
// --skip-cluster-checks escape hatch and its error wording.
package requirements
