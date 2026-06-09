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

// Package selfupdate lets the d8 binary update itself through the cluster
// (registry-packages-proxy, kubeconfig identity - no registry credentials).
//
// It implements the `d8 cli` command tree:
//
//   - check    - is a newer version available
//   - versions - list published versions (alias: list)
//   - update   - install a version and switch to it
//   - use      - switch to a version (instant if already installed)
//
// How it works, in short:
//
//   - Versions are kept in a per-user store (~/.deckhouse-cli/cli/versions/<tag>/d8);
//     the active one is selected by the `current` symlink, so switching is an
//     atomic repoint - no file copying, no sudo.
//   - Every downloaded binary is smoke-tested (`--version`) before it becomes
//     active; a corrupt or wrong-platform artifact never replaces a working d8.
//   - After ordinary commands d8 prints a one-line notice when a newer version
//     is cached; the cache is refreshed by a detached background child at most
//     once per TTL. D8_DISABLE_UPDATE_NOTIFY=1 turns all of it off.
//
// Wiring: the cobra commands live in the cmd subpackage; this package holds
// the update flow (update.go), the store (store.go) and the notice (notify.go).
// Downloads go through the Source interface (source.go) backed by internal/rpp.
// Details, trade-offs, and the full file map are in README.md next to this file.
package selfupdate
