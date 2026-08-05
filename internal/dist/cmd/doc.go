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

// Package distcmd implements the `d8 dist` command tree - management of the
// d8 distribution: the deckhouse-cli binary itself and its plugins.
//
//	d8 dist status     summary: the d8 version, plugins, what is outdated
//	d8 dist check      report whether a newer deckhouse-cli is available
//	d8 dist update     update the deckhouse-cli binary
//	d8 dist use        switch the deckhouse-cli binary to a specific version
//	d8 dist versions   list deckhouse-cli versions published in the registry
//	d8 dist plugins    the plugins subtree (implemented in internal/plugins/cmd)
//
// The package holds only the command layer. The binary version machinery
// (store, updater) lives in internal/selfupdate; the plugins machinery in
// internal/plugins.
//
// File layout: dist.go assembles the tree, one command per file (check.go,
// status.go, update.go, use.go, versions.go), updater.go builds the shared
// Updater, ui.go holds the output palette.
package distcmd
