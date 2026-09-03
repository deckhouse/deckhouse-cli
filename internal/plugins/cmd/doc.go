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

// Package pluginscmd implements the `d8 dist plugins` command tree and the
// per-plugin wrapper command on top of the internal/plugins machinery.
//
//	d8 dist plugins list       list installed plugins
//	d8 dist plugins versions   list published versions of a plugin
//	d8 dist plugins contract   show a plugin's contract
//	d8 dist plugins install    install a plugin, or update one (--all: every installed)
//	d8 dist plugins remove     remove an installed plugin
//
// The subtree is mounted under `d8 dist` (internal/dist/cmd) and inherits the
// cluster access flags (kubeconfig/context, rpp-*) from it. File layout: one
// command per file, plugin.go holds the per-plugin wrapper (e.g. `d8 system`).
package pluginscmd
