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

// Package dist mirrors the d8 CLI distribution into the images bundle: the
// binary itself and the plugins that extend it.
//
// Everything here lives under one registry root, OUTSIDE the edition segment
// (the same asymmetry as the installer):
//
//	<root>/deckhouse-cli:<vX.Y.Z>                 - the d8 binary (multi-platform OCI index)
//	<root>/deckhouse-cli/plugins                  - plugin catalog; its tags are plugin names
//	<root>/deckhouse-cli/plugins/<name>:<vX.Y.Z>  - one plugin version (multi-platform OCI index)
//
// This is the tree the in-cluster registry-packages-proxy serves, so an
// air-gapped bundle that carries it needs no extra setup on the target side:
// `d8 dist update` and `d8 plugins install <name>` work as they do online.
//
// The binary is mirrored at one version - the newest published stable one, or
// the tag pinned with --deckhouse-cli-tag.
//
// Plugins are selected, not enumerated. A plugin declares its requirements
// (Deckhouse modules, other plugins, platform versions) in a contract: a
// base64-JSON annotation on the image manifest. Reading a contract is a single
// manifest fetch, so deciding WHAT to mirror needs no layer downloads.
//
// Selection principle: nothing extra, with one standing exception. A plugin
// enters the bundle when a mirrored module needs it (its contract names that
// module), when another selected plugin requires it, or when the user asks for
// it explicitly with --include-plugin.
//
// The exception is PlatformPlugins: they ship with the platform rather than
// with any module, so mirroring the platform mirrors them too, unconditionally.
// A bundle without them can install the platform but not operate it.
//
// A dependency whose name matches a built-in d8 command is mirrored when it is
// published and falls back to the built-in when it is not, so it can never block
// the bundle - see ResolveInput.Builtins.
//
// Release metadata (a <name>/version repository per artifact) is not mirrored
// yet: the registries do not publish it for the CLI, and only a test tag for
// plugins.
package dist
