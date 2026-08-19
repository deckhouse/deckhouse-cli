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

// Package plugins mirrors d8 CLI plugins into the images bundle.
//
// Plugins live in a single registry catalog OUTSIDE the edition segment (same
// asymmetry as the installer):
//
//	<root>/deckhouse-cli/plugins                  - catalog; its tags are plugin names
//	<root>/deckhouse-cli/plugins/<name>:<vX.Y.Z>  - one plugin version (multi-platform OCI index)
//
// A plugin declares its requirements (Deckhouse modules, other plugins,
// platform versions) in a contract: a base64-JSON annotation on the image
// manifest. Reading a contract is a single manifest fetch, so deciding WHAT
// to mirror needs no layer downloads.
//
// Selection principle: nothing extra. A plugin enters the bundle only when a
// mirrored module needs it (its contract names that module), when another
// selected plugin requires it, or when the user asks for it explicitly with
// --include-plugin.
package plugins
