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

// Package plugins implements the d8-cli plugin system.
//
// A d8 plugin is a standalone binary published to an OCI Registry. It is
// not part of d8-cli and can be developed independently.
//
// # Why plugins exist
//
//   - Isolate dependencies.
//   - Enable independent, parallel development.
//   - Let different teams own different plugins (delivery, system, ...).
//   - Keep d8 itself compact, with only the dependencies it actually needs.
//
// # What d8-cli can do with plugins
//
//   - Download them.
//   - Validate their dependencies (requirements the plugin declares in its contract).
//   - Run them as if they were native subcommands.
//
// # Where a plugin lives
//
// A plugin lives in the cluster's OCI registry, reached exclusively through the
// in-cluster registry-packages-proxy. The image carries the plugin binary in its
// layers and a contract, published as a manifest annotation, that describes the plugin:
//
//   - name;
//   - version;
//   - description;
//   - environment variables;
//   - flags;
//   - requirements.
//
// # How a plugin invocation works
//
//  1. The user invokes a command through d8.
//  2. The parent CLI checks whether the plugin is installed.
//  3. If it is not, the image is pulled through the registry-packages-proxy.
//  4. The binary is unpacked.
//  5. Requirements are validated.
//  6. A symlink is pointed at the current major version.
//  7. The plugin is exec'd with the forwarded arguments.
//
// # On-disk layout
//
// Installed plugins live under the install root: /opt/deckhouse/lib/deckhouse-cli
// by default (override with --plugins-dir / DECKHOUSE_CLI_PATH; ~/.deckhouse-cli
// when the default is not writable). Concrete paths:
//
//	<root>/plugins/<name>/v<major>/<name>   plugin binary (one per major version)
//	<root>/plugins/<name>/current           symlink to the active major's binary
//	<root>/plugins/<name>/install.lock      install lock (one per plugin)
//	<root>/cache/contracts/<name>.json      cached contract
//
// Versions are kept per major; the `current` symlink selects the active one, so
// switching is an atomic repoint - the same idea selfupdate uses for d8 itself.
// Package internal/plugins/layout holds the authoritative path builders.
//
// # What the plugin system is made of
//
//  1. Discover - learn what plugins exist and what their contracts declare.
//  2. Install - download and place the plugin in the right location with
//     proper validation.
//  3. Exec - run the plugin as part of d8 without losing argument context.
package plugins
