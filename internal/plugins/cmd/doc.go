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
//   - Validate their dependencies (requirements declared in contract.yaml that plugins declare).
//   - Run them as if they were native subcommands.
//
// # Where a plugin lives
//
// A plugin lives in an OCI Registry as a packaged file with a "contract"
// annotation. The annotation carries a base64-encoded JSON contract.
// Plugin metadata:
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
//  3. If it is not, the image is pulled from the registry.
//  4. The binary is unpacked.
//  5. Requirements are validated.
//  6. A symlink is pointed at the current major version.
//  7. The plugin is exec'd with the forwarded arguments.
//
// # What the plugin system is made of
//
//  1. Discover - learn what plugins exist and what their contracts declare.
//  2. Install - download and place the plugin in the right location with
//     proper validation.
//  3. Exec - run the plugin as part of d8 without losing argument context.
package plugins
