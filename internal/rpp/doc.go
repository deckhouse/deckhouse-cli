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

// Package rpp is a client for the Deckhouse registry-packages-proxy (RPP) CLI
// routes:
//
//   - GET /v1/images/<image>/tags             - list available versions
//   - GET /v1/images/<image>/images/<version> - download a version's image
//
// It lets deckhouse-cli list available versions of itself and its plugins and
// download their images. All traffic goes to the in-cluster proxy and is
// authenticated with the caller's kubeconfig identity; no separate registry
// credentials are needed, because the proxy fetches from the backing registry
// on the CLI's behalf.
//
// Pulls carry a ?platform=<os>-<arch> query (from the running binary's
// GOOS/GOARCH) so the proxy selects the matching image from a multi-platform
// index. A proxy too old to honor it falls back to its default platform.
package rpp
