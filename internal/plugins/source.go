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

package plugins

import (
	"context"

	"github.com/deckhouse/deckhouse-cli/internal"
)

// Transport names how the plugin subsystem reaches the registry. The two differ in
// more than plumbing - see pluginCatalog for the capability that only one of them
// has - so the transport in use is named in errors rather than left implicit.
type Transport string

const (
	// TransportRPP goes through the in-cluster registry-packages-proxy, authenticated
	// by the caller's kubeconfig identity and needing no registry credentials. It is
	// the default and the only supported transport (ADR #386).
	TransportRPP Transport = "registry-packages-proxy"

	// TransportRegistry talks to a registry repository directly, with credentials,
	// selected by the legacy --source bypass (see source_legacy.go).
	TransportRegistry Transport = "registry"
)

// pluginSource is the backend the plugin commands pull from: it lists a plugin's
// versions, reads a plugin contract, and extracts a plugin binary to disk. Every
// transport provides this much, addressing a plugin by exact name.
type pluginSource interface {
	Transport() Transport
	ListPluginTags(ctx context.Context, pluginName string) ([]string, error)
	GetPluginContract(ctx context.Context, pluginName, tag string) (*internal.Plugin, error)
	ExtractPlugin(ctx context.Context, pluginName, tag, destination string) error
}

// pluginCatalog is the optional capability of enumerating the published plugins,
// which a source declares by implementing it. The catalog is the tag list of the
// plugins repository, where each plugin has an image tagged with its name.
//
// Only TransportRegistry has it. The proxy cannot serve it: its allowlist admits
// deckhouse-cli and deckhouse-cli/plugins/<name> and explicitly refuses the bare
// deckhouse-cli/plugins path, so over TransportRPP there is nothing to ask and the
// request is never made.
type pluginCatalog interface {
	ListPluginNames(ctx context.Context) ([]string, error)
}
