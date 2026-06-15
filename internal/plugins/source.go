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

// PluginSource is the backend the plugin commands pull from: it lists a plugin's
// versions, reads a plugin contract, and extracts a plugin binary to disk. The
// in-cluster registry-packages-proxy client (rppPluginSource) implements it.
// Listing the whole catalog is not part of the contract: the proxy serves only
// allow-listed images by exact name and exposes no catalog endpoint.
type PluginSource interface {
	ListPluginTags(ctx context.Context, pluginName string) ([]string, error)
	GetPluginContract(ctx context.Context, pluginName, tag string) (*internal.Plugin, error)
	ExtractPlugin(ctx context.Context, pluginName, tag, destination string) error
}
