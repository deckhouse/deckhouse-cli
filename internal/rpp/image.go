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

package rpp

import (
	"fmt"
	"net/url"
	"regexp"
)

const (
	// cliImageName is the registry image of the deckhouse-cli binary itself.
	cliImageName = "deckhouse-cli"

	// pluginsSegment namespaces plugin images under the CLI image path
	// (deckhouse-cli/plugins/<plugin>). The proxy allow-list permits exactly one
	// plugin name segment after it.
	pluginsSegment = "plugins"

	// imagesPathPrefix is the proxy route prefix for CLI image operations.
	imagesPathPrefix = "/v1/images/"

	// tagsPathSegment is the route segment that lists or addresses tags.
	tagsPathSegment = "tags"
)

// pluginNamePattern is the OCI repository path-component grammar (lowercase
// alphanumerics joined by single ./_/- separators). The proxy allow-list
// addresses a plugin as exactly one such component; anything else cannot name a
// published plugin and would only smuggle URL metacharacters into the route.
var pluginNamePattern = regexp.MustCompile(`^[a-z0-9]+(?:[._-][a-z0-9]+)*$`)

// tagPattern is the OCI tag grammar: up to 128 chars of [A-Za-z0-9._-], not
// starting with a separator. A string outside it (e.g. with ?, # or a leading
// dot) cannot be a real registry tag and must not reach the request URL.
var tagPattern = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`)

// ImageRef identifies an image the proxy is allowed to serve over /v1/images:
// either the deckhouse-cli binary or a single plugin. Construct it through
// CLIImage or PluginImage so the value always stays within the allow-list.
type ImageRef struct {
	path string
}

// CLIImage refers to the deckhouse-cli binary image.
func CLIImage() ImageRef {
	return ImageRef{path: cliImageName}
}

// PluginImage refers to a plugin image (deckhouse-cli/plugins/<name>). name must
// be a single OCI path component, matching the proxy allow-list.
func PluginImage(name string) (ImageRef, error) {
	if name == "" {
		return ImageRef{}, fmt.Errorf("%w: plugin name is empty", ErrInvalidImage)
	}

	if !pluginNamePattern.MatchString(name) {
		return ImageRef{}, fmt.Errorf("%w: plugin name %q is not a valid image path component", ErrInvalidImage, name)
	}

	return ImageRef{path: cliImageName + "/" + pluginsSegment + "/" + name}, nil
}

// String returns the image path as used in proxy URLs, e.g. "deckhouse-cli" or
// "deckhouse-cli/plugins/stronghold".
func (r ImageRef) String() string {
	return r.path
}

// tagsPath is the route that lists the image tags.
func (r ImageRef) tagsPath() string {
	return imagesPathPrefix + r.path + "/" + tagsPathSegment
}

// tagPath is the route that addresses a single tag of the image. The tag is
// path-escaped as defense in depth; after validateTag this is a no-op, but it
// keeps URL metacharacters out of the route even if validation ever loosens.
func (r ImageRef) tagPath(tag string) string {
	return r.tagsPath() + "/" + url.PathEscape(tag)
}

// validateTag rejects strings that cannot be a registry tag, so the proxy route
// (anchored on the final /tags/<tag> segment) cannot be altered by a crafted
// --version value (slashes, ?, #, leading dots).
func validateTag(tag string) error {
	if tag == "" {
		return fmt.Errorf("%w: tag is empty", ErrInvalidImage)
	}

	if !tagPattern.MatchString(tag) {
		return fmt.Errorf("%w: %q is not a valid image tag", ErrInvalidImage, tag)
	}

	return nil
}
