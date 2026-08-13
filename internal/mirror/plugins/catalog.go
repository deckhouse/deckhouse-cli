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
	"encoding/base64"
	"errors"
	"fmt"
	"sort"

	"github.com/Masterminds/semver/v3"
	"sigs.k8s.io/yaml"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/requirements"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// Catalog is the resolver's read model of the plugins registry catalog.
// Results are memoized for the lifetime of the catalog (one pull), so however
// many resolution paths probe the same plugin, the registry is asked once.
type Catalog interface {
	// PluginNames lists the plugin names published in the catalog.
	PluginNames(ctx context.Context) ([]string, error)
	// PluginVersions lists a plugin's published stable versions, newest
	// first. Non-semver tags (werf build junk) and genuine pre-releases are
	// dropped - the same notion of "stable" that plugin install uses.
	PluginVersions(ctx context.Context, name string) ([]*semver.Version, error)
	// Contract returns the decoded contract of one plugin version. An image
	// without a contract annotation yields a degenerate {Name, Version}
	// contract, not an error.
	Contract(ctx context.Context, name string, version *semver.Version) (*internal.Plugin, error)
}

// ErrInvalidContract marks a deterministic content problem of a published
// contract (broken base64, malformed JSON, failed domain validation). The
// resolver skips such versions and tries older ones; transport errors never
// carry this sentinel and fail the pull instead.
var ErrInvalidContract = errors.New("invalid plugin contract")

// registryCatalog implements Catalog over the plugins registry service.
type registryCatalog struct {
	service *registryservice.PluginsService

	versionsByName map[string][]*semver.Version
	contractsByRef map[string]*internal.Plugin

	logger *dkplog.Logger
}

// NewCatalog creates a memoizing catalog over the plugins registry service.
func NewCatalog(service *registryservice.PluginsService, logger *dkplog.Logger) Catalog {
	return &registryCatalog{
		service: service,

		versionsByName: make(map[string][]*semver.Version),
		contractsByRef: make(map[string]*internal.Plugin),

		logger: logger,
	}
}

func (c *registryCatalog) PluginNames(ctx context.Context) ([]string, error) {
	return c.service.ListTags(ctx)
}

func (c *registryCatalog) PluginVersions(ctx context.Context, name string) ([]*semver.Version, error) {
	if versions, ok := c.versionsByName[name]; ok {
		return versions, nil
	}

	tags, err := c.service.Plugin(name).ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("list versions of plugin %q: %w", name, err)
	}

	versions := stableVersions(sortedSemverDesc(tags))
	c.versionsByName[name] = versions

	return versions, nil
}

func (c *registryCatalog) Contract(ctx context.Context, name string, version *semver.Version) (*internal.Plugin, error) {
	tag := version.Original()

	ref := name + "@" + tag
	if contract, ok := c.contractsByRef[ref]; ok {
		return contract, nil
	}

	encoded, err := c.service.Plugin(name).ContractAnnotation(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("get contract of plugin %q %s: %w", name, tag, err)
	}

	contract, err := decodeContract(encoded, name, tag)
	if err != nil {
		return nil, err
	}

	c.contractsByRef[ref] = contract

	return contract, nil
}

// decodeContract turns the base64 contract annotation into a domain Plugin.
// An empty annotation is a contract-less image: only name and version are
// known, there is nothing to enforce. Same decode chain as the plugin install
// sources (internal/plugins rpp_source/source_legacy).
func decodeContract(encoded, name, tag string) (*internal.Plugin, error) {
	if encoded == "" {
		return &internal.Plugin{Name: name, Version: tag}, nil
	}

	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, invalidContract(name, tag, err)
	}

	// The decoded annotation is JSON; YAMLToJSON is a tolerant pass-through
	// (valid JSON is valid YAML) and the shared decoder expects JSON.
	jsonRaw, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return nil, invalidContract(name, tag, err)
	}

	var dto registryservice.PluginContract
	if err := registryservice.UnmarshalContract(jsonRaw, &dto); err != nil {
		return nil, invalidContract(name, tag, err)
	}

	plugin, err := registryservice.ContractToDomain(&dto)
	if err != nil {
		return nil, invalidContract(name, tag, err)
	}

	if plugin.Name == "" {
		plugin.Name = name
	}

	if plugin.Version == "" {
		plugin.Version = tag
	}

	return plugin, nil
}

// invalidContract wraps a deterministic contract-content error with the
// ErrInvalidContract sentinel. The original error is flattened into text: the
// resolver classifies by the sentinel alone and never unwraps further.
func invalidContract(name, tag string, err error) error {
	return fmt.Errorf("%w: plugin %q %s: %s", ErrInvalidContract, name, tag, err)
}

// stableVersions and sortedSemverDesc are ports of the same helpers in
// internal/plugins/select.go, so mirror keeps install's notion of a stable
// published version. Kept as copies: importing the plugin manager package
// from mirror would cross subsystem boundaries for two small functions.

// stableVersions drops genuine pre-releases (rc/alpha/beta), keeping CI/build
// markers like "v1.77.0-main".
func stableVersions(versions []*semver.Version) []*semver.Version {
	stable := make([]*semver.Version, 0, len(versions))

	for _, version := range versions {
		if version.Prerelease() != "" && requirements.IsGenuinePrerelease(version.Prerelease()) {
			continue
		}

		stable = append(stable, version)
	}

	return stable
}

// sortedSemverDesc parses tags as semver, drops the unparseable ones, and
// returns them sorted newest first.
func sortedSemverDesc(tags []string) []*semver.Version {
	versions := make([]*semver.Version, 0, len(tags))

	for _, tag := range tags {
		version, err := semver.NewVersion(tag)
		if err != nil {
			continue
		}

		versions = append(versions, version)
	}

	sort.Slice(versions, func(i, j int) bool { return versions[i].GreaterThan(versions[j]) })

	return versions
}
