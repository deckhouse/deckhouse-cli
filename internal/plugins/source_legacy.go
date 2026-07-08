/*
Copyright 2025 Flant JSC

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

// LEGACY --source bypass (temporary).
//
// registryPluginSource restores the pre-#386 direct registry access: it pulls
// plugin images straight from a registry through go-containerregistry, bypassing
// the registry-packages-proxy and the cluster. Activated only when --source is
// set (see internal/plugins/init.go). Removal steps are listed in
// internal/plugins/flags/source_legacy.go. Grep marker: "legacy --source".

package plugins

import (
	"archive/tar"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"
	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal"
	d8flags "github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
)

// legacyPluginsSegment mirrors the RPP layout deckhouse-cli/plugins/<name>: the
// direct path is <--source>/plugins/<name>, so --source names the CLI image repo.
const legacyPluginsSegment = "plugins"

// legacyBinaryByteLimit caps the extracted plugin binary (512 MiB), matching
// rpp.DefaultBinaryByteLimit.
const legacyBinaryByteLimit int64 = 512 << 20

// legacyArchiveByteLimit bounds the TOTAL bytes read while walking the image
// tar, so a huge entry before the plugin binary cannot exhaust resources.
// Mirrors rpp maxArchiveBytes (1 GiB); the per-binary cap is the tighter guard.
const legacyArchiveByteLimit int64 = 1 << 30

// legacyExecutableMode is forced on the extracted binary so it is runnable
// regardless of the mode recorded in the image.
const legacyExecutableMode os.FileMode = 0o755

// initLegacyRegistrySource wires m.service to a direct registry (the --source
// bypass) and disables cluster-side requirement checks: the bypass targets
// environments without a cluster, matching the pre-#386 behavior where no such
// checks existed. Called from InitPluginServices when --source is set.
func (m *Manager) initLegacyRegistrySource() error {
	sanitized, err := validateLegacySource(d8flags.SourceRegistryRepo)
	if err != nil {
		return fmt.Errorf("invalid --source: %w", err)
	}

	d8flags.SourceRegistryRepo = sanitized

	m.logger.Warn("using the legacy --source registry bypass; cluster-side requirement checks are skipped",
		slog.String("source_repo", d8flags.SourceRegistryRepo))

	d8flags.SkipClusterChecks = true
	m.service = newRegistryPluginSource(m.logger)

	return nil
}

// validateLegacySource sanitizes a --source repo: it strips any URL scheme and a
// trailing slash, then validates the result as a <registry>/<repo> path. It
// fails fast with a clear error instead of letting a bad value surface as an
// opaque failure on the first registry call.
func validateLegacySource(source string) (string, error) {
	repo := strings.NewReplacer("http://", "", "https://", "").Replace(source)
	repo = strings.TrimSuffix(repo, "/")

	if repo == "" {
		return "", errors.New("registry repo is empty")
	}

	// Rejects an uppercase host, a :tag or @digest suffix, and other malformed
	// repositories; a trailing slash was already trimmed above.
	if _, err := name.NewRepository(repo); err != nil {
		return "", fmt.Errorf("not a valid registry repo: %w", err)
	}

	u, err := url.ParseRequestURI("docker://" + repo)
	if err != nil {
		return "", err
	}

	if u.Host == "" {
		return "", errors.New("no registry host")
	}

	if u.Path == "" {
		return "", errors.New("no registry path (expected <registry>/<repo>)")
	}

	return repo, nil
}

// registryPluginSource implements pluginSource against a registry reached
// directly, so the install pipeline is unchanged and only the transport differs.
type registryPluginSource struct {
	client dkpreg.Client
	logger *dkplog.Logger
}

var _ pluginSource = (*registryPluginSource)(nil)

// newRegistryPluginSource builds a source pinned to d8flags.SourceRegistryRepo.
// Auth priority: --source-login/password, then --license, then the Docker config
// (what `d8 dk cr login` writes), then anonymous.
func newRegistryPluginSource(logger *dkplog.Logger) *registryPluginSource {
	source := d8flags.SourceRegistryRepo
	auth := legacyRegistryAuth(legacyRegistryHost(source), logger)

	client := pkgclient.NewFromOptions(
		source,
		regclient.WithAuth(auth),
		regclient.WithInsecure(d8flags.Insecure),
		regclient.WithTLSSkipVerify(d8flags.TLSSkipVerify),
		regclient.WithLogger(logger.Named("registry-client")),
	)

	return &registryPluginSource{client: client, logger: logger}
}

// pluginClient scopes the base client to <source>/plugins/<name>.
func (s *registryPluginSource) pluginClient(pluginName string) dkpreg.Client {
	return s.client.WithSegment(legacyPluginsSegment, pluginName)
}

func (s *registryPluginSource) ListPluginTags(ctx context.Context, pluginName string) ([]string, error) {
	tags, err := s.pluginClient(pluginName).ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tags for plugin %q: %w", pluginName, err)
	}

	return tags, nil
}

func (s *registryPluginSource) GetPluginContract(ctx context.Context, pluginName, tag string) (*internal.Plugin, error) {
	encoded, err := s.resolveContractAnnotation(ctx, s.pluginClient(pluginName), tag)
	if err != nil {
		return nil, fmt.Errorf("get manifest for plugin %q: %w", pluginName, err)
	}

	if encoded == "" {
		// No contract annotation: surface just name+version, like rppPluginSource.
		s.logger.Debug("plugin image has no contract annotation",
			slog.String("plugin", pluginName), slog.String("tag", tag))

		return &internal.Plugin{Name: pluginName, Version: tag}, nil
	}

	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode contract annotation for plugin %q: %w", pluginName, err)
	}

	return contractFromBytes(raw, pluginName, tag)
}

// resolveContractAnnotation returns the base64 contract annotation for tag. The
// contract may sit on the index or on its (identical) child manifests: the index
// is read first, and the first child is followed only when the index carries
// none (matching rppPluginSource). An empty string means no contract was found.
func (s *registryPluginSource) resolveContractAnnotation(ctx context.Context, client dkpreg.Client, tag string) (string, error) {
	result, err := client.GetManifest(ctx, tag)
	if err != nil {
		return "", err
	}

	if !result.GetMediaType().IsIndex() {
		man, err := result.GetManifest()
		if err != nil {
			return "", err
		}

		return man.GetAnnotations()[contractAnnotation], nil
	}

	index, err := result.GetIndexManifest()
	if err != nil {
		return "", err
	}

	if encoded := index.GetAnnotations()[contractAnnotation]; encoded != "" {
		return encoded, nil
	}

	children := index.GetManifests()
	if len(children) == 0 {
		return "", nil
	}

	child, err := client.GetManifest(ctx, "@"+children[0].GetDigest().String())
	if err != nil {
		return "", err
	}

	childManifest, err := child.GetManifest()
	if err != nil {
		return "", err
	}

	return childManifest.GetAnnotations()[contractAnnotation], nil
}

func (s *registryPluginSource) ExtractPlugin(ctx context.Context, pluginName, tag, destination string) error {
	platform := &v1.Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH}

	img, err := s.pluginClient(pluginName).GetImage(ctx, tag, regclient.WithPlatform{Platform: platform})
	if err != nil {
		return fmt.Errorf("get image for plugin %q: %w", pluginName, err)
	}

	body := img.Extract()

	defer func() { _ = body.Close() }()

	if err := extractPluginBinary(body, destination); err != nil {
		return fmt.Errorf("extract %q binary: %w", pluginName, err)
	}

	return nil
}

// extractPluginBinary writes the entry named pluginBinaryEntryName from the
// image's flattened (uncompressed) tar to destination, forced executable and
// capped in size. The whole walk is bounded by legacyArchiveByteLimit so a huge
// entry before the target cannot exhaust resources. Non-regular entries and
// other files are skipped.
func extractPluginBinary(r io.Reader, destination string) error {
	tr := tar.NewReader(io.LimitReader(r, legacyArchiveByteLimit+1))

	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("binary %q not found in image", pluginBinaryEntryName)
		}

		if err != nil {
			return fmt.Errorf("read tar: %w", err)
		}

		if header.Typeflag != tar.TypeReg || path.Base(header.Name) != pluginBinaryEntryName {
			continue
		}

		return writeCappedBinary(destination, tr)
	}
}

func writeCappedBinary(destination string, r io.Reader) error {
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, legacyExecutableMode)
	if err != nil {
		return fmt.Errorf("create %q: %w", destination, err)
	}

	defer func() { _ = out.Close() }()

	written, err := io.Copy(out, io.LimitReader(r, legacyBinaryByteLimit+1))
	if err != nil {
		return fmt.Errorf("write %q: %w", destination, err)
	}

	if written > legacyBinaryByteLimit {
		return fmt.Errorf("%q exceeds the %d-byte limit", destination, legacyBinaryByteLimit)
	}

	// OpenFile honors the umask, so force the exact mode for an executable.
	if err := os.Chmod(destination, legacyExecutableMode); err != nil {
		return fmt.Errorf("chmod %q: %w", destination, err)
	}

	return nil
}

// legacyRegistryHost extracts the registry host from a --source repo (e.g.
// "registry.example.com/deckhouse/deckhouse-cli" -> "registry.example.com"),
// used only for the Docker-config credential lookup.
func legacyRegistryHost(source string) string {
	if !strings.Contains(source, "/") {
		return source
	}

	ref, err := name.ParseReference(source)
	if err != nil {
		host, _, _ := strings.Cut(source, "/")

		return host
	}

	return ref.Context().RegistryStr()
}

// legacyRegistryAuth resolves credentials for the source registry, mirroring the
// pre-#386 priority: explicit login/password, then license token, then the
// Docker config, then anonymous.
func legacyRegistryAuth(registryHost string, logger *dkplog.Logger) authn.Authenticator {
	if d8flags.SourceRegistryLogin != "" {
		logger.Debug("using --source-login credentials", slog.String("username", d8flags.SourceRegistryLogin))

		return authn.FromConfig(authn.AuthConfig{
			Username: d8flags.SourceRegistryLogin,
			Password: d8flags.SourceRegistryPassword,
		})
	}

	if d8flags.DeckhouseLicenseToken != "" {
		logger.Debug("using --license token")

		return authn.FromConfig(authn.AuthConfig{
			Username: "license-token",
			Password: d8flags.DeckhouseLicenseToken,
		})
	}

	if auth, ok := dockerConfigAuth(registryHost, logger); ok {
		return auth
	}

	logger.Debug("using anonymous access for the source registry", slog.String("registry", registryHost))

	return authn.Anonymous
}

// dockerConfigAuth resolves credentials for registryHost from the Docker config
// (~/.docker/config.json, written by `d8 dk cr login`). ok is false when the
// config holds no usable entry for the host.
func dockerConfigAuth(registryHost string, logger *dkplog.Logger) (authn.Authenticator, bool) {
	reg, err := name.NewRegistry(registryHost)
	if err != nil {
		return nil, false
	}

	auth, err := authn.DefaultKeychain.Resolve(reg)
	if err != nil || auth == authn.Anonymous {
		return nil, false
	}

	cfg, err := auth.Authorization()
	if err != nil {
		return nil, false
	}

	if cfg.Username == "" && cfg.Password == "" && cfg.Auth == "" && cfg.IdentityToken == "" {
		return nil, false
	}

	logger.Debug("using Docker config credentials", slog.String("registry", reg.String()))

	return auth, true
}
