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

// Package completion provides shell-completion helpers for the `d8 cr`
// subtree. The command files in basic/ and fs/ wire these in via
// ValidArgsFunction and RegisterFlagCompletionFunc.
//
// Completion-time network calls (ListCatalog/ListTags) are bounded by
// completionTimeout and degrade silently to an empty result on any
// failure - completion must never surface a stack trace into the
// user's terminal.
package completion

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/rootflagnames"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/imageio"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

// PullFormats returns the static enum used for `cr pull --format`.
// Forwarded from imageio so that the cobra command and completion read
// from the same source of truth.
func PullFormats() []string { return imageio.PullFormats() }

const (
	// Shell typically aborts a completion at ~2-3s. We cap shorter so a
	// slow registry returns an empty list instead of a truncated frame.
	completionTimeout = 2 * time.Second

	// Cap on the suggestion list. ListTags for a popular repo (e.g. nginx)
	// can return thousands of pages - more than the user can scan anyway.
	completionMaxItems = 200
)

// errStopPagination breaks ListTags/ListCatalog iteration once we have
// enough items. Treated as a clean stop, not an error.
var errStopPagination = errors.New("stop pagination")

// refKind classifies what the user is currently typing in an IMAGE/REPO
// argument so the completer knows what to suggest.
type refKind int

const (
	kindEmpty      refKind = iota // ""
	kindHost                      // "docker.io" - hostname (no '/' yet)
	kindHostSlash                 // "docker.io/" - registry chosen, list repos
	kindRepoPath                  // "docker.io/lib" - typing repo path
	kindRepoColon                 // "docker.io/lib/nginx:" - typing tag
	kindRepoDigest                // "docker.io/lib/nginx@sha256:..." - typing digest
)

type refParts struct {
	kind     refKind
	host     string
	repoPath string
	tagPart  string
}

// parseRef classifies toComplete. Tag separator detection is anchored to
// the part AFTER the first '/', so "localhost:5000/repo:tag" stays parseable
// (the ':' before 5000 is the port, not a tag separator). Digest refs
// ("repo@sha256:...") are detected before the tag-separator scan so the
// ':' inside "sha256:hex" is not misread as a tag boundary - otherwise
// completion would synthesize an invalid repo path "repo@sha256" and
// silently fan out a doomed ListTags request.
func parseRef(s string) refParts {
	if s == "" {
		return refParts{kind: kindEmpty}
	}
	host, pathPart, found := strings.Cut(s, "/")
	if !found {
		return refParts{kind: kindHost, host: s}
	}
	if pathPart == "" {
		return refParts{kind: kindHostSlash, host: host}
	}
	if before, _, found := strings.Cut(pathPart, "@"); found {
		return refParts{
			kind:     kindRepoDigest,
			host:     host,
			repoPath: strings.TrimRight(before, "/"),
		}
	}
	if i := strings.LastIndex(pathPart, ":"); i != -1 {
		return refParts{
			kind:     kindRepoColon,
			host:     host,
			repoPath: pathPart[:i],
			tagPart:  pathPart[i+1:],
		}
	}
	// Trailing slashes are noise - "host/repo/" and "host/repo" address the
	// same repository for completion purposes. Storing the slash would only
	// matter if a future caller used parts.repoPath to build a request and
	// produced "host//repo".
	return refParts{kind: kindRepoPath, host: host, repoPath: strings.TrimRight(pathPart, "/")}
}

// ImageRef completes IMAGE arguments (host, host/repo, host/repo:tag).
func ImageRef() cobra.CompletionFunc {
	return func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return completeRefValue(cmd, toComplete, true)
	}
}

// RepoRef completes REPO arguments (cr ls): host, host/repo - never tags.
func RepoRef() cobra.CompletionFunc {
	return func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return completeRefValue(cmd, toComplete, false)
	}
}

// RegistryHost completes REGISTRY (cr catalog) from ~/.docker/config.json.
// No network call - just local credential config.
func RegistryHost() cobra.CompletionFunc {
	return func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return filterByPrefix(loadDockerConfigRegistries(), toComplete), cobra.ShellCompDirectiveNoFileComp
	}
}

// Static completes a flag value from a fixed enum.
func Static(values ...string) cobra.CompletionFunc {
	return func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return filterByPrefix(values, toComplete), cobra.ShellCompDirectiveNoFileComp
	}
}

// PathThenImage handles `push PATH IMAGE`-style commands: file completion
// for the first positional, image completion for the rest.
func PathThenImage() cobra.CompletionFunc {
	imgFn := ImageRef()
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return nil, cobra.ShellCompDirectiveDefault
		}
		return imgFn(cmd, args, toComplete)
	}
}

// ImageThenPath handles `export IMAGE [TARBALL]`-style commands: image
// completion for the first positional, file completion for the rest.
func ImageThenPath() cobra.CompletionFunc {
	imgFn := ImageRef()
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return imgFn(cmd, args, toComplete)
		}
		return nil, cobra.ShellCompDirectiveDefault
	}
}

// ImageThenInImagePath handles `cr fs ls/cat/tree/info IMAGE PATH`-style
// commands. First positional is IMAGE (network completion); subsequent
// positionals are in-image paths which are intentionally NOT completed
// (would require fetching layers per TAB - too expensive). NoFileComp is
// returned to avoid misleading the user with local file suggestions.
func ImageThenInImagePath() cobra.CompletionFunc {
	imgFn := ImageRef()
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return imgFn(cmd, args, toComplete)
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
}

// completeRefValue is the shared implementation for IMAGE/REPO completion.
// withTags=false short-circuits the kindRepoColon branch (cr ls REPO does
// not accept tags - if the user typed ':' anyway, we silently offer nothing).
func completeRefValue(cmd *cobra.Command, toComplete string, withTags bool) ([]string, cobra.ShellCompDirective) {
	parts := parseRef(toComplete)

	switch parts.kind {
	case kindEmpty, kindHost:
		// No registry selected yet - suggest hosts from docker config so
		// the user does not have to remember them. NoSpace because the
		// user must continue typing '/repo' after picking a host.
		return filterByPrefix(loadDockerConfigRegistries(), toComplete),
			cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace

	case kindHostSlash, kindRepoPath:
		repos := tryListCatalog(cmd, parts.host)
		if len(repos) == 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		suggestions := make([]string, 0, len(repos))
		for _, r := range repos {
			suggestions = append(suggestions, parts.host+"/"+r)
		}
		// NoSpace: user may want to keep typing ':tag' after a repo match.
		return filterByPrefix(suggestions, toComplete),
			cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace

	case kindRepoDigest:
		// Once the user typed '@', they have committed to pinning a digest
		// (a 64-char sha256 hex). Suggesting anything is unhelpful - the
		// registry exposes no API to enumerate blobs by prefix - and trying
		// would make doomed network calls under the 2s completion budget.
		return nil, cobra.ShellCompDirectiveNoFileComp

	case kindRepoColon:
		if !withTags {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		repoFull := parts.host + "/" + parts.repoPath
		tags := tryListTags(cmd, repoFull)
		if len(tags) == 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		suggestions := make([]string, 0, len(tags))
		for _, t := range tags {
			suggestions = append(suggestions, repoFull+":"+t)
		}
		return filterByPrefix(suggestions, toComplete), cobra.ShellCompDirectiveNoFileComp
	}
	return nil, cobra.ShellCompDirectiveNoFileComp
}

// tryListCatalog calls ListCatalog with a bounded context. Errors (404 from
// registries that do not implement /v2/_catalog, timeouts, auth failures)
// turn into an empty list - completion stays silent.
func tryListCatalog(cmd *cobra.Command, host string) []string {
	ctx, cancel := completionContext(cmd)
	defer cancel()
	opts := buildCompletionOpts(cmd)

	var items []string
	err := registry.ListCatalog(ctx, host, opts, func(repos []string) error {
		items = append(items, repos...)
		if len(items) >= completionMaxItems {
			return errStopPagination
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopPagination) {
		return nil
	}
	if len(items) > completionMaxItems {
		items = items[:completionMaxItems]
	}
	return items
}

// tryListTags is the ListTags counterpart to tryListCatalog.
func tryListTags(cmd *cobra.Command, repoRef string) []string {
	ctx, cancel := completionContext(cmd)
	defer cancel()
	opts := buildCompletionOpts(cmd)

	var items []string
	err := registry.ListTags(ctx, repoRef, opts, func(tags []string) error {
		items = append(items, tags...)
		if len(items) >= completionMaxItems {
			return errStopPagination
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopPagination) {
		return nil
	}
	if len(items) > completionMaxItems {
		items = items[:completionMaxItems]
	}
	return items
}

func completionContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, completionTimeout)
}

// buildCompletionOpts builds a fresh *registry.Options for completion-time
// network calls. PersistentPreRunE is NOT invoked during shell completion,
// so the cr persistent flags (--insecure, --platform) have not been applied
// to the shared opts. We re-read them off cmd here so completion respects
// the same flags the eventual RunE would.
func buildCompletionOpts(cmd *cobra.Command) *registry.Options {
	opts := registry.New()
	if insecure, err := cmd.Flags().GetBool(rootflagnames.Insecure); err == nil && insecure {
		opts.WithInsecure().WithTransport(registry.InsecureTransport())
	}
	if platform, err := cmd.Flags().GetString(rootflagnames.Platform); err == nil && platform != "" {
		if p, err := v1.ParsePlatform(platform); err == nil {
			opts.WithPlatform(p)
		}
	}
	return opts
}

// dockerConfigDir mirrors Docker's canonical resolution: $DOCKER_CONFIG
// (used by CI containers, rootless docker, and multi-profile setups) wins
// over $HOME/.docker. authn.DefaultKeychain (which `cr` uses at runtime)
// follows the same rule, so completion must too - otherwise a logged-in
// user sees an empty host list.
func dockerConfigDir() string {
	if d := os.Getenv("DOCKER_CONFIG"); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".docker")
}

// loadDockerConfigRegistries returns the registry hosts the user has logged
// into, parsed from <dockerConfigDir>/config.json. Both `auths` and
// `credHelpers` are considered. Hosts are normalized:
// "https://index.docker.io/v1/" -> "index.docker.io". Returns nil on any
// read/parse error - completion degrades to "no suggestions".
func loadDockerConfigRegistries() []string {
	dir := dockerConfigDir()
	if dir == "" {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		return nil
	}
	var cfg struct {
		Auths       map[string]json.RawMessage `json:"auths"`
		CredHelpers map[string]string          `json:"credHelpers"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil
	}
	seen := make(map[string]struct{}, len(cfg.Auths)+len(cfg.CredHelpers))
	var hosts []string
	add := func(raw string) {
		h := normalizeHost(raw)
		if h == "" {
			return
		}
		if _, dup := seen[h]; dup {
			return
		}
		seen[h] = struct{}{}
		hosts = append(hosts, h)
	}
	for k := range cfg.Auths {
		add(k)
	}
	for k := range cfg.CredHelpers {
		add(k)
	}
	sort.Strings(hosts)
	return hosts
}

// normalizeHost strips scheme and any trailing path so "https://index.docker.io/v1/"
// and "index.docker.io" collapse to the same key.
func normalizeHost(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "https://")
	s = strings.TrimPrefix(s, "http://")
	if i := strings.Index(s, "/"); i != -1 {
		s = s[:i]
	}
	return s
}

// filterByPrefix returns items whose value starts with prefix. An empty
// prefix returns the items unchanged.
func filterByPrefix(items []string, prefix string) []string {
	if prefix == "" {
		return items
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if strings.HasPrefix(item, prefix) {
			out = append(out, item)
		}
	}
	return out
}
