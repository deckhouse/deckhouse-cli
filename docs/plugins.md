# `d8 dist plugins`

This document specifies d8 plugins: how a plugin is published in a registry, the contract it declares, what a working setup requires, how d8 reaches the plugin, how `d8 dist plugins` selects a version, resolves dependencies, checks cluster requirements and installs it, where it lives on disk, and how `d8` runs it. It describes the implementation as of 2026-10-01, deckhouse-cli at this revision and the registry-packages-proxy of deckhouse `main`, and every rule names the code that implements it. Where the implementation breaks the contract, §15 lists the deviation.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119. Rules for the publisher apply to anything that puts plugin images into a registry, a plugin's CI or `d8 mirror push`; rules for the client describe what d8 does.

Plugins reach the registry-packages-proxy the same way self-update does, and the identity, the endpoint, TLS, the status mapping and the proxy itself are specified in [self-update.md](self-update.md) §4 and §5; this document states only what differs for plugins. How `d8 mirror` selects plugins and carries them into an air-gapped registry is described in [internal/mirror/README.MD](../internal/mirror/README.MD) "Plugin Mirroring" and in [mirror-bundle-layout.md](mirror-bundle-layout.md) §6.8 and §8. The package structure is in [internal/plugins/README.md](../internal/plugins/README.md).

- [1. Terms](#1-terms)
- [2. Registry format](#2-registry-format)
- [3. Contract](#3-contract)
- [4. Requirements](#4-requirements)
- [5. Reaching the registry](#5-reaching-the-registry)
- [6. Commands](#6-commands)
- [7. Version selection](#7-version-selection)
- [8. Plugin dependencies](#8-plugin-dependencies)
- [9. Cluster requirements](#9-cluster-requirements)
- [10. Install and remove](#10-install-and-remove)
- [11. On-disk layout](#11-on-disk-layout)
- [12. Running a plugin](#12-running-a-plugin)
- [13. Publishing a plugin](#13-publishing-a-plugin)
- [14. Example](#14-example)
- [15. Known deviations](#15-known-deviations)

## 1. Terms

| Term | Meaning |
|---|---|
| Plugin | An executable published as an OCI image under `deckhouse-cli/plugins/<name>`, which d8 installs and runs. It is not part of the d8 binary. |
| Name | The name of the plugin's repository, matching `^[a-z0-9]+(?:[._-][a-z0-9]+)*$` (`ValidatePluginName` in [plugins/layout/layout.go](../internal/plugins/layout/layout.go)). It is also the name of the install directory and of the command. |
| Proxy | The registry-packages-proxy ([self-update.md](self-update.md#5-the-registry-packages-proxy) §5). |
| `<root>` | The registry repository that holds `deckhouse-cli` and `deckhouse-cli/plugins/<name>` (§2.6). |
| Platform | `<GOOS>-<GOARCH>` of the running d8, for example `linux-amd64` (`currentPlatform` in [rpp/platform.go](../internal/rpp/platform.go)). |
| Version | A tag of the plugin repository that parses as a semantic version (§2.2). |
| Stable version | A version without a pre-release part, or with one whose first dot-separated identifier does not start, case-insensitively, with `alpha`, `beta`, `rc`, `pre`, `preview` or `snapshot` (`IsGenuinePrerelease` in [plugins/requirements/checks.go](../internal/plugins/requirements/checks.go)). `v1.2.0` and `v1.2.0-main` are stable, `v1.2.0-rc.1` is not. |
| Major | The major number of a version. The installed major is read from the `current` link (§11.2). |
| Contract | The document a plugin publishes in its `contract` annotation (§3). |
| Cached contract | The contract of the active version, stored at `<dir>/cache/contracts/<name>.json` (§11.2). |
| `<dir>` | The install root (§11.1). |
| Transport | How d8 reaches the registry: through the proxy, or directly with the hidden `--source` (§5.1). |
| Cluster requirement | A `kubernetes`, `deckhouse` or `modules` requirement of a contract, checked against the cluster (§9). |
| Overridable command | One of the nine top-level commands that an installed plugin of the same name replaces (§12.1). |
| Wrapper | The command `d8 <name>` that runs an installed plugin in place of an overridable command (§12). |
| Built-in dependency | `delivery-kit` or `package` while d8 serves it with its built-in command: a plugin dependency on that name counts as satisfied (§8.4). |

## 2. Registry format

### 2.1. Repository

A plugin `<name>` is the repository `<root>/deckhouse-cli/plugins/<name>`. d8 addresses it by exact name only: through the proxy as the image `deckhouse-cli/plugins/<name>` (`PluginImage` in [rpp/image.go](../internal/rpp/image.go)), with `--source <repo>` as `<repo>/plugins/<name>` (`pluginClient` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)). A name that does not match the pattern of §1 is rejected before it reaches a file path or a route.

### 2.2. Tags

- Every tag that parses as a semantic version is a version of the plugin. Parsing is lenient (`semver.NewVersion` of Masterminds semver): the `v` prefix is optional and missing minor and patch numbers are 0, so `1.2` is `1.2.0`. Other tags, such as `latest`, are skipped by selection and listing (`sortedSemverDesc` in [plugins/select.go](../internal/plugins/select.go)).
- d8 requests a version by the original text of its tag, so `--version` MUST be spelled as published: `--version 1.2.0` does not find the tag `v1.2.0`.
- A tag MUST match `^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$` to be downloadable through the proxy (`validateTag` in [rpp/image.go](../internal/rpp/image.go)).
- Versions are ordered by SemVer precedence: a pre-release sorts below its release, and pre-release identifiers compare one by one, alphanumeric ones in ASCII order, so `v1.0.0-windows-amd64` sorts above `v1.0.0-linux-amd64`.
- A tag `<version>-<os>-<arch>` whose last two hyphen-separated tokens are a known GOOS and GOARCH is a per-platform tag of `<version>` (`SplitPlatform` in [plugins/platform.go](../internal/plugins/platform.go)). Only `versions`, `list` and the current-version marker collapse such tags; selection and install treat each as a version of its own (§15.5). Publishers SHOULD publish a release as one tag, a multi-platform index (§2.3).

### 2.3. Image

- A tag MUST resolve to an image manifest or an image index, in OCI or Docker media types (`acceptManifest` in [rpp/client.go](../internal/rpp/client.go)).
- A download through the proxy requests the platform of the running d8 (`PullImage` in [rpp/client.go](../internal/rpp/client.go)). The proxy resolves an index to the child with that `os` and `architecture` and answers 404 when the index has none; a single image is served whatever its platform. The response is the last layer of the resolved image, as stored in the registry ([self-update.md](self-update.md#55-platform-and-body) §5.5). With `--source`, d8 pulls the same platform with go-containerregistry and reads the filesystem merged from all layers (`ExtractPlugin` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)).
- The executable is the first regular-file entry whose base name is `plugin`, so `plugin`, `./plugin` and `bin/plugin` all match; every other entry is skipped. Through the proxy it MUST therefore be in the last layer, and that layer MUST be a gzip-compressed tar. The file MUST NOT exceed 512 MiB, and d8 reads at most 1 GiB of decompressed tar in all (`ExtractFileToPath` in [rpp/extract.go](../internal/rpp/extract.go)). It is written with mode `0755` whatever the archive records. A tar without it fails the install with `file not found in image: "plugin"`.
- The executable MUST exit 0 within 10 s when run with `--version` or, if that fails, with `version` (`pluginVersionProbe` in [plugins/install.go](../internal/plugins/install.go)). This is the smoke test of every download (§10.1).
- Its stdout for that call, without surrounding white space, MUST be a version equal to the tag (`pluginBinaryVersion`). d8 takes the installed version from it and from nowhere else: output that does not parse makes every install download the plugin again (§10.2), disables the downgrade guard (§7.2), shows `ERROR` in `list`, and stops the install of every plugin that depends on it (§8.2).

### 2.4. Contract annotation

- The contract is the manifest annotation `contract`, whose value is the standard base64 encoding, with padding, of the contract document (§3) (`contractAnnotation` in [plugins/rpp_source.go](../internal/plugins/rpp_source.go)).
- For an index, d8 reads the annotation of the index and, when the index has none, the annotation of its first child (`manifests[0]`) and of no other (`contractAnnotation`; `resolveContractAnnotation` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)). A multi-platform plugin MUST carry the contract on the index or, identically, on every child.
- Reading a contract costs one manifest request, two for an index without the annotation; no layer is pulled. A manifest longer than 1 MiB is cut off and fails to decode (`maxManifestResponseBytes` in [rpp/client.go](../internal/rpp/client.go)).
- An image without the annotation is a contract-less plugin: its contract is the name and the tag, with no requirements, env or flags (`GetPluginContract`). An annotation that is not valid base64, or a document that fails §3, is an error, and selection stops on it instead of trying an older version (§7.2).

### 2.5. Catalog

- The tags of the repository `<root>/deckhouse-cli/plugins` are plugin names: a tag `<name>` there announces the plugin `<name>` (`pluginCatalog` in [plugins/source.go](../internal/plugins/source.go)). `d8 mirror push` writes such a tag for every plugin it pushes ([mirror-bundle-layout.md](mirror-bundle-layout.md#88-discovery-tags) §8.8).
- Only `--source` reads the catalog, for `d8 dist plugins list` (`ListPluginNames` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)). The proxy serves no image `deckhouse-cli/plugins` ([self-update.md](self-update.md#53-routes) §5.3), so d8 never asks it, and `list` reports that half as unsupported (`AvailablePlugins` in [plugins/list.go](../internal/plugins/list.go)). The automatic plugin selection of `d8 mirror pull` starts from the catalog too.
- Catalog names are validated like any name, and an invalid one is listed with the note `not a valid plugin name` (`remotePluginInfo`).

### 2.6. Location

The proxy looks for `deckhouse-cli/plugins/<name>` under the cluster repository C and then, when C ends with `ce`, `be`, `se`, `se-plus`, `ee` or `fe` and the host and at least one path segment remain without it, under the parent of C, exactly as for `deckhouse-cli` ([self-update.md](self-update.md#54-repository-lookup) §5.4):

| Cluster repository C | Plugin `<name>` is read from |
|---|---|
| `registry.deckhouse.ru/deckhouse/ee` | `registry.deckhouse.ru/deckhouse/ee/deckhouse-cli/plugins/<name>`, then `registry.deckhouse.ru/deckhouse/deckhouse-cli/plugins/<name>` |
| `registry.local/mirror` | `registry.local/mirror/deckhouse-cli/plugins/<name>` |
| `registry.local/ee` | `registry.local/ee/deckhouse-cli/plugins/<name>`; the edition has no parent |
| `registry.local/deckhouse/cse` | `registry.local/deckhouse/cse/deckhouse-cli/plugins/<name>`; `cse` is not stripped |

`d8 mirror push` to a target that ends with an edition publishes plugins one level up, at `R/deckhouse-cli/plugins/<name>` ([mirror-bundle-layout.md](mirror-bundle-layout.md#810-routing-table) §8.10). The proxy finds them there for every edition but `cse`, so a CSE cluster filled by `d8 mirror push` finds no plugin, for the reason given in [self-update.md](self-update.md#14-known-deviations) §14.12.

## 3. Contract

### 3.1. Schema

```json
{
  "name": "example",
  "version": "v1.4.0",
  "description": "Example plugin",
  "env": [{"name": "KUBECONFIG"}, {"name": "PLUGINS_CALLER"}],
  "flags": [{"name": "--verbose"}],
  "requirements": {
    "kubernetes": {"constraint": ">=1.28"},
    "deckhouse": {"constraint": ">=1.70"},
    "modules": {
      "mandatory": [{"name": "console", "constraint": ">=1.0"}],
      "conditional": [{"name": "prometheus", "constraint": ">=0.5"}],
      "anyOf": [{"name": "storage", "description": "a storage backend", "modules": [{"name": "sds-local-volume"}, {"name": "csi-ceph"}]}],
      "noneOf": [{"name": "legacy", "modules": [{"name": "old-module", "constraint": "<2.0"}]}]
    },
    "plugins": {
      "mandatory": [{"name": "delivery-kit", "constraint": ">=0.1"}],
      "conditional": [{"name": "system", "constraint": ">=1.0"}]
    }
  }
}
```

| Field | Meaning |
|---|---|
| `name` | MUST equal the repository name. d8 does not check it, but matches the requirements of other plugins against it (§8.1). Empty means the repository name. |
| `version` | MUST equal the tag. Empty means the tag. Matched against the requirements of other plugins, and then it MUST parse as a version (§8.1). |
| `description` | Shown in the install banner, in `list` and in the help of the wrapper, with control characters removed (`printable` in [plugins/install.go](../internal/plugins/install.go)). |
| `env[].name` | Environment variables the plugin asks d8 to provide (§12.3). |
| `flags[].name` | Flags the plugin accepts. Used only in the help of the wrapper. |
| `requirements.kubernetes.constraint` | Constraint on the Kubernetes version (§9.2). |
| `requirements.deckhouse.constraint` | Constraint on the Deckhouse version (§9.2). |
| `requirements.modules.mandatory[]` | Modules that MUST be enabled, at a version that satisfies `constraint` when one is given. |
| `requirements.modules.conditional[]` | Modules whose version is checked only when they are enabled. |
| `requirements.modules.anyOf[]` | Named groups, each of which MUST have an enabled member that satisfies its constraint. |
| `requirements.modules.noneOf[]` | Named groups none of whose members may be enabled within its constraint; a member without a constraint is forbidden at any version. |
| `requirements.plugins.mandatory[]` | Plugins that MUST be installed at a version that satisfies `constraint`. d8 installs and upgrades them (§8.2). |
| `requirements.plugins.conditional[]` | Plugins whose version is checked only when they are installed. d8 never installs them (§8.3). |

Constraints use the dialect of Masterminds semver, such as `>=1.2`, `~1.2`, `^1` or `>=1.0 <2.0 || >=3`. Every `constraint` MAY be empty, which matches any version, but a plugin requirement without a constraint breaks the dependency (§15.1).

### 3.2. Decoding

- d8 decodes the annotation from base64, converts it from YAML to JSON with `sigs.k8s.io/yaml`, so a YAML document is accepted too, and unmarshals it with `encoding/json`, which matches keys case-insensitively and ignores unknown ones (`contractFromBytes` in [plugins/rpp_source.go](../internal/plugins/rpp_source.go), `UnmarshalContract` in [pkg/registry/service/contract.go](../pkg/registry/service/contract.go)).
- `requirements.modules` and `requirements.plugins` MUST be objects. The flat arrays of the former v1 schema fail with `invalid contract: field "requirements.modules" must be an object with mandatory/conditional sections, got a JSON array`.

### 3.3. Validation

`ContractToDomain` ([pkg/registry/service/contract.go](../pkg/registry/service/contract.go)) rejects a contract, and with it the operation, when:

- a module is in both `modules.mandatory` and `modules.conditional`;
- an `anyOf` or `noneOf` group has no name, a name used by another group of the same bucket, no members, a member without a name, a member listed twice, or a member constraint that does not parse;
- a module is in `mandatory` or `conditional` and also in an `anyOf` or `noneOf` group, or is in both an `anyOf` and a `noneOf` group.

A module MAY appear in two groups of the same bucket. Every other constraint is parsed only when it is checked, and one that does not parse is then an operational error, not an unmet requirement (§9.2).

Module requirements SHOULD name external modules, served from `<edition>/modules/<name>`, and not modules embedded in the platform, because the automatic selection of `d8 mirror pull` follows external modules only ([internal/plugins/README.md](../internal/plugins/README.md) "Boundaries and deliberate decisions").

## 4. Requirements

Installing and running plugins works when all of the following hold:

1. The identity reaches the proxy as for self-update ([self-update.md](self-update.md#3-requirements) §3, items 1 to 4): a kubeconfig with a bearer token, the ClusterRole `d8:registry-packages-proxy:cli-download`, which covers plugins because they travel the same `/v1/images/` routes, `get` on the Ingress `registry-packages-proxy` or an explicit endpoint, and a reachable endpoint with a trusted certificate. Every subcommand but `list` initializes the proxy client, so even `remove` needs a readable kubeconfig and, without an explicit endpoint, a reachable API server (§6.2).
2. For a plugin with a cluster requirement, unless checks are skipped (§9.4), the identity may also `get` the Deployment `deckhouse` in `d8-system` and `list` `modules.deckhouse.io`; `GET /version` is open to every authenticated identity by default. `cli-download` grants neither (§9.1, §15.11).
3. The user can create `<dir>/plugins`, or `<dir>` cannot be created and `$HOME/.deckhouse-cli` is writable (§11.1).
4. The plugin is published as §13 requires, at a root the proxy tries (§2.6).
5. To run it as `d8 <name>`, `<name>` is one of the nine overridable commands (§12.1).

## 5. Reaching the registry

### 5.1. Transports

`InitPluginServices` ([plugins/init.go](../internal/plugins/init.go)) picks the transport when a command starts:

| | proxy | `--source` |
|---|---|---|
| Selected by | default | the hidden flag `--source <repo>` |
| Plugin repository | `deckhouse-cli/plugins/<name>` at a root of §2.6 | `<repo>/plugins/<name>` |
| Credentials | the kubeconfig identity | registry credentials (§5.3) |
| Catalog for `list` | not served | read |
| Cluster requirement checks | enforced | always skipped |
| Code | `rppPluginSource` in [plugins/rpp_source.go](../internal/plugins/rpp_source.go), [rpp](../internal/rpp/) | `registryPluginSource` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go) |

`--source` is a temporary escape hatch for environments without a cluster and is meant to be removed (grep marker `legacy --source`).

### 5.2. Through the proxy

Identity, endpoint, TLS, request limits and status mapping are those of [self-update.md](self-update.md#4-reaching-the-proxy) §4. The plugin requests are made by `ListTags`, `GetManifest` and `PullImage` in [rpp/client.go](../internal/rpp/client.go):

| Operation | Request | Used by |
|---|---|---|
| List versions | `GET <endpoint>/v1/images/deckhouse-cli/plugins/<name>/tags`; the body, read up to 4 MiB, MUST be a JSON object whose member `tags` is an array of strings | selection, `versions`, `contract`, dependencies |
| Read a manifest | `GET <endpoint>/v1/images/deckhouse-cli/plugins/<name>/manifests/<ref>`, accepting the OCI and Docker manifest and index media types; the body is read up to 1 MiB | contracts (§2.4) |
| Download | `GET <endpoint>/v1/images/deckhouse-cli/plugins/<name>/images/<tag>?platform=<platform>` | install (§2.3) |

- `<ref>` MUST be a tag that matches the pattern of §2.2 or a digest `<algorithm>:<hex>` (`validateRef` in [rpp/image.go](../internal/rpp/image.go)).
- During selection, a `404` on the tags of a dependency means that the dependency is not published, and it only rejects the candidate (§8.2). Every other error stops the command.
- d8 verifies no digest of a download, since the proxy reports none for the body; integrity rests on the TLS connection and the smoke test (§10.1).
- The plugin commands turn recognised errors into their own diagnostics (`Diagnose` in [plugins/cmd/errdetect/diagnose.go](../internal/plugins/cmd/errdetect/diagnose.go)):

| Error | Diagnostic category |
|---|---|
| `unauthorized` (`401`) | `registry-packages-proxy: unauthorized (401)` |
| `forbidden` (`403`) | `registry-packages-proxy: forbidden (403)`, advising the wrong ClusterRole (§15.4) |
| `image or tag not found` (`404`) | `registry-packages-proxy: plugin or version not found (404)` |
| `registry proxy upstream error` (`5xx`) | `registry-packages-proxy: upstream error (5xx)` |
| endpoint discovery failed | `registry-packages-proxy: endpoint discovery via the Kubernetes API failed` |

### 5.3. `--source`

- `http://`, `https://` and a trailing `/` are removed from `<repo>`, and the rest MUST be a valid repository with a host and a path (`validateLegacySource` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)). Plugins are `<repo>/plugins/<name>` and the catalog is `<repo>/plugins`, so `<repo>` names the `deckhouse-cli` repository, such as `registry.deckhouse.ru/deckhouse/deckhouse-cli`.
- Credentials are `--source-login` and `--source-password`, by default `$D8_MIRROR_SOURCE_LOGIN` and `$D8_MIRROR_SOURCE_PASSWORD`; else `--license`, by default `$D8_MIRROR_LICENSE_TOKEN`, as the user `license-token`; else the Docker config entry of the host; else anonymous access (`legacyRegistryAuth`). `--insecure` talks HTTP, `--tls-skip-verify` skips certificate verification.
- It turns on `--skip-cluster-checks` and needs no kubeconfig (`initLegacyRegistrySource`).

The six flags are hidden from `--help` ([plugins/flags/source_legacy.go](../internal/plugins/flags/source_legacy.go)).

## 6. Commands

### 6.1. Command tree

| Command | Effect |
|---|---|
| `d8 dist plugins install <name>` | Installs `<name>`, or updates it, to the default selection (§7.2). |
| `d8 dist plugins install <name> --version V` | Installs the tag V, pre-releases included (§7.3). |
| `d8 dist plugins install <name> --use-major N` | Installs the default selection within major N, switching majors if needed (§7.2). |
| `d8 dist plugins install … --force` | Downloads again even when the selected version is active (§10.1). |
| `d8 dist plugins install --all [--force]` | Updates every installed plugin, each within its own major (§7.4). |
| `d8 dist plugins versions <name>` | Lists every version, pre-releases included, newest first, one line per version with its per-platform tags collapsed into a list of platforms. The version the installed binary reports is marked `*` and `current`, newer ones `newer`; an installed version that is not published is reported below the list (`PublishedVersions`, `InstalledVersionOrNil` in [plugins/versions.go](../internal/plugins/versions.go)). |
| `d8 dist plugins contract <name>` | Prints as YAML the contract of the newest stable version, whatever its major and cluster compatibility (`LatestVersion` in [plugins/validators.go](../internal/plugins/validators.go), [plugins/cmd/contract.go](../internal/plugins/cmd/contract.go)). |
| `d8 dist plugins list` | Lists every entry of `<dir>/plugins` with the version its binary reports and the description of its cached contract, then the catalog (§2.5). |
| `d8 dist plugins remove <name>`, aliases `uninstall` and `delete` | Removes one plugin (§10.4). |
| `d8 dist plugins remove all` | Removes every plugin (§10.4). |
| `d8 <name> [args]` | Runs an installed plugin, for the nine overridable names only (§12). |

`install` takes exactly one name, or `--all` and no name; `--all` rejects `--version` and `--use-major` (`validateInstallArgs` in [plugins/cmd/install.go](../internal/plugins/cmd/install.go)). `versions`, `contract` and `remove` take exactly one name, `list` takes none.

### 6.2. Startup

Before any subcommand of `d8 dist plugins` but `list` runs, d8 (`PersistentPreRunE` in [plugins/cmd/plugins.go](../internal/plugins/cmd/plugins.go)):

1. takes `<dir>` from `--plugins-dir`;
2. builds the client of the transport (§5.1). A failure ends the command, so `versions`, `contract` and even `remove`, which does not use the registry, need a readable kubeconfig and, without an explicit endpoint, a reachable API server;
3. creates `<dir>/plugins`, switching to the home root on a permission error (§11.1). A failure here is only a warning.

`list` creates the root first and does not fail on the transport: it prints the installed half, then why the registry half is missing, and exits 0 ([plugins/cmd/list.go](../internal/plugins/cmd/list.go)).

### 6.3. Flags and environment

The kubeconfig and proxy flags of `d8 dist` apply to every plugin command ([self-update.md](self-update.md#11-flags-and-environment-variables) §11). The plugin commands add ([plugins/flags/flags.go](../internal/plugins/flags/flags.go)):

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--plugins-dir` | `DECKHOUSE_CLI_PATH` | `/opt/deckhouse/lib/deckhouse-cli` | install root (§11.1); command registration reads only the environment (§12.1) |
| `--skip-cluster-checks` | `D8_PLUGINS_SKIP_CLUSTER_CHECKS` | off | skip cluster requirement checks (§9.4) |
| `--source`, `--source-login`, `--source-password`, `--license`, `--insecure`, `--tls-skip-verify` | §5.3 | | hidden, direct registry access (§5.3) |
| `--version`, `--use-major`, `--force`, `--all` (`install` only) | | | §6.1, §7 |

`D8_PLUGINS_SKIP_CLUSTER_CHECKS` is on for `1`, `true`, `TRUE` and `True` only (`skipClusterChecksDefault`). The wrapper parses no flags (§12.3): it reads `KUBECONFIG`, `D8_RPP_ENDPOINT`, `D8_RPP_CA_FILE` and `D8_PLUGINS_SKIP_CLUSTER_CHECKS`, uses the current context of the kubeconfig, and cannot skip verification of the proxy certificate.

### 6.4. Output and exit status

A failed command prints its error to stderr and exits 1; proxy failures and failed selections print as a diagnostic with causes and fixes (`wrapProxyDiagnostics` in [plugins/cmd/plugins.go](../internal/plugins/cmd/plugins.go)). `install --all` reports each failed plugin as it goes, continues with the next, and fails at the end with `failed to update N plugin(s): <names>` (`UpdateAll` in [plugins/update.go](../internal/plugins/update.go)). The wrapper exits with the plugin's status (§12.3).

## 7. Version selection

### 7.1. Candidates

The candidates are the versions of the plugin (§2.2), newest first. The default selection considers stable versions only (`stableVersions` in [plugins/select.go](../internal/plugins/select.go)).

### 7.2. Default selection

`InstallPlugin` ([plugins/install.go](../internal/plugins/install.go)) without `--version`:

1. Lists the tags, once per plugin and command (`listTags` in [plugins/select.go](../internal/plugins/select.go)).
2. Pins a major. `--use-major N` keeps the versions of major N. Without it, an installed plugin keeps the major its `current` link points at; a fresh install, or a link whose target does not parse, keeps every major (`inheritInstalledMajor`). A major without versions fails with `no versions found for major version: N`.
3. Walks the stable versions newest first and takes the first whose contract passes the cluster requirements (§9) and whose dependencies resolve (§8) (`selectCompatible` in [plugins/select.go](../internal/plugins/select.go), `selectTopWithPlan` in [plugins/planner.go](../internal/plugins/planner.go)). Each contract is fetched once per command (`PluginContract`). The newer versions it skips are printed with the reason. A contract or cluster read that fails stops the walk with that error: an operational failure never makes d8 settle for an older version.
4. Guards against downgrades. Without `--use-major`, when the installed binary reports a version newer than the selection, d8 keeps it, prints `Selected X is older than the installed version; keeping the installed version (use --version to downgrade explicitly).` and exits 0 (`installedIsNewerThan`).
5. When nothing qualifies, fails with `cannot install plugin "<name>": unresolved dependencies` and one entry per missing dependency when every rejection was a dependency problem, with `cannot install plugin "<name>": no installable version` and every rejected version with its reason otherwise, or, when no stable version is left to try, with `no stable version of plugin "<name>" is published` (`noCompatibleError` in [plugins/select.go](../internal/plugins/select.go)).

### 7.3. Explicit version

`--version V` parses V as a version and requests its original text as the tag (§2.2). It skips the walk and the downgrade guard, so V may be a pre-release, older than the installed version, or of any major. Its dependencies are planned from the contract of V (§8), but its own cluster requirements are first checked in the pipeline, after the dependencies are installed (§10.1, §15.7). Combined with `--use-major`, it lets dependencies cross their majors.

### 7.4. `--all`

`install --all` runs §7.2 for every installed plugin of `<dir>` (§11.3) in name order, each pinned to its own major, or for those of the home root when `<dir>` holds none (`switchToFallbackRoot` in [plugins/update.go](../internal/plugins/update.go)). `--force` applies to each of them.

## 8. Plugin dependencies

`planFor` ([plugins/planner.go](../internal/plugins/planner.go)) resolves the plugin requirements of a candidate's contract into a plan: the dependencies to install or upgrade, each before its dependents. It reads the registry, the cached contracts and the versions that the installed binaries report, and writes nothing. A candidate whose plan cannot be built is rejected with the reason (§7.2).

### 8.1. Reverse conflicts

Every cached contract in `<dir>/cache/contracts/` that lists the candidate's `name` in its `plugins.mandatory` or `plugins.conditional` constrains the candidate: the `version` of the candidate's contract MUST satisfy that constraint, or the candidate is rejected (`reverseConflictReason`, `validatePluginConflict` in [plugins/validators.go](../internal/plugins/validators.go)). An empty constraint fails here (§15.1).

### 8.2. Mandatory dependencies

For each entry of `plugins.mandatory` (`resolveMandatoryDep`):

1. A built-in dependency (§8.4) is satisfied, whatever its constraint.
2. A name already on the path from the requested plugin is a cycle, and the candidate is rejected.
3. The effective version is the planned one, else the version the installed binary reports; a binary whose output does not parse stops the command. When there is an effective version and the entry has no constraint or the version satisfies it, the dependency is satisfied.
4. A planned version that does not satisfy the constraint is a conflict between two dependents, and the candidate is rejected.
5. Otherwise, when the dependency is missing or installed below the constraint, d8 selects a version (`selectDepVersion`): the newest stable version that satisfies the constraint, passes the cluster requirements and whose own dependencies resolve the same way. An installed dependency is never downgraded and stays within its major unless `--use-major` was given. A dependency whose tags answer `404` is not published, which rejects the candidate, and so does finding no version.

The recursion fails with an error beyond a depth of 16 (`maxResolveDepth`).

### 8.3. Conditional dependencies

An entry of `plugins.conditional` applies only when the dependency is installed or planned: then its version MUST satisfy the constraint, or the candidate is rejected (`conditionalReason`). d8 never installs or upgrades a conditional dependency.

### 8.4. Built-in dependencies

`delivery-kit` and `package` are built-in dependencies while no plugin of that name is installed: a mandatory or conditional dependency on them is satisfied without a registry request, and its constraint is not checked (`isBuiltinCommand` in [plugins/builtins.go](../internal/plugins/builtins.go), `registerCommands` in [cmd/d8/root.go](../cmd/d8/root.go)). Once a plugin of that name is installed, it takes over the command (§12.1), and the dependency resolves against it like any other. Only `d8 dist plugins` receives this list, the pre-run gate of the wrapper does not (§15.2).

### 8.5. Execution and matching

The plan is printed and executed before the plugin itself, each dependency through the full pipeline of §10.1 with its own lock, without `--force` and without further planning (`executePlan` in [plugins/install.go](../internal/plugins/install.go)). A failure stops the install, and the dependencies installed before it stay installed.

§8.1 to §8.3 match constraints against raw versions, unlike §9.3, so a version with a pre-release part satisfies only constraints that themselves name a pre-release (§15.6).

## 9. Cluster requirements

### 9.1. Snapshot

When a contract being checked declares a cluster requirement and checks are not skipped (§9.4), d8 takes a snapshot of the cluster, once per command, with three requests of at most 30 s each (`LoadClusterState` in [plugins/requirements/clusterstate.go](../internal/plugins/requirements/clusterstate.go), `clusterState` in [plugins/validators.go](../internal/plugins/validators.go)):

| Fact | Request | Value |
|---|---|---|
| Kubernetes version | `GET /version` | `gitVersion`; unknown when it does not parse |
| Deckhouse version | `get` the Deployment `d8-system/deckhouse` | its annotation `core.deckhouse.io/version`; unknown when absent or not a version |
| Modules | `list` `modules.deckhouse.io/v1alpha1` | per module: enabled when its condition `EnabledByModuleManager` or `EnabledByModuleConfig` is `True`; the version from `properties.version`, unknown when absent or not a version |

All three requests are made whichever requirement is declared, and a failed one fails the snapshot and with it the command: d8 does not install or run a plugin whose requirements it cannot verify (§4, item 2).

### 9.2. Checks

The checks run in this order, and the first failure decides (`Checks` in [plugins/requirements/checks.go](../internal/plugins/requirements/checks.go)):

| Requirement | Met when | Unknown value |
|---|---|---|
| `kubernetes` | the Kubernetes version satisfies the constraint | an error |
| `deckhouse` | the Deckhouse version satisfies the constraint | skipped with a warning, as on `dev` builds |
| `modules.mandatory` | the module is enabled and its version satisfies the constraint | the version check is skipped with a warning |
| `modules.conditional` | the module is not enabled, or its version satisfies the constraint | the version check is skipped with a warning |
| `modules.anyOf`, each group | a member is enabled and, if it has a constraint, has a known version that satisfies it | a member without a version does not count |
| `modules.noneOf`, each group | no member is enabled within its constraint | a member with a constraint and without a version is skipped with a warning |

An unmet requirement makes selection skip the candidate (§7.2) and makes install and run fail. A constraint that does not parse, and an unknown Kubernetes version, are operational errors that stop selection.

### 9.3. Version matching

Cluster versions are normalized before matching (`NormalizedForConstraint` in [plugins/requirements/checks.go](../internal/plugins/requirements/checks.go)): build metadata is dropped, a genuine pre-release part, one that makes a version not stable (§1), is kept, and any other pre-release part is dropped. `v1.28.3-eks-1-30` satisfies `>=1.28`, `v1.30.0-rc.1` does not satisfy `>=1.30`.

### 9.4. Skipping

`--skip-cluster-checks`, or `D8_PLUGINS_SKIP_CLUSTER_CHECKS`, turns the cluster checks off: selection treats every candidate as compatible, and install and run log `skipping cluster-side requirement checks` and go on (`clusterCompatible` in [plugins/select.go](../internal/plugins/select.go), `validateClusterRequirements` in [plugins/validators.go](../internal/plugins/validators.go)). Plugin dependencies are still enforced. `--source` turns it on unconditionally.

## 10. Install and remove

### 10.1. Pipeline

`installPlugin` ([plugins/install.go](../internal/plugins/install.go)) installs the selected version V of major M:

1. Executes the dependency plan (§8.5).
2. Creates `<dir>/plugins/<name>/` and `<dir>/plugins/<name>/v<M>/`.
3. Takes the lock `<dir>/plugins/<name>/install.lock` (§10.3).
4. Unless `--force` is given, probes `v<M>/<name>` (§2.3). If it reports V and `current` points at it, prints `Plugin '<name>' is already at V, nothing to do (use --force to reinstall).` and stops.
5. Fetches the contract of V, prints the banner, and checks every requirement against what is installed: reverse conflicts (§8.1), mandatory and conditional plugin dependencies, cluster requirements (§9). A failure stops here, before anything of `<name>` changes (`validateInstalledRequirements`).
6. If `v<M>/<name>` reports V but `current` points elsewhere, writes the cached contract, repoints `current`, prints `Switched plugin '<name>' to the already-installed V.` and stops. Nothing is downloaded.
7. Downloads to `v<M>/<name>.new`. The staged file is removed on any failure.
8. Runs the smoke test on the staged file (§2.3).
9. Renames the staged file over `v<M>/<name>`.
10. Writes the cached contract (§11.2).
11. Repoints `current` at `v<M>/<name>`.
12. Releases the lock.

### 10.2. Idempotency

V is already installed when the binary of major M reports exactly V (`pluginAlreadyAtVersion`). A binary that reports anything else, a banner or V with a platform suffix (§15.5), is downloaded again by every install. Each major holds one binary: installing a version replaces any other version of the same major, so going back to it downloads it again, while switching between majors only repoints `current` (step 6).

### 10.3. Atomicity and locking

- The download and the smoke test never touch the live binary. The rename of step 9 and the link swap of step 11, a symlink created as `current.new` and renamed over `current`, are atomic, so a plugin that starts meanwhile runs the old or the new binary, never a missing one (`linkCurrent`).
- Within one major, `current` already points at `v<M>/<name>`, so step 9 is the switch, and a failure in step 10 leaves the new binary running with the old cached contract (§15.8). Across majors the switch is step 11, after the cached contract is written.
- The lock is an empty file created with `O_EXCL` (`Acquire` in [lockfile/lockfile.go](../internal/lockfile/lockfile.go)). While it exists, another install of the plugin fails at once with `plugin is locked by: <path>`; a lock older than 1 h counts as left behind by a killed process and is reclaimed with the warning `reclaiming a stale plugin install lock` (`acquireInstallLock`). Selection and planning run without the lock.

### 10.4. Remove

`remove <name>` validates the name and, when `<dir>/plugins/<name>` does not exist, prints `Plugin '<name>' is not installed.` and exits 0. Otherwise it takes the plugin's lock, deletes the directory with every major in it, and deletes `<dir>/cache/contracts/<name>.json` (`Remove` in [plugins/remove.go](../internal/plugins/remove.go)). `remove all` does the same for every directory under `<dir>/plugins`, leftovers of failed installs included, and stops at the first failure (`RemoveAll`). Neither looks at dependents: a plugin whose mandatory dependency was removed fails its next run (§12.2).

## 11. On-disk layout

### 11.1. Install root

- `<dir>` is `--plugins-dir`, else `$DECKHOUSE_CLI_PATH`, else `/opt/deckhouse/lib/deckhouse-cli` ([plugins/flags/flags.go](../internal/plugins/flags/flags.go), `NewRootCommand` in [cmd/d8/root.go](../cmd/d8/root.go)).
- The home root is `$HOME/.deckhouse-cli` (`HomeFallbackPath` in [plugins/layout/layout.go](../internal/plugins/layout/layout.go)).
- The `d8 dist plugins` commands switch to the home root when creating `<dir>/plugins` fails with a permission error (`EnsureInstallRoot` in [plugins/plugins.go](../internal/plugins/plugins.go)). An existing `<dir>/plugins` is used even when it is not writable (§15.10).
- Command registration (§12.1), `install --all` and `d8 dist status` instead use the root that holds installs: `<dir>` when it holds at least one installed plugin, else the home root when that does (`ResolveInstalled` in [plugins/layout/layout.go](../internal/plugins/layout/layout.go)). The two roots are never combined.

### 11.2. Files

```
<dir>/
├── plugins/
│   └── <name>/
│       ├── v<major>/
│       │   ├── <name>           the binary of that major, mode 0755
│       │   └── <name>.new       the staged download, only during an install
│       ├── current              symlink to the absolute path of v<major>/<name>
│       ├── current.new          the staged link, only while current is repointed
│       └── install.lock         only while an install or a remove runs
└── cache/
    └── contracts/
        ├── <name>.json          the contract of the active version, mode 0644
        └── <name>.json.tmp-*    only while it is written
```

- `current` holds an absolute path (`linkCurrent` in [plugins/install.go](../internal/plugins/install.go)), so `<dir>` MUST NOT be moved: every link would dangle.
- The installed major is the `v<N>` directory of the link target; the binary is not run to find it (`installedMajorFromDisk`).
- The cached contract belongs to the version `current` points at and is written atomically, as a temporary file renamed into place (`cacheContract`). It drives reverse conflicts (§8.1), the pre-run gate and the environment of the plugin (§12), and the help of the wrapper.

### 11.3. Installed plugins

A plugin is installed when `<dir>/plugins/<name>/current` exists (`InstalledNames` in [plugins/layout/layout.go](../internal/plugins/layout/layout.go)). A directory without it, left behind by a failed install, is not installed, although `list` shows it (§15.9). Running a plugin resolves the link instead (`checkInstalled` in [plugins/run.go](../internal/plugins/run.go)), so a dangling `current` makes the wrapper install the plugin again (§12.2).

## 12. Running a plugin

### 12.1. Commands

At startup, before flags are parsed, d8 resolves the root that holds installs (§11.1) from `$DECKHOUSE_CLI_PATH` and the home root. It registers each overridable command as the wrapper when a plugin of that name is installed there, and as the built-in otherwise (`registerCommands`, `overridableCommands` in [cmd/d8/root.go](../cmd/d8/root.go)):

| Command | Aliases | Built-in dependency (§8.4) |
|---|---|---|
| `delivery-kit` | `dk` | yes |
| `data` | | |
| `snapshot` | | |
| `iam` | | |
| `network` | `n` | |
| `v` | `virtualization` | |
| `stronghold` | | |
| `package` | | yes |
| `system` | `s`, `p`, `platform` | |

- The plugin name MUST equal the command name. Aliases are not matched, but carry over to the wrapper.
- An installed plugin with any other name gets no command: `d8 <name>` is an unknown command (§15.3), and the binary can only be run as `<dir>/plugins/<name>/current`.
- The help of the wrapper is the description of the cached contract followed by its declared flags and environment variables (`NewPluginCommand` in [plugins/cmd/plugin.go](../internal/plugins/cmd/plugin.go)).
- `--plugins-dir` is parsed too late to affect registration; only `DECKHOUSE_CLI_PATH` can.

### 12.2. Pre-run gate

`RunInstalled` ([plugins/run.go](../internal/plugins/run.go)):

1. If `current` does not resolve, builds the proxy client from the environment, installs the default selection (§7.2), and goes on.
2. Reads the cached contract. Without one the plugin runs ungated and without contract environment; a corrupt one fails with a hint to reinstall with `--force`.
3. Unless the invocation is local, checks every requirement as in step 5 of §10.1. An invocation is local when its first argument is `--help`, `-h`, `--version`, `-v`, `help`, `completion`, `__complete` or `__completeNoDesc`, or when `--help` or `-h` comes before any `--` (`isLocalPluginInvocation`).

The gate runs on every run: a plugin with a cluster requirement makes the three requests of §9.1 each time, and the binary of every mandatory dependency with a constraint is run with `--version`.

### 12.3. Process

- `<dir>/plugins/<name>/current` is executed with every argument after the command name, unparsed. The wrapper has no flags of its own, not even `--help`, so it takes its configuration from the environment only (§6.3).
- The plugin gets d8's environment plus `KUBECONFIG`, set to the kubeconfig path d8 uses, and `PLUGINS_CALLER`, set to the absolute path of the running d8, each only when the `env` of its contract lists it (`pluginRunEnv`). d8 sets no other name, `MODULE_CONFIG_INFO` included; such names pass through when set.
- stdin, stdout and stderr are inherited.
- When d8 receives SIGINT or SIGTERM, the plugin gets SIGTERM, and SIGKILL 10 s later if it is still running (`pluginStopGracePeriod`).
- When the plugin exits with a non-zero status, d8 exits with the same status. When the plugin is killed by a signal, d8 prints `Error: plugin run: signal: <signal>` and exits 1. A failed gate prints `Error: <message>` and exits 1.

## 13. Publishing a plugin

A registry serves a plugin to a client on platform P when:

1. the plugin is at `<root>/deckhouse-cli/plugins/<name>`, where `<root>` is a root the proxy tries (§2.6), and `<name>` matches the pattern of §1;
2. every release is one tag, a version without a pre-release part (§2.2, §15.5, §15.6), that is an image index with a child whose `os` and `architecture` match P, or a single image built for P;
3. the last layer of that image is a gzip-compressed tar holding a regular file with the base name `plugin`, at most 512 MiB long, and the tar decompresses to at most 1 GiB up to the end of that file (§2.3);
4. that file runs on P, and `plugin --version` exits 0 within 10 s printing the tag as a version (§2.3);
5. the image, or the index or each of its children, carries the annotation `contract` with the base64 of a contract that passes §3, whose `name` is `<name>` and whose `version` is the tag (§2.4);
6. every plugin requirement of that contract has a constraint (§15.1);
7. `<root>/deckhouse-cli/plugins` has a tag `<name>`, for `list` with `--source` and for the automatic selection of `d8 mirror pull` (§2.5);
8. a published tag is never moved to another image, since a client that already runs the tag does not download it again without `--force` (§10.2).

To run as `d8 <name>`, `<name>` MUST also be an overridable command (§12.1).

## 14. Example

The registry holds, at the root `registry.example.com/deckhouse` of clusters on `registry.example.com/deckhouse/ee`:

```
registry.example.com/deckhouse/deckhouse-cli/plugins/system:v1.1.0       image
registry.example.com/deckhouse/deckhouse-cli/plugins/system:v1.2.0       index of linux/amd64 and darwin/arm64, contract on the index
registry.example.com/deckhouse/deckhouse-cli/plugins/system:v2.0.0       index, contract requires kubernetes >=1.40
registry.example.com/deckhouse/deckhouse-cli/plugins/system:v2.1.0-rc.1  index
registry.example.com/deckhouse/deckhouse-cli/plugins:system              catalog
```

On a linux/amd64 workstation with `system` v1.1.0 installed in `/opt/deckhouse/lib/deckhouse-cli` and a cluster on Kubernetes 1.30:

```console
$ d8 dist plugins versions system
  v2.1.0-rc.1  newer
  v2.0.0       newer
  v1.2.0       newer
* v1.1.0       current

$ d8 dist plugins install system
Installing plugin: system
Tag: v1.2.0
Plugin: system v1.2.0
Description: Operate system options in DKP
Installing to: /opt/deckhouse/lib/deckhouse-cli/plugins/system/v1/system
Downloading and extracting plugin...
✓ Plugin 'system' successfully installed!
```

The install stays in major 1, where v1.2.0 is the newest stable version and declares no cluster requirement. It sends three requests, which the proxy serves from `registry.example.com/deckhouse/ee/deckhouse-cli/plugins/system` or, when that does not exist, from `registry.example.com/deckhouse/deckhouse-cli/plugins/system` (§2.6):

```
GET /v1/images/deckhouse-cli/plugins/system/tags
GET /v1/images/deckhouse-cli/plugins/system/manifests/v1.2.0
GET /v1/images/deckhouse-cli/plugins/system/images/v1.2.0?platform=linux-amd64
```

Afterwards:

```
/opt/deckhouse/lib/deckhouse-cli/
├── plugins/system/
│   ├── v1/system           reports v1.2.0
│   └── current -> /opt/deckhouse/lib/deckhouse-cli/plugins/system/v1/system
└── cache/contracts/system.json
```

`d8 s status` now executes `/opt/deckhouse/lib/deckhouse-cli/plugins/system/current status`, sending no request. Major 2 is refused while the cluster runs Kubernetes 1.30:

```console
$ d8 dist plugins install system --use-major 2

error: cannot install plugin "system": no installable version

  * v2.0.0: plugin system requires Kubernetes >=1.40, but the cluster runs v1.30.4
  * no version could be installed
    -> inspect a version's requirements: d8 dist plugins contract system
    -> or install an exact version: d8 dist plugins install system --version <version>
  * the search was limited to major 2
    -> pass --use-major to consider another major
```

With `--skip-cluster-checks` the same command downloads v2.0.0 into `v2/system` and repoints `current`. Later installs then stay in major 2, and `d8 dist plugins install system --use-major 1` returns to `v1/system` without a download, printing `Switched plugin 'system' to the already-installed v1.2.0.`

## 15. Known deviations

Verified on 2026-10-01 by running `d8` of this revision against a fake registry-packages-proxy and Kubernetes API. Each item breaks a rule above, a promise of [internal/plugins/README.md](../internal/plugins/README.md), a statement of [self-update.md](self-update.md), or the command help.

1. A plugin requirement without a constraint breaks the dependency. The planner and the mandatory check read an empty constraint as any version (§3.1), but the reverse-conflict check parses it unconditionally (`validatePluginConflict` in [plugins/validators.go](../internal/plugins/validators.go)), and the empty string is not a valid constraint. After the install of a plugin that lists `system` in `plugins.mandatory` without a constraint, which installed `system` as its dependency, `d8 system` failed every run with `plugin conflicts: validate plugin conflict: failed to parse constraint: improper constraint: ""`, and `install system` rejected every version with `reverse conflict: failed to parse constraint`. Removing the dependent restores `system`.
2. A plugin that depends on a built-in command cannot run. `d8 dist plugins` counts `delivery-kit` and `package` as built-in dependencies (§8.4), but `NewPluginCommand` ([plugins/cmd/plugin.go](../internal/plugins/cmd/plugin.go)) never passes that list to the manager of the wrapper, so the pre-run gate looks for an installed `delivery-kit` plugin. A `package` plugin requiring `delivery-kit` installed without one, after which `d8 package` failed every run with `Error: plugin "package" requirements not satisfied`. That message is only the category of the diagnostic: the wrapper prints the error itself (`fmt.Fprintln(os.Stderr, "Error:", err)`), without the causes and fixes that `helpfulError` attaches, so it names neither the missing dependency nor the remedy.
3. Only the nine overridable names can be run, and nothing is installed on first use. README lists `d8 <plugin> ...` as "run an installed plugin; auto-installs it on first use", and the package documentation in [plugins/doc.go](../internal/plugins/doc.go) has an invocation pull a missing plugin. `registerCommands` ([cmd/d8/root.go](../cmd/d8/root.go)) creates wrappers only for overridable names whose plugin is already installed (§12.1): an installed plugin `appa` gave `unknown command "appa" for "d8"`, and without a `system` plugin `d8 system` ran the built-in and sent no request. The install branch of `RunInstalled` is reached only through a dangling `current`: with `v1/network` deleted, `d8 network x` printed `Not installed, installing...` and installed it again.
4. The `403` diagnostic advises a role that does not grant plugin downloads. `d8 dist plugins` advises binding `d8:registry-packages-proxy:packages-download` (`Diagnose` in [plugins/cmd/errdetect/diagnose.go](../internal/plugins/cmd/errdetect/diagnose.go)), a role that grants `deployments/packages`, which the proxy reserves for its `/v1/packages/` routes; `/v1/images/`, plugins included, is authorized with `deployments/cli-binary`, which only `d8:registry-packages-proxy:cli-download` grants (§4). The diagnostic also gives the wrong wait, as in [self-update.md](self-update.md#14-known-deviations) §14.2.
5. Per-platform tags are understood only for display. `SplitPlatform` ([plugins/platform.go](../internal/plugins/platform.go)) recognizes `<version>-<os>-<arch>` in `versions`, `list` and the current marker, while selection, `--version` and the idempotency check use raw tags and versions. With `bar` published only as `v0.0.34-darwin-arm64`, `v0.0.34-linux-amd64` and `v0.0.34-windows-amd64`, `versions bar` listed `v0.0.34  windows/amd64, linux/amd64, darwin/arm64`; `install bar` on linux/amd64 took `v0.0.34-windows-amd64`, the highest by SemVer precedence, and failed its smoke test with `exec format error`; `install bar --version v0.0.34` got `404`. A binary that reports its per-platform version, `v1.0.0-linux-amd64` for the tag `v1.0.0` (the case `InstalledVersionOrNil` strips), never counts as installed, so every install downloads it again.
6. Plugin dependencies are matched against raw versions. Cluster versions are normalized (§9.3), plugin versions are not, so an installed dependency whose version has a pre-release part satisfies no plain range such as `>=1.0.0`. Selection itself treats CI markers as stable (§1): `install lib` took `v1.2.0-main` over `v1.0.0`, and then `install app`, which needs `lib >=1.0.0`, failed with `no version of required plugin "lib" satisfies >=1.0.0`. With a dependency `baz` whose binary reports `v1.0.0-linux-amd64` (item 5), `install qux`, which needs `baz >=1.0.0`, downloaded `baz` again and failed with `baz must satisfy >=1.0.0`.
7. `--version` installs dependencies before it checks the plugin's own cluster requirements. `planForExplicit` ([plugins/install.go](../internal/plugins/install.go)) plans only plugin dependencies, and the cluster requirements of the plugin are first checked in step 5 of §10.1, after the plan ran. `install top --version v1.0.0`, for a `top` that requires Kubernetes `>=1.40` and the plugin `lib`, installed `lib`, then failed with `plugin top requires Kubernetes >=1.40, but the cluster runs v1.30.4`, leaving `lib` installed and an empty `top/v1/` behind. Without `--version`, selection rejects the same contract and nothing is installed.
8. An update within one major switches the binary before the contract is cached. Step 9 renames the new binary over the one `current` points at, and only step 10 writes its contract (§10.3), so a failure in between leaves the new binary running under the old contract, against README's "A failure at any step leaves the previous version installed and working". With `cache/contracts` read-only, updating `lib` from v1.0.0 to v1.1.0 failed with `failed to create temp contract file`, after which `current` reported v1.1.0 and the cached contract still said v1.0.0. The switch of step 6 writes the contract first for exactly this reason.
9. `list` counts leftovers of failed installs. README defines installed as having a `current` link (§11.3), as `install --all`, `d8 dist status` and command registration do, but `fetchInstalledPlugins` ([plugins/list.go](../internal/plugins/list.go)) lists every entry of `<dir>/plugins`. After the failed install of item 5, `list` printed `bar  ERROR  run plugin binary: fork/exec …/plugins/bar/current: no such file or directory` and `Total: 1 plugin(s) installed`, while `d8 dist status` reported none.
10. The home root replaces only a root that cannot be created, and commands disagree on the root. `EnsureInstallRoot` falls back only when creating `<dir>/plugins` fails (§11.1), which an existing directory never does: with `<dir>/plugins` present and read-only, as after an earlier install with `sudo` into `/opt/deckhouse/lib/deckhouse-cli`, the install failed with `failed to create plugin directory: … permission denied`, against README's "if it is not writable, installs fall back to `~/.deckhouse-cli`". With `<dir>` writable but empty and the plugins in the home root, `install --all`, `d8 dist status` and `d8 system` used the home root, while `list` showed `No plugins installed`, `versions lib` marked no current version, `remove lib` printed `Plugin 'lib' is not installed.`, and `install lib` put a second copy into `<dir>`, after which the home root was ignored and `d8 system` ran the built-in again.
11. Cluster checks need more than the download grant. [self-update.md](self-update.md#3-requirements) §3 says that the permission of `cli-download` covers `d8 dist plugins`, but a plugin with any cluster requirement makes all three requests of §9.1, which need `get` on the Deployment `d8-system/deckhouse` and `list` on `modules.deckhouse.io`. A plugin declaring only `kubernetes: >=1.28` failed to install with `cannot reach the cluster to select a compatible version (use --skip-cluster-checks to pick the latest regardless): read deckhouse deployment to determine version: deployments.apps "deckhouse" is forbidden`: a denied read is reported as an unreachable cluster.
12. `d8 dist status` reports updates that the command it suggests does not install. Its `LATEST` column is the newest stable version of any major (`LatestVersion` in [plugins/validators.go](../internal/plugins/validators.go), [self-update.md](self-update.md#101-d8-dist-status) §10.1), and it suggests `d8 dist plugins install <name>` or `install --all`, which stay within the installed major (§7.2). With `lib` v1.1.0 installed and v2.0.0 published, `status` showed `lib  v1.1.0  v2.0.0  update available`, and `install --all` printed `Plugin 'lib' is already at v1.1.0, nothing to do (use --force to reinstall).`
