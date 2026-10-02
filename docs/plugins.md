# `d8 dist plugins`

This document specifies d8 plugins: how a plugin is published in a registry, the contract it declares, what a working setup requires, how d8 reaches the plugin, how `d8 dist plugins` selects a version, resolves dependencies, checks cluster requirements and installs it, where it lives on disk, and how `d8` runs it. It describes the implementation as of 2026-10-02, deckhouse-cli at this revision and the registry-packages-proxy of deckhouse `main`, and every rule names the code that implements it. Where the implementation breaks the contract, §15 lists the deviation.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119. Rules for the publisher apply to anything that puts plugin images into a registry, a plugin's CI or `d8 mirror push`; rules for the client describe what d8 does, rules for the proxy what the registry-packages-proxy does, and rules for the user what the user supplies.

Plugins reach the registry-packages-proxy the same way self-update does. The identity, the endpoint, TLS, the status mapping and the proxy itself are specified in [self-update.md](self-update.md) §4 and §5, and this document repeats them only where plugins depend on them. How `d8 mirror` selects plugins and carries them into an air-gapped registry is described in [internal/mirror/README.MD](../internal/mirror/README.MD) "Plugin Mirroring" and in [mirror-bundle-layout.md](mirror-bundle-layout.md) §6.8 and §8. The package structure is in [internal/plugins/README.md](../internal/plugins/README.md).

- [1. Terms and invariants](#1-terms-and-invariants)
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

## 1. Terms and invariants

### 1.1. Terms

| Term | Meaning |
|---|---|
| Plugin | An executable published as an OCI image under `deckhouse-cli/plugins/<name>`, which d8 installs and runs. It is not part of the d8 binary. |
| Name | The name of the plugin's repository, matching `^[a-z0-9]+(?:[._-][a-z0-9]+)*$` (`ValidatePluginName` in [plugins/layout/layout.go](../internal/plugins/layout/layout.go)). It also names the install directory and, for an overridable command, the command. |
| Proxy | The registry-packages-proxy ([self-update.md](self-update.md#5-the-registry-packages-proxy) §5). |
| C | The cluster repository, which the proxy reads from the Secret `d8-system/deckhouse-registry` (§2.6). |
| `<root>` | A repository under which the proxy looks for plugins: C or the parent of C (§2.6). |
| Platform | `<GOOS>-<GOARCH>` of the running d8, for example `linux-amd64` (`currentPlatform` in [rpp/platform.go](../internal/rpp/platform.go)). |
| Image, index | What a tag resolves to: one image manifest, or an index of per-platform image manifests (§2.3). |
| Version | A tag of the plugin repository that parses as a semantic version (§2.2). |
| Stable version | A version without a pre-release part, or with one whose first dot-separated identifier does not start, case-insensitively, with `alpha`, `beta`, `rc`, `pre` or `snapshot` (`IsGenuinePrerelease` in [plugins/requirements/checks.go](../internal/plugins/requirements/checks.go)). `v1.2.0` and `v1.2.0-main` are stable; `v1.2.0-rc.1` and `v1.2.0-preview` are not. |
| Major | The major number of a version. The installed major is read from the `current` link (§11.2). |
| `<dir>` | The install root (§11.1). |
| Home root | `$HOME/.deckhouse-cli`, the fallback install root (§11.1). |
| Contract | The document a plugin publishes in its `contract` annotation (§3). |
| Cached contract | The contract of the active version, stored at `<dir>/cache/contracts/<name>.json` (§11.2). |
| Transport | How d8 reaches the registry: through the proxy, or directly with the hidden `--source` (§5.1). |
| Cluster requirement | A `kubernetes`, `deckhouse` or `modules` requirement of a contract, checked against the cluster (§9). |
| Overridable command | One of the nine top-level commands that an installed plugin of the same name replaces (§12.1). |
| Wrapper | The command `d8 <name>` that runs an installed plugin in place of an overridable command (§12). |
| Built-in dependency | `delivery-kit` or `package` while d8 serves it with its built-in command: a plugin dependency on that name counts as satisfied (§8.4). |

### 1.2. Invariants

- Source. d8 gets the versions, contracts and binaries of plugins only from the proxy, authenticated with the identity of the current kubeconfig, or with the hidden `--source` directly from a registry (§5).
- Activation. A plugin runs only as `<dir>/plugins/<name>/current`. d8 installs a version only by renaming a smoke-tested binary over the binary of its major and atomically repointing `current`, and removes a plugin only by deleting its directory (§10).
- Verification. A downloaded binary replaces nothing until `<binary> --version`, or `<binary> version`, exited 0 within 10 s (§2.3, §10.1).
- Requirements. d8 checks the requirements of a plugin before every switch to it and before every run that is not local, and a cluster requirement it cannot verify blocks both unless the checks are skipped (§9.4, §10.1, §12.2).
- Locality. Running an installed plugin sends no request to the proxy, unless its binary is gone (§12.2); it reads the cluster only for a cluster requirement (§9.1).

## 2. Registry format

### 2.1. Repository

A plugin `<name>` is the repository `<root>/deckhouse-cli/plugins/<name>`. d8 addresses it by exact name only: through the proxy as the path `deckhouse-cli/plugins/<name>` (`PluginImage` in [rpp/image.go](../internal/rpp/image.go)), and with `--source <repo>` as `<repo>/plugins/<name>` (`pluginClient` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)). A name given on the command line or read from the catalog is checked against the pattern of §1.1 before it reaches a file path or a route; a dependency name read from a contract is not checked (§15.13).

### 2.2. Tags

- Every tag that parses as a semantic version is a version of the plugin. Parsing is lenient (`semver.NewVersion` of Masterminds semver): the `v` prefix is optional and missing minor and patch numbers are 0, so `1.2` is `1.2.0` and `v1` is `1.0.0`, while `V1.2.3` and `latest` are not versions. Tags that are not versions are skipped by selection and listing (`sortedSemverDesc` in [plugins/select.go](../internal/plugins/select.go)).
- d8 requests a version by the original text of its tag, so `--version` MUST be spelled as published: `--version 1.2.0` does not find the tag `v1.2.0`.
- A tag MUST match `^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$` to be downloadable through the proxy (`validateTag` in [rpp/image.go](../internal/rpp/image.go)).
- Versions are ordered by SemVer precedence: a pre-release sorts below its release, and pre-release identifiers compare one by one, alphanumeric ones in ASCII order, so `v1.0.0` > `v1.0.0-windows-amd64` > `v1.0.0-rc.1` > `v1.0.0-linux-amd64`.
- A tag `<version>-<os>-<arch>` whose last two hyphen-separated tokens are a known GOOS and GOARCH is a per-platform tag of `<version>` (`SplitPlatform` in [plugins/platform.go](../internal/plugins/platform.go)). Only `versions` with its current marker, and the `LATEST` column that `list` prints with `--source`, collapse such tags; selection, install, the installed half of `list` and `d8 dist status` treat each as a version of its own (§15.5). Publishers SHOULD publish a release as one tag, a multi-platform index (§2.3).

### 2.3. Image

- A tag MUST resolve to an image or an index in the OCI or Docker v2 format. d8 asks the proxy for these media types (`acceptManifest` in [rpp/client.go](../internal/rpp/client.go)), but the proxy fetches the manifest with its own list, and d8 does not check the type it gets back (`fetchManifest` in [plugins/rpp_source.go](../internal/plugins/rpp_source.go)): a Docker schema 1 tag reads as contract-less and then fails to download.
- A download through the proxy requests the platform of the running d8 (`PullImage` in [rpp/client.go](../internal/rpp/client.go)). The proxy resolves an index to the child with that `os` and `architecture` and answers 404 when the index has none; a single image is served whatever its platform. It returns the last layer of the resolved image, as stored in the registry ([self-update.md](self-update.md#55-platform-and-body) §5.5). With `--source`, d8 pulls the same platform with go-containerregistry and reads the filesystem merged from all layers (`ExtractPlugin` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)).
- The executable is the first regular-file entry whose base name is `plugin`, so `plugin`, `./plugin` and `bin/plugin` match, and a symlink of that name is skipped. Through the proxy it MUST therefore be in the last layer, and that layer MUST be a gzip-compressed tar. The file MUST NOT exceed 512 MiB, and d8 reads at most 1 GiB of decompressed tar in all (`ExtractFileToPath` in [rpp/extract.go](../internal/rpp/extract.go), and the same limits in `extractPluginBinary` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)). The file is written with mode `0755` whatever the archive records. A tar without it fails the install with `file not found in image: "plugin"` through the proxy, and with `binary "plugin" not found in image` with `--source`.
- The executable MUST exit 0 when run with `--version` or, if that fails, with `version`, both runs together within 10 s (`pluginVersionProbe`, `smokeTestPlugin` in [plugins/install.go](../internal/plugins/install.go)). This is the smoke test of every download (§10.1).
- Its stdout for that call, without surrounding white space, MUST be a version equal to the tag (`pluginBinaryVersion` in [plugins/install.go](../internal/plugins/install.go)). d8 takes the installed version from it and from nowhere else. Output that does not parse, or a probe that fails, makes every install download the plugin again (§10.2), disables the downgrade guard (§7.2), shows `ERROR` in `list` and `d8 dist status`, and stops the install of every plugin that depends on it (§8.2).

### 2.4. Contract annotation

- The contract is the manifest annotation `contract`, whose value is the standard base64 encoding, with padding, of the contract document (§3) (`contractAnnotation` in [plugins/rpp_source.go](../internal/plugins/rpp_source.go)).
- For an index, d8 reads the annotation of the index and, when the index has none, the annotation of its first child (`manifests[0]`) and of no other (`contractAnnotation`; `resolveContractAnnotation` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)). A multi-platform plugin MUST carry the contract on the index or, identically, on every child.
- Reading a contract costs one manifest request, two for an index without the annotation; no layer is pulled. Through the proxy, a manifest longer than 1 MiB is cut off and fails to decode (`maxManifestResponseBytes` in [rpp/client.go](../internal/rpp/client.go)).
- An image without the annotation, or with an empty one, is a contract-less plugin: its contract is the name and the tag, with no requirements, env or flags (`GetPluginContract` in [plugins/rpp_source.go](../internal/plugins/rpp_source.go)). An annotation that is not valid standard base64, or a document that fails §3, is an error, and selection stops on it instead of trying an older version (§7.2).

### 2.5. Catalog

- The tags of the repository `<root>/deckhouse-cli/plugins` are plugin names: a tag `<name>` there announces the plugin `<name>` (`pluginCatalog` in [plugins/source.go](../internal/plugins/source.go)). `d8 mirror push` writes such a tag for every plugin it pushes ([mirror-bundle-layout.md](mirror-bundle-layout.md#88-discovery-tags) §8.8).
- Within `d8 dist plugins`, only `--source` reads the catalog, for `list` (`ListPluginNames` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)). The proxy serves no path `deckhouse-cli/plugins` ([self-update.md](self-update.md#53-routes) §5.3), so d8 never asks it, and `list` reports that half as unsupported (`AvailablePlugins` in [plugins/list.go](../internal/plugins/list.go)). The automatic plugin selection of `d8 mirror pull` starts from the catalog too.
- Catalog names are checked like any name, and an invalid one is listed with the note `not a valid plugin name` (`remotePluginInfo` in [plugins/list.go](../internal/plugins/list.go)).

### 2.6. Location

The proxy reads C from the Secret `d8-system/deckhouse-registry` and looks for `deckhouse-cli/plugins/<name>` under C and, when the last segment of C is `ce`, `be`, `se`, `se-plus`, `ee` or `fe` and the host and at least one path segment remain without it, under the parent of C. It tries them in the order of [self-update.md](self-update.md#54-repository-lookup) §5.4, starting with the root that answered last, for the CLI and every plugin alike.

| C | Plugin `<name>` is looked up at |
|---|---|
| `registry.deckhouse.ru/deckhouse/ee` | `registry.deckhouse.ru/deckhouse/ee/deckhouse-cli/plugins/<name>` and `registry.deckhouse.ru/deckhouse/deckhouse-cli/plugins/<name>` |
| `registry.local/mirror` | `registry.local/mirror/deckhouse-cli/plugins/<name>` |
| `registry.local/ee` | `registry.local/ee/deckhouse-cli/plugins/<name>`; the edition has no parent |
| `registry.local/deckhouse/cse` | `registry.local/deckhouse/cse/deckhouse-cli/plugins/<name>`; `cse` is not stripped |

`d8 mirror push` to a target whose last segment is an edition, with a path before it, publishes plugins one level up, at `<target without the edition>/deckhouse-cli/plugins/<name>`, and to any other target at `<target>/deckhouse-cli/plugins/<name>` ([mirror-bundle-layout.md](mirror-bundle-layout.md#82-edition-split) §8.2, §8.10). The proxy finds them there for every edition but `cse`, so a CSE cluster filled by `d8 mirror push` finds no plugin, for the reason given in [self-update.md](self-update.md#14-known-deviations) §14.12.

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
| `name` | MUST equal the repository name. d8 does not check it, but matches the requirements of other plugins against it (§8.1), starts the dependency path and cycle check with it (§8.2), and prints it in the install banner and in diagnostics. Empty means the repository name. |
| `version` | MUST equal the tag. Empty means the tag. Matched against the requirements of other plugins, and then it MUST parse as a version (§8.1). |
| `description` | Shown in the install banner with control characters removed (`printable` in [plugins/install.go](../internal/plugins/install.go)), and verbatim in `list` and in the help of the wrapper (§15.14). |
| `env[].name` | Environment variables the plugin asks d8 to provide (§12.3). |
| `flags[].name` | Flags the plugin accepts. Used only, verbatim, in the help of the wrapper. |
| `requirements.kubernetes.constraint` | Constraint on the Kubernetes version (§9.2). |
| `requirements.deckhouse.constraint` | Constraint on the Deckhouse version (§9.2). |
| `requirements.modules.mandatory[]` | Modules required to be enabled, at a version that satisfies `constraint` when one is given. |
| `requirements.modules.conditional[]` | Modules whose version is checked only when they are enabled. |
| `requirements.modules.anyOf[]` | Named groups, each of which needs an enabled member that satisfies its constraint. |
| `requirements.modules.noneOf[]` | Named groups none of whose members is allowed to be enabled within its constraint; a member without a constraint is forbidden at any version. |
| `requirements.plugins.mandatory[]` | Plugins required to be installed at a version that satisfies `constraint`. d8 installs and upgrades them (§8.2). |
| `requirements.plugins.conditional[]` | Plugins whose version is checked only when they are installed or planned. d8 never installs them (§8.3). |

Constraints use the dialect of Masterminds semver, such as `>=1.2`, `~1.2`, `^1` or `>=1.0 <2.0 || >=3`. Every `constraint` MAY be empty, which matches any version, but a plugin requirement without a constraint breaks the dependency (§15.1).

### 3.2. Decoding

- d8 decodes the annotation from base64, converts it from YAML to JSON with `sigs.k8s.io/yaml`, so a YAML document is accepted too, and unmarshals it with `encoding/json`, which matches keys case-insensitively and ignores unknown ones (`contractFromBytes` in [plugins/rpp_source.go](../internal/plugins/rpp_source.go), `UnmarshalContract` in [pkg/registry/service/contract.go](../pkg/registry/service/contract.go)).
- `requirements.modules` and `requirements.plugins` MUST be objects. The flat arrays of the former v1 schema fail with `decode contract for plugin "<name>": invalid contract: field "requirements.modules" must be an object with mandatory/conditional sections, got a JSON array`.

### 3.3. Validation

`ContractToDomain` ([pkg/registry/service/contract.go](../pkg/registry/service/contract.go)) rejects a contract, and with it the operation, when:

- a module is in both `modules.mandatory` and `modules.conditional`;
- an `anyOf` or `noneOf` group has no name, a name used by another group of the same bucket, no members, a member without a name, a member listed twice, or a member constraint that does not parse;
- a module is in `mandatory` or `conditional` and also in an `anyOf` or `noneOf` group, or is in both an `anyOf` and a `noneOf` group.

A module MAY appear in two groups of the same bucket. Every other constraint is parsed only when it is checked; one that does not parse is then an operational error, except in a cached contract during selection, where it rejects the candidate as a reverse conflict (§8.1, §15.1). The names of plugin requirements are not checked at all (§15.13).

Module requirements MUST name external modules, served from `C/modules/<name>` (§2.6), and not modules embedded in the platform ([internal/plugins/README.md](../internal/plugins/README.md) "Boundaries and deliberate decisions"): the automatic selection of `d8 mirror pull` follows external modules only, and it gates every plugin version on its mandatory modules being in the bundle, reporting an embedded one with `requires module "<name>" which is not in the bundle` (`bundleGate` in [mirror/dist/resolver.go](../internal/mirror/dist/resolver.go)).

## 4. Requirements

Installing a plugin works when items 1 to 4 hold. Running an installed one as `d8 <name>` needs item 5 and, for a cluster requirement, item 2; it needs item 1 only to reinstall a plugin whose binary is gone (§12.2).

1. The identity reaches the proxy as for self-update ([self-update.md](self-update.md#3-requirements) §3, items 1 to 4): a kubeconfig with a bearer token, the ClusterRole `d8:registry-packages-proxy:cli-download`, which covers plugins because they use the same `/v1/images/` routes, `get` on the Ingress `registry-packages-proxy` or an explicit endpoint, and a reachable endpoint with a trusted certificate. Without `--source`, every subcommand but `list` fails at startup without a readable kubeconfig and, unless the endpoint is explicit, a successful discovery, `remove` included (§6.2).
2. For a plugin with a cluster requirement, unless the checks are skipped (§9.4), the identity also needs `get` on the Deployment `deckhouse` in `d8-system` and `list` on `modules.deckhouse.io`; `GET /version` is open to every authenticated identity by default. `cli-download` grants neither (§9.1, §15.11). For example, for a group:

   ```shell
   d8 k create clusterrole d8-plugin-requirements --verb=list --resource=modules.deckhouse.io
   d8 k create clusterrolebinding d8-plugin-requirements --clusterrole=d8-plugin-requirements --group=<group>
   d8 k -n d8-system create role d8-plugin-deckhouse-version --verb=get --resource=deployments --resource-name=deckhouse
   d8 k -n d8-system create rolebinding d8-plugin-deckhouse-version --role=d8-plugin-deckhouse-version --group=<group>
   ```

3. The user can create or write `<dir>/plugins` and `<dir>/cache/contracts`, or creating `<dir>/plugins` fails with a permission error and the same holds under the home root (§11.1, §15.10).
4. The plugin is published as §13 requires, under a `<root>` that the proxy tries (§2.6).
5. `<name>` is one of the nine overridable commands (§12.1), and no executable `kubectl-<name>` is on `PATH` (§15.19).

## 5. Reaching the registry

### 5.1. Transports

`InitPluginServices` ([plugins/init.go](../internal/plugins/init.go)) picks the transport when a command starts:

| | proxy | `--source` |
|---|---|---|
| Selected by | default | the hidden flag `--source <repo>` |
| Plugin repository | `deckhouse-cli/plugins/<name>` under a `<root>` of §2.6 | `<repo>/plugins/<name>` |
| Credentials | the kubeconfig identity | registry credentials (§5.3) |
| Catalog for `list` | not served | read |
| Cluster requirement checks | enforced unless skipped (§9.4) | always skipped |
| `404` on the tags of a dependency | rejects the candidate (§8.2) | stops the command |
| Code | `rppPluginSource` in [plugins/rpp_source.go](../internal/plugins/rpp_source.go), [rpp](../internal/rpp/) | `registryPluginSource` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go) |

`--source` is a temporary escape hatch for environments without a cluster and is meant to be removed (grep marker `legacy --source`).

### 5.2. Through the proxy

Identity, endpoint, TLS, request limits and status mapping are those of [self-update.md](self-update.md#4-reaching-the-proxy) §4, and the defects of the shared proxy client in [self-update.md](self-update.md#14-known-deviations) §14.14 to §14.16 apply to plugins as well. The plugin requests are made by `ListTags`, `GetManifest` and `PullImage` in [rpp/client.go](../internal/rpp/client.go):

| Operation | Request | Used by |
|---|---|---|
| List versions | `GET <endpoint>/v1/images/deckhouse-cli/plugins/<name>/tags`; the body, read up to 4 MiB, MUST be a JSON object whose member `tags` is an array of strings | selection, `versions`, `contract`, dependencies, `d8 dist status` |
| Read a manifest | `GET <endpoint>/v1/images/deckhouse-cli/plugins/<name>/manifests/<ref>`, accepting the OCI and Docker manifest and index media types; the body is read up to 1 MiB | contracts (§2.4) |
| Download | `GET <endpoint>/v1/images/deckhouse-cli/plugins/<name>/images/<tag>?platform=<platform>` | install (§2.3) |

- `<ref>` MUST be a tag that matches the pattern of §2.2, or a digest: a lowercase alphanumeric algorithm, `:` and at least 32 hex digits (`validateRef` in [rpp/image.go](../internal/rpp/image.go)).
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

- `http://` and `https://` are removed wherever they occur in `<repo>`, and so is one trailing `/`; the rest MUST be a valid repository with a host and a path (`validateLegacySource` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)). Plugins are `<repo>/plugins/<name>` and the catalog is `<repo>/plugins`, so `<repo>` names the `deckhouse-cli` repository, such as `registry.deckhouse.ru/deckhouse/deckhouse-cli`.
- Credentials are `--source-login` and `--source-password`, by default `$D8_MIRROR_SOURCE_LOGIN` and `$D8_MIRROR_SOURCE_PASSWORD`; else `--license`, by default `$D8_MIRROR_LICENSE_TOKEN`, as the user `license-token`; else the Docker config entry of the host; else anonymous access (`legacyRegistryAuth`). `--insecure` lets go-containerregistry fall back to HTTP after HTTPS, which it does for loopback and private addresses anyway, and `--tls-skip-verify` skips certificate verification.
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
| `d8 dist plugins versions <name>` | Lists every version, pre-releases included, one line per version with its per-platform tags collapsed into a list of platforms, newest first by the highest tag of each version, so a release published only as per-platform tags can follow its own pre-releases (§15.5). The version the installed binary reports, without a platform suffix, is marked `*` and `current`, newer ones `newer`; an installed version that is not published is reported below the list (`PublishedVersions`, `InstalledVersionOrNil` in [plugins/versions.go](../internal/plugins/versions.go)). |
| `d8 dist plugins contract <name>` | Prints as YAML the contract of the newest stable version, whatever its major and cluster compatibility (`LatestVersion` in [plugins/validators.go](../internal/plugins/validators.go), [plugins/cmd/contract.go](../internal/plugins/cmd/contract.go)). |
| `d8 dist plugins list` | Lists every entry of `<dir>/plugins` with the version its binary reports and the description of its cached contract (§15.9), then the catalog (§2.5). |
| `d8 dist plugins remove <name>`, aliases `uninstall` and `delete` | Removes one plugin (§10.4). A plugin named `all` cannot be removed this way, since `remove all` is a subcommand. |
| `d8 dist plugins remove all` | Removes every plugin (§10.4), ignoring any further argument. |
| `d8 <name> [args]` | Runs an installed plugin, for the nine overridable names only (§12). |

`install` takes exactly one name, or `--all` and no name; `--all` rejects `--version` and `--use-major` (`validateInstallArgs` in [plugins/cmd/install.go](../internal/plugins/cmd/install.go)). These rules are checked after startup (§6.2), so a startup failure is reported first. `versions`, `contract` and `remove` take exactly one name, and `list` takes none.

### 6.2. Startup

Before any subcommand of `d8 dist plugins` but `list` runs, d8 (`PersistentPreRunE` in [plugins/cmd/plugins.go](../internal/plugins/cmd/plugins.go)):

1. takes `<dir>` from `--plugins-dir`;
2. builds the transport (§5.1). A failure ends the command, so without `--source` even `remove`, which does not use the registry, needs a readable kubeconfig and, unless the endpoint is explicit, a successful discovery: a reachable API server and `get` on the Ingress (§4, item 1);
3. creates `<dir>/plugins`, switching to the home root on a permission error (§11.1). A failure here is only a warning.

`list` creates the root first and then builds the transport without failing on it: it prints the installed half, then the reason the registry half is missing, and exits 0 ([plugins/cmd/list.go](../internal/plugins/cmd/list.go)).

### 6.3. Flags and environment

The kubeconfig and proxy flags of `d8 dist` apply to every plugin command ([self-update.md](self-update.md#11-flags-and-environment-variables) §11). The plugin commands add ([plugins/flags/flags.go](../internal/plugins/flags/flags.go)):

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--plugins-dir` | `DECKHOUSE_CLI_PATH` | `/opt/deckhouse/lib/deckhouse-cli` | install root (§11.1); command registration and `d8 dist status` read only the environment |
| `--skip-cluster-checks` | `D8_PLUGINS_SKIP_CLUSTER_CHECKS` | off | skip cluster requirement checks (§9.4) |
| `--source`, `--source-login`, `--source-password`, `--license`, `--insecure`, `--tls-skip-verify` | §5.3 | | hidden, direct registry access (§5.3) |
| `--version`, `--use-major`, `--force`, `--all` (`install` only) | | | §6.1, §7 |

`D8_PLUGINS_SKIP_CLUSTER_CHECKS` is on for `1`, `true`, `TRUE` and `True` only (`skipClusterChecksDefault`). The wrapper parses no flags (§12.3): it reads `KUBECONFIG`, `D8_RPP_ENDPOINT`, `D8_RPP_CA_FILE` and `D8_PLUGINS_SKIP_CLUSTER_CHECKS`, uses the current context of the kubeconfig, and cannot skip verification of the proxy certificate.

### 6.4. Output and exit status

Recognised proxy errors print as the diagnostic of `Diagnose` (§5.2), which `wrapProxyDiagnostics` in [plugins/cmd/plugins.go](../internal/plugins/cmd/plugins.go) applies, and a selection that finds no version prints as the diagnostic of `noCompatibleError` (§7.2, step 5); `execute` in [cmd/d8/root.go](../cmd/d8/root.go) writes both to stderr. Any other error prints as `Error executing command: <error>`. A failed command exits 1, unless its error carries the exit status of a plugin binary whose version probe failed, such as a failed smoke test; d8 then exits with that status (§15.18). `install --all` prints each failed plugin as it goes, proxy errors without their diagnostic, continues with the next, and fails at the end with `failed to update <n> plugin(s): <names>` (`UpdateAll` in [plugins/update.go](../internal/plugins/update.go)). The wrapper exits as §12.3 describes.

### 6.5. Errors and troubleshooting

| Message | Cause | Remedy |
|---|---|---|
| `registry-packages-proxy: unauthorized (401)` | the kubeconfig carries no token the proxy accepts, for example a client-certificate kubeconfig ([self-update.md](self-update.md#52-authentication-and-authorization) §5.2) | use a token kubeconfig (§4, item 1) |
| `registry-packages-proxy: forbidden (403)` | `cli-download` is not bound to the identity, or a denial from before the binding is cached | bind `d8:registry-packages-proxy:cli-download`, not the role the diagnostic names (§15.4); a cached denial clears within 30 s |
| `registry-packages-proxy: plugin or version not found (404)` | the plugin or the tag is not published, is spelled differently, or its index has no image for this platform | check with `d8 dist plugins versions <name>`, and spell `--version` as published (§2.2) |
| `registry-packages-proxy: endpoint discovery via the Kubernetes API failed` | the API server is unreachable or untrusted, or the identity may not `get` the Ingress (§15.4) | fix the cause, or pass `--rpp-endpoint https://registry-packages-proxy.<publicDomain>` |
| `cannot reach the cluster to select a compatible version (…)`, `cannot reach the cluster to verify "<name>" requirements (…)` | the API server is unreachable, or a read of §9.1 is denied (§15.11) | grant the reads (§4, item 2), or skip the checks (§9.4) |
| `cannot install plugin "<name>": no installable version` | every candidate was rejected, each for the reason listed (§7.2) | read the reasons and `d8 dist plugins contract <name>`; pass `--use-major` or `--version` |
| `cannot install plugin "<name>": unresolved dependencies` | a dependency is not published, or no version of it fits (§8.2) | publish it; an installed dependency above the constraint is never downgraded |
| `no stable version of plugin "<name>" is published` | the major has pre-releases only | `--version <pre-release>` |
| `no versions found for major version: <N>` | the pinned major has no versions | `--use-major` with another major |
| `plugin "<name>" has unsatisfied requirements` at install, `plugin "<name>" requirements not satisfied` at run | a mandatory dependency is missing or outside its constraint; at run, also a built-in dependency (§15.2) | `d8 dist plugins install <dependency>` |
| `plugin conflicts: …`, `reverse conflict: …` | an installed plugin constrains this one, or its constraint is empty (§15.1) | install a version that fits, or remove the dependent |
| `read "<name>" contract (reinstall with …)`, `read installed contract "<entry>": …` | a cached contract, or another file in `cache/contracts`, cannot be read (§15.17) | `d8 dist plugins remove <name>`, or delete the stray file |
| `plugin is locked by: <path>` | another install or remove runs, or one was killed less than 1 h ago | wait, or delete the lock when no d8 runs |
| `installed "<name>" binary failed its smoke test: …` | the binary does not run here: another platform (§15.5), missing libraries, a crash, or no exit within 10 s | publish a build for this platform (§13) |
| `failed to create plugin directory: … permission denied` | `<dir>/plugins` exists and is not writable (§15.10) | point `--plugins-dir` or `DECKHOUSE_CLI_PATH` at a writable directory |
| `unknown command "<name>" for "d8"` | `<name>` is not an overridable command (§12.1, §15.3) | run `<dir>/plugins/<name>/current` |
| `Error: flags cannot be placed before plugin name: <flag>` | a flag before the command name, seen by kubectl's plugin handler (§15.19) | put the flags after the command |

`Selected <version> is older than the installed version; keeping the installed version …` and `Plugin '<name>' is already at <version>, nothing to do …` are not errors; `--version` and `--force` override them.

## 7. Version selection

### 7.1. Candidates

The candidates are the versions of the plugin (§2.2), newest first. The default selection considers stable versions only (`stableVersions` in [plugins/select.go](../internal/plugins/select.go)).

### 7.2. Default selection

`InstallPlugin` ([plugins/install.go](../internal/plugins/install.go)) without `--version`:

1. Lists the tags once per plugin and command (`listTags` in [plugins/select.go](../internal/plugins/select.go)). A failed listing is not kept, so an unpublished dependency is asked again for every candidate that needs it.
2. Pins a major. `--use-major N` keeps the versions of major N. Without it, an installed plugin keeps the major its `current` link points at; a fresh install, a `current` that does not resolve, or a link whose target does not parse keeps every major (`inheritInstalledMajor` in [plugins/install.go](../internal/plugins/install.go), §15.15). A major without versions fails with `no versions found for major version: <N>`.
3. Walks the stable versions newest first and takes the first whose contract passes the cluster requirements (§9) and whose dependencies resolve (§8) (`selectCompatible` in [plugins/select.go](../internal/plugins/select.go), `selectTopWithPlan` in [plugins/planner.go](../internal/plugins/planner.go)). Each contract is fetched once per command (`PluginContract` in [plugins/select.go](../internal/plugins/select.go)), and the newer versions it skips are printed with the reason. A contract or cluster read that fails stops the walk with that error instead of settling for an older version. The exception is a reverse conflict that does not parse, which only rejects the candidate (§8.1).
4. Guards against downgrades. Without `--use-major`, when the installed binary reports a version newer than the selection, d8 keeps it, prints `Selected <version> is older than the installed version; keeping the installed version (use --version to downgrade explicitly).` and exits 0 (`installedIsNewerThan` in [plugins/install.go](../internal/plugins/install.go)).
5. When nothing qualifies, fails (`noCompatibleError` in [plugins/select.go](../internal/plugins/select.go)):
   - with `cannot install plugin "<name>": unresolved dependencies` and one entry per missing dependency, when every rejection was a dependency problem;
   - with `cannot install plugin "<name>": no installable version` and every rejected version with its reason, otherwise;
   - with `no stable version of plugin "<name>" is published`, when no stable version was left to try.

### 7.3. Explicit version

`--version V` parses V as a version and requests its original text as the tag (§2.2). It skips the walk and the downgrade guard, so V can be a pre-release, older than the installed version, or of any major. Its dependencies are planned from the contract of V (§8), but its own cluster requirements are first checked in the pipeline, after the dependencies are installed (§10.1, §15.7). With `--version`, `--use-major N` ignores N and only lets dependencies leave their majors (§8.2).

### 7.4. `--all`

`install --all` runs §7.2 for every installed plugin of `<dir>` (§11.3) in name order, each pinned to its own major (but see §15.15), or for those of the home root when `<dir>` holds none (`InstalledPluginNames`, `switchToFallbackRoot` in [plugins/update.go](../internal/plugins/update.go)). `--force` applies to each of them.

## 8. Plugin dependencies

`planFor` ([plugins/planner.go](../internal/plugins/planner.go)) resolves the plugin requirements of a candidate's contract into a plan: the dependencies to install or upgrade, each before its dependents. It reads the registry, the cluster (§9), the cached contracts and the versions that the installed binaries report, and writes nothing. A candidate whose plan cannot be built is rejected with the reason (§7.2). The plan is checked as it grows, not as a whole, so some conflicts surface only when it runs (§15.16).

### 8.1. Reverse conflicts

Every entry of `<dir>/cache/contracts/` is read as a contract, the old contract of the plugin being installed included, and one that cannot be read stops the command (§15.17). Each contract that lists the candidate's `name` in its `plugins.mandatory` or `plugins.conditional` constrains the candidate: the candidate is rejected unless the `version` of its contract satisfies that constraint (`reverseConflictReason` in [plugins/planner.go](../internal/plugins/planner.go), `validatePluginConflict` in [plugins/validators.go](../internal/plugins/validators.go)). During selection, a constraint there that does not parse rejects every candidate (§15.1), and a candidate `version` that does not parse rejects that candidate; at install and at run, both are errors.

### 8.2. Mandatory dependencies

For each entry of `plugins.mandatory` (`resolveMandatoryDep` in [plugins/planner.go](../internal/plugins/planner.go)):

1. A built-in dependency (§8.4) is satisfied, whatever its constraint.
2. A name already on the path from the requested plugin is a cycle, and the candidate is rejected.
3. The effective version is the planned one, else the version the installed binary reports; a dependency binary whose version probe fails or prints no version stops the command. When there is an effective version and the entry has no constraint or the version satisfies it, the dependency is satisfied and is not looked at again for this plan (§15.16).
4. A planned version that does not satisfy the constraint is a conflict between two dependents, and the candidate is rejected.
5. Otherwise, when the dependency is missing or its installed version is outside the constraint, d8 selects a version (`selectDepVersion` in [plugins/planner.go](../internal/plugins/planner.go)): the newest stable version that satisfies the constraint, passes the cluster requirements and whose own dependencies resolve the same way. An installed dependency is never downgraded, so an installed version above the constraint rejects the candidate with `no version of required plugin "<name>" satisfies <constraint>` and the advice to publish one, even when a matching version is published. An installed dependency also stays within its major unless `--use-major` was given. Through the proxy, a dependency whose tags answer `404` is not published, which rejects the candidate; with `--source` it stops the command. Finding no version rejects the candidate too.

The recursion fails with an error beyond a depth of 16 (`maxResolveDepth` in [plugins/planner.go](../internal/plugins/planner.go)).

### 8.3. Conditional dependencies

An entry of `plugins.conditional` applies only when the dependency is installed, or is planned by the time the entry is checked: then the candidate is rejected unless that version satisfies the constraint (`conditionalReason` in [plugins/planner.go](../internal/plugins/planner.go)). The conditional entries of a contract are checked before its mandatory ones, so a version that the mandatory ones plan is first checked in step 5 of §10.1, after the plan ran (§15.16). d8 never installs or upgrades a conditional dependency.

### 8.4. Built-in dependencies

`delivery-kit` and `package` are built-in dependencies unless a plugin of that name is installed in the root that command registration resolves (§11.1), whatever `--plugins-dir` says: a mandatory or conditional dependency on them is then satisfied without a registry request, and its constraint is not checked (`isBuiltinCommand` in [plugins/builtins.go](../internal/plugins/builtins.go), `registerCommands` in [cmd/d8/root.go](../cmd/d8/root.go)). Once a plugin of that name is installed there, it takes over the command (§12.1), and the dependency resolves against it like any other. Only `d8 dist plugins` receives the list of built-in dependencies; the wrapper, its pre-run gate and its reinstall do not (§15.2).

### 8.5. Execution and matching

The plan is printed and executed before the plugin itself, each dependency through the full pipeline of §10.1 with its own lock, without `--force` and without further planning (`executePlan` in [plugins/install.go](../internal/plugins/install.go)). A failure stops the install, and the dependencies installed before it stay installed.

§8.1 to §8.3 match constraints against raw versions, unlike §9.3, so a version with a pre-release part satisfies only constraints that themselves name a pre-release (§15.6).

## 9. Cluster requirements

### 9.1. Snapshot

When a contract being checked declares a cluster requirement and the checks are not skipped (§9.4), d8 takes a snapshot of the cluster with three requests of at most 30 s each (`LoadClusterState` in [plugins/requirements/clusterstate.go](../internal/plugins/requirements/clusterstate.go), `clusterState` in [plugins/validators.go](../internal/plugins/validators.go)):

| Fact | Request | Value |
|---|---|---|
| Kubernetes version | `GET /version` | `gitVersion`; unknown when it does not parse |
| Deckhouse version | `get` the Deployment `d8-system/deckhouse` | its annotation `core.deckhouse.io/version`; unknown when absent or not a version |
| Modules | `list` `modules.deckhouse.io/v1alpha1` | per module: enabled when its condition `EnabledByModuleManager` or `EnabledByModuleConfig` is `True`; the version from `properties.version`, unknown when absent or not a version |

All three requests are made whichever requirement is declared, and a failed one fails the snapshot and with it the command: d8 does not install or run a plugin whose requirements it cannot verify (§4, item 2). A snapshot is kept for the rest of the command, but a failed one is not, so `install --all` tries again for each plugin.

### 9.2. Checks

The checks run in this order, and the first failure decides (`Checks` in [plugins/requirements/checks.go](../internal/plugins/requirements/checks.go)):

| Requirement | Met when | Unknown value |
|---|---|---|
| `kubernetes` | the Kubernetes version satisfies the constraint | an error |
| `deckhouse` | the Deckhouse version satisfies the constraint | skipped with a warning, as on `dev` builds |
| `modules.mandatory` | the module is enabled and its version satisfies the constraint | the version check is skipped with a warning |
| `modules.conditional` | the module is not enabled, or its version satisfies the constraint | the version check is skipped with a warning |
| `modules.anyOf`, each group | a member is enabled and, if it has a constraint, has a known version that satisfies it | a member with a constraint and without a version does not count, without a warning |
| `modules.noneOf`, each group | no member is enabled within its constraint | a member with a constraint and without a version is skipped with a warning |

An unmet requirement makes selection skip the candidate (§7.2) and makes install and run fail. A constraint that does not parse, and an unknown Kubernetes version, are operational errors that stop selection.

### 9.3. Version matching

Cluster versions are normalized before matching (`NormalizedForConstraint` in [plugins/requirements/checks.go](../internal/plugins/requirements/checks.go)): build metadata such as `+k3s1` is dropped, a pre-release part that makes a version not stable (§1.1) is kept, and any other pre-release part is dropped. `v1.28.3-eks-1-30` satisfies `>=1.28`, `v1.30.0-rc.1` does not satisfy `>=1.30`.

### 9.4. Skipping

`--skip-cluster-checks`, or `D8_PLUGINS_SKIP_CLUSTER_CHECKS`, turns the cluster checks off: selection treats every candidate as compatible, and install and run go on, logging `skipping cluster-side requirement checks` for a plugin with a cluster requirement (`clusterCompatible` in [plugins/select.go](../internal/plugins/select.go), `validateClusterRequirements` in [plugins/validators.go](../internal/plugins/validators.go)). Plugin dependencies are still enforced. `--source` always skips the checks (§5.3).

## 10. Install and remove

### 10.1. Pipeline

`installPlugin` ([plugins/install.go](../internal/plugins/install.go)) installs the selected version V of major M:

1. Executes the dependency plan (§8.5).
2. Creates `<dir>/plugins/<name>/` and `<dir>/plugins/<name>/v<M>/`.
3. Takes the lock `<dir>/plugins/<name>/install.lock` (§10.3).
4. Unless `--force` is given, probes `v<M>/<name>` (§2.3). If it reports V and `current` points at it, prints `Plugin '<name>' is already at V, nothing to do (use --force to reinstall).`, or `Plugin '<name>' is already at V; installed its missing dependencies.` when step 1 installed any, and stops.
5. Fetches the contract of V, prints the banner, and checks every requirement against what is installed: reverse conflicts (§8.1), mandatory and conditional plugin dependencies, cluster requirements (§9) (`validateInstalledRequirements`). A failure stops here, before the binary, `current` or the cached contract changes; on a fresh install the directories of step 2 stay behind (§15.9).
6. If `v<M>/<name>` reports V but `current` points elsewhere, writes the cached contract, repoints `current`, prints `Switched plugin '<name>' to the already-installed V.` and stops. Nothing is downloaded.
7. Downloads to `v<M>/<name>.new`. The staged file is removed on any failure.
8. Runs the smoke test on the staged file (§2.3).
9. Renames the staged file over `v<M>/<name>`.
10. Writes the cached contract (§11.2).
11. Repoints `current` at `v<M>/<name>`.
12. Releases the lock.

### 10.2. Idempotency

V is already installed when the binary of major M reports a version equal to V by SemVer precedence, so the `v` prefix and build metadata do not matter (`pluginAlreadyAtVersion` in [plugins/install.go](../internal/plugins/install.go)). A binary that reports anything else, such as `plugin version v1.2.0` or V with a platform suffix (§15.5), is downloaded again by every install. Each major holds one binary: installing a version replaces any other version of the same major, so going back to it downloads it again, while returning to a major whose binary already reports the version selected for it only repoints `current` (step 6).

### 10.3. Atomicity and locking

- The download and the smoke test never touch the live binary. The rename of step 9 and the link swap of step 11, a symlink created as `current.new` and renamed over `current`, are atomic, so a plugin that starts meanwhile runs the old or the new binary, never a missing one (`linkCurrent` in [plugins/install.go](../internal/plugins/install.go)).
- Within one major, `current` already points at `v<M>/<name>`, so step 9 is the switch, and a failure in step 10 leaves the new binary running with the old cached contract (§15.8). Across majors the switch is step 11, after the cached contract is written.
- The lock is an empty file created with `O_EXCL` (`Acquire` in [lockfile/lockfile.go](../internal/lockfile/lockfile.go)). While it exists, another install or remove of the plugin fails at once with `plugin is locked by: <path>`; a lock older than 1 h counts as left behind by a killed process and is reclaimed with the warning `reclaiming a stale plugin install lock` (`acquireInstallLock` in [plugins/install.go](../internal/plugins/install.go)). Selection and planning run without the lock.

### 10.4. Remove

`remove <name>` checks the name and, when `<dir>/plugins/<name>` does not exist, prints `Plugin '<name>' is not installed.` and exits 0. Otherwise it takes the plugin's lock, deletes the directory with every major in it, and deletes `<dir>/cache/contracts/<name>.json` (`Remove` in [plugins/remove.go](../internal/plugins/remove.go)). `remove all` does the same for every directory under `<dir>/plugins`, leftovers of failed installs included, and stops at the first failure (`RemoveAll`). Neither looks at dependents: a plugin whose mandatory dependency was removed fails its next run (§12.2).

## 11. On-disk layout

### 11.1. Install root

- For `d8 dist plugins`, `<dir>` is `--plugins-dir`, else `$DECKHOUSE_CLI_PATH`, else `/opt/deckhouse/lib/deckhouse-cli` ([plugins/flags/flags.go](../internal/plugins/flags/flags.go), `NewRootCommand` in [cmd/d8/root.go](../cmd/d8/root.go)). Command registration (§12.1) and `d8 dist status` take it from `$DECKHOUSE_CLI_PATH` or the default only.
- The home root is `$HOME/.deckhouse-cli` (`HomeFallbackPath` in [plugins/layout/layout.go](../internal/plugins/layout/layout.go)).
- The `d8 dist plugins` commands switch to the home root when creating `<dir>/plugins` fails with a permission error (`EnsureInstallRoot` in [plugins/plugins.go](../internal/plugins/plugins.go)); an existing `<dir>/plugins` is used as it is, even when it is not writable (§15.10).
- Command registration and `d8 dist status` instead use the root that holds installs: `<dir>` when it holds at least one installed plugin, else the home root when that does (`ResolveInstalled` in [plugins/layout/layout.go](../internal/plugins/layout/layout.go)). `install --all` makes the same choice (`switchToFallbackRoot` in [plugins/update.go](../internal/plugins/update.go)). The two roots are never combined.
- On cluster nodes, bashible installs `/opt/deckhouse/bin/d8` setuid root for access to the default root ([self-update.md](self-update.md#14-known-deviations) §14.11).

### 11.2. Files

M stands for a major:

```
<dir>/
├── plugins/
│   └── <name>/
│       ├── v<M>/
│       │   ├── <name>                   the binary of major M, mode 0755
│       │   └── <name>.new               the staged download, only during an install
│       ├── current                      symlink to the absolute path of v<M>/<name>
│       ├── current.new                  the staged link, only while current is repointed
│       ├── install.lock                 only while an install or a remove runs
│       └── install.lock.reclaim.<pid>   only while a stale lock is reclaimed
└── cache/
    └── contracts/
        ├── <name>.json                  the contract of the active version, mode 0644
        └── <name>.json.tmp-*            only while it is written
```

- Directories are created with mode `0755` and the lock with `0644`, both minus the umask; only the binary and the cached contract get their modes forced.
- `current` holds an absolute path (`linkCurrent` in [plugins/install.go](../internal/plugins/install.go)), so `<dir>` MUST NOT be moved: every link would dangle.
- The installed major is the `v<M>` directory of the link target; the binary is not run to find it (`installedMajorFromDisk` in [plugins/install.go](../internal/plugins/install.go)).
- The cached contract belongs to the version `current` points at and is written atomically, as a temporary file renamed into place (`cacheContract` in [plugins/install.go](../internal/plugins/install.go)). It drives reverse conflicts (§8.1), the pre-run gate and the environment of the plugin (§12), and the help of the wrapper.
- Every entry of `cache/contracts/` is read as a contract (§8.1), so a `<name>.json.tmp-*` left behind by a killed install, or any other file there, fails every install and every gated run (§15.17).

### 11.3. Installed plugins

A plugin is installed when `<dir>/plugins/<name>/current` exists, even dangling (`InstalledNames` in [plugins/layout/layout.go](../internal/plugins/layout/layout.go)); `install --all`, command registration and the root choice of `d8 dist status` follow this. A directory without it, left behind by a failed install, is not installed, but `list` and, once the root holds a real install, `d8 dist status` show it (§15.9). Running and updating resolve the link instead (`checkInstalled` in [plugins/run.go](../internal/plugins/run.go)): a dangling `current` makes the wrapper install the plugin again, and both then pick from every major (§12.2, §15.15).

## 12. Running a plugin

### 12.1. Commands

At startup, before flags are parsed, d8 resolves the root that holds installs (§11.1) from `$DECKHOUSE_CLI_PATH`, or the default, and the home root. It registers each overridable command as the wrapper when a plugin of that name is installed there, and as the built-in otherwise (`registerCommands`, `overridableCommands` in [cmd/d8/root.go](../cmd/d8/root.go)):

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
- An installed plugin with any other name gets no wrapper: `d8 <name>` runs the built-in of that name, such as `mirror` or `status`, or is an unknown command (§15.3), and the binary can only be run as `<dir>/plugins/<name>/current`.
- The help of the wrapper is the description of the cached contract, or the built-in's short text without one, followed by the flags and environment variables the contract declares, all verbatim (`NewPluginCommand`, `withContractHelp` in [plugins/cmd/plugin.go](../internal/plugins/cmd/plugin.go), §15.14).
- `--plugins-dir` is parsed too late to affect registration; only `DECKHOUSE_CLI_PATH` can.
- Before any command runs, building `d8 k` lets kubectl's plugin handler replace d8 with an executable `kubectl-<name>` from `PATH` (§15.19).

### 12.2. Pre-run gate

`RunInstalled` ([plugins/run.go](../internal/plugins/run.go)):

1. If `current` does not resolve, builds the proxy client from the environment, installs the default selection as for a fresh install, from every major and without built-in dependencies (§7.2, §8.4, §15.15), and goes on.
2. Reads the cached contract. Without one the plugin runs ungated and without contract environment. Any other read failure, such as an unreadable file, broken JSON or a contract failing §3.3, fails with `read "<name>" contract (reinstall with 'd8 dist plugins install <name> --force'): …`; that reinstall fails on the same file (§8.1, §15.17), so only `remove <name>` or deleting the file recovers.
3. Unless the invocation is local, checks every requirement as in step 5 of §10.1. An invocation is local when its first argument is `--help`, `-h`, `--version`, `-v`, `help`, `completion`, `__complete` or `__completeNoDesc`, or when `--help` or `-h` comes before any `--` (`isLocalPluginInvocation`).

Every invocation that is not local and has a cached contract repeats the checks: a plugin with a cluster requirement makes the three requests of §9.1 each time, and the binary of every installed mandatory or conditional dependency with a constraint is run with `--version`.

### 12.3. Process

- `<dir>/plugins/<name>/current` is executed with every argument after the command name, unparsed. The wrapper has no flags of its own, not even `--help`, so it takes its configuration from the environment only (§6.3).
- The plugin gets d8's environment plus `KUBECONFIG`, set to the kubeconfig path d8 uses, and `PLUGINS_CALLER`, set to the absolute path of the running d8, each only when the `env` of its contract lists it (`pluginRunEnv`). Any other listed name, `MODULE_CONFIG_INFO` included, reaches the plugin only from d8's environment.
- stdin, stdout and stderr are inherited.
- When d8 receives SIGINT or SIGTERM, the plugin gets SIGTERM, and SIGKILL 10 s later if it is still running (`pluginStopGracePeriod`). Only the first signal is handled: a second one terminates d8 at once and leaves the plugin running. A Ctrl-C in the terminal also reaches the plugin directly, through the process group.
- When the plugin exits with a non-zero status, d8 exits with the same status. When the plugin is killed by a signal, d8 prints `Error: plugin run: signal: <signal>` and exits 1, and when it exits 0 after a forwarded SIGTERM, d8 prints `Error: plugin run: context canceled` and exits 1 (§15.18). A failed gate prints `Error: <message>` and exits 1.

## 13. Publishing a plugin

A plugin is fully usable on platform P, through the proxy, with `--source` and by `d8 mirror pull`, when:

1. it is at `<root>/deckhouse-cli/plugins/<name>`, where `<root>` is a root the proxy tries (§2.6), and `<name>` matches the pattern of §1.1;
2. every release is one tag, a version without a pre-release part (§2.2, §15.5, §15.6), that is an index with a child whose `os` and `architecture` match P, or a single image built for P;
3. the last layer of that image is a gzip-compressed tar holding a regular file with the base name `plugin`, at most 512 MiB long, and the tar decompresses to at most 1 GiB up to the end of that file (§2.3);
4. that file runs on P, and `plugin --version` exits 0 within 10 s, printing the tag as a version (§2.3);
5. the image, or the index or each of its children, carries the annotation `contract` with the base64 of a contract that passes §3, whose `name` is `<name>` and whose `version` is the tag (§2.4); without it the plugin installs, but declares nothing;
6. every plugin requirement of that contract has a constraint (§15.1) and a name that matches the pattern of §1.1 (§15.13), and every module requirement names an external module (§3.3);
7. `<root>/deckhouse-cli/plugins` has a tag `<name>`, for `list` with `--source` and for the automatic selection of `d8 mirror pull` (§2.5);
8. a published tag is never moved to another image, since a client that already runs the tag does not download it again without `--force` (§10.2).

To run as `d8 <name>`, `<name>` MUST also be an overridable command (§12.1).

## 14. Example

A cluster on `registry.example.com/deckhouse/ee` reads plugins from the parent root `registry.example.com/deckhouse` (§2.6), which holds:

```
registry.example.com/deckhouse/deckhouse-cli/plugins/system:v1.1.0       image
registry.example.com/deckhouse/deckhouse-cli/plugins/system:v1.2.0       index of linux/amd64 and darwin/arm64, contract on the index
registry.example.com/deckhouse/deckhouse-cli/plugins/system:v2.0.0       index, contract requires kubernetes >=1.40
registry.example.com/deckhouse/deckhouse-cli/plugins/system:v2.1.0-rc.1  index
registry.example.com/deckhouse/deckhouse-cli/plugins:system              catalog
```

On a linux/amd64 workstation with `system` v1.1.0 installed in `/opt/deckhouse/lib/deckhouse-cli`, and a cluster on Kubernetes v1.30.4:

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

The install stays in major 1, where v1.2.0 is the newest stable version and declares no cluster requirement. It sends three requests, which the proxy, finding no `registry.example.com/deckhouse/ee/deckhouse-cli/plugins/system`, serves from `registry.example.com/deckhouse/deckhouse-cli/plugins/system`:

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

`d8 s status` now executes `/opt/deckhouse/lib/deckhouse-cli/plugins/system/current status`, sending no request. Major 2 is refused while the cluster runs Kubernetes v1.30.4:

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

With `--skip-cluster-checks` the same command downloads v2.0.0 into `v2/system` and repoints `current`. From then on `d8 s` fails every run with `Error: kubernetes requirement: plugin system requires Kubernetes >=1.40, but the cluster runs v1.30.4`, and plain installs, pinned to major 2, fail as above, until the cluster runs Kubernetes 1.40 or `D8_PLUGINS_SKIP_CLUSTER_CHECKS=1` is set (§12.2). `d8 dist plugins install system --use-major 1` returns to `v1/system` without a download, printing `Switched plugin 'system' to the already-installed v1.2.0.`

## 15. Known deviations

Verified on 2026-10-01 and 2026-10-02 by running `d8` of this revision against a fake registry-packages-proxy and Kubernetes API, unless an item says otherwise. Each item breaks a rule above, a promise of [internal/plugins/README.md](../internal/plugins/README.md), of the code or of the command help, or a statement of the module documentation, or gives a misleading message.

1. A plugin requirement without a constraint breaks the dependency. The planner and the mandatory and conditional checks read an empty constraint as any version (§3.1), but the reverse-conflict check parses it unconditionally (`validatePluginConflict` in [plugins/validators.go](../internal/plugins/validators.go)), and the empty string is not a valid constraint. After the install of a plugin `app` that lists `system` in `plugins.mandatory` without a constraint, which installed `system` as its dependency, every run of `d8 system` that is not local (§12.2) failed with `plugin conflicts: validate plugin conflict: failed to parse constraint: improper constraint: ""`, and `install system`, with or without `--force` or `--version`, rejected every version with `dependency "app": reverse conflict: failed to parse constraint: improper constraint: ""`, which also calls the dependent a dependency. A conditional requirement without a constraint does the same. Removing the dependent restores `system`.
2. A plugin with a mandatory dependency on a built-in command cannot run. `d8 dist plugins` counts `delivery-kit` and `package` as built-in dependencies (§8.4), but `NewPluginCommand` ([plugins/cmd/plugin.go](../internal/plugins/cmd/plugin.go)) never passes that list to the manager of the wrapper, so the pre-run gate looks for an installed plugin of that name. A `package` plugin that requires `delivery-kit` installed although no `delivery-kit` plugin was present, and then every run of `d8 package` that is not local failed with `Error: plugin "package" requirements not satisfied`; a `system` plugin that requires `package`, served by the built-in, fails the same way. A conditional dependency on a built-in is unaffected. The message is only the category of the diagnostic: the wrapper prints the error itself (`fmt.Fprintln(os.Stderr, "Error:", err)`), without the causes and fixes that `helpfulError` attaches, so it names neither the missing dependency nor the remedy.
3. Only the nine overridable names can be run as `d8 <name>`, and nothing is installed on first use. README lists `d8 <plugin> ...` as "run an installed plugin; auto-installs it on first use" and says that "Auto-install on first use therefore applies only to plugins that are *not* shadowing a built-in", and the package documentation in [plugins/doc.go](../internal/plugins/doc.go) has an invocation pull a missing plugin. `registerCommands` ([cmd/d8/root.go](../cmd/d8/root.go)) creates wrappers only for overridable names whose plugin is already installed (§12.1), so the plugins that README promises to auto-install get no command at all: an installed plugin `appa` gave `unknown command "appa" for "d8"`. The install branch of `RunInstalled` is reached only through a dangling `current`: with `v1/network` deleted, `d8 network x` printed `Not installed, installing...` and installed it again (§15.15).
4. The `403` diagnostic advises a role that does not grant plugin downloads. `d8 dist plugins` advises binding `d8:registry-packages-proxy:packages-download` (`Diagnose` in [plugins/cmd/errdetect/diagnose.go](../internal/plugins/cmd/errdetect/diagnose.go)), which grants `deployments/packages`, reserved for the `/v1/packages/` routes of the proxy, while `/v1/images/`, plugins included, is authorized with `deployments/cli-binary`, which of the proxy's roles only `d8:registry-packages-proxy:cli-download` grants (§4). The diagnostic also gives the wrong wait ([self-update.md](self-update.md#14-known-deviations) §14.2). The discovery and `404` diagnostics share the flaws of self-update.md §14.5 and §14.3: a denied `get` on the Ingress is described as an unreachable API server or an invalid certificate, and a `404` caused by an index without the client's platform as an unpublished plugin. The diagnostics were reproduced against the fake; which role grants what is established from the RBAC templates of deckhouse `main`, not by running.
5. Per-platform tags are understood only for display. `SplitPlatform` ([plugins/platform.go](../internal/plugins/platform.go)) recognizes `<version>-<os>-<arch>` in `versions` and its current marker and in the `LATEST` column that `list` prints with `--source`, while selection, `--version`, the idempotency check, the installed half of `list` and `d8 dist status` use raw tags and versions. With `bar` published only as the single images `v0.0.34-darwin-arm64`, `v0.0.34-linux-amd64` and `v0.0.34-windows-amd64`, `versions bar` listed `v0.0.34` with the platforms `windows/amd64, linux/amd64, darwin/arm64`; `install bar` on linux/amd64 took `v0.0.34-windows-amd64`, the highest by SemVer precedence, and failed its smoke test with `exec format error`; `install bar --version v0.0.34` got `404`; and after `install bar --version v0.0.34-linux-amd64`, every plain `install bar` took the windows tag again and failed. Published as single-child indexes instead, the windows tag fails with `404` for `?platform=linux-amd64`. `versions` also places such a release after its own pre-releases, which sort between the platform tags (§2.2): `v1.0.0-rc.1` came before `v1.0.0` with `linux/amd64, darwin/arm64`. A binary that reports its per-platform version, `v1.0.0-linux-amd64` for the tag `v1.0.0` (the case `InstalledVersionOrNil` strips), never counts as already installed at its tag, so every install downloads it again.
6. Plugin dependencies are matched against raw versions. Cluster versions are normalized (§9.3), plugin versions are not, so an installed dependency whose version has a pre-release part satisfies no plain range such as `>=1.0.0`. Selection itself treats CI markers as stable (§1.1): `install lib` took `v1.2.0-main` over `v1.0.0`, and then `install app`, which needs `lib >=1.0.0`, failed with `no version of required plugin "lib" satisfies >=1.0.0` and the advice to publish one, although v1.0.0 is published and only the no-downgrade rule (§8.2) excludes it. With a dependency `baz` whose binary reports `v1.0.0-linux-amd64` (item 5), `install qux`, which needs `baz >=1.0.0`, downloaded `baz` again and failed with `baz must satisfy >=1.0.0`.
7. `--version` installs dependencies before it checks the plugin's own cluster requirements. `planForExplicit` ([plugins/install.go](../internal/plugins/install.go)) plans only plugin dependencies, and the cluster requirements of the plugin are first checked in step 5 of §10.1, after the plan ran. `install top --version v1.0.0`, for a `top` that requires Kubernetes `>=1.40` and the plugin `lib`, installed `lib`, then failed with `plugin top requires Kubernetes >=1.40, but the cluster runs v1.30.4`, leaving `lib` and an empty `top/v1/` behind; with `lib` v1.0.0 installed and `top` needing `lib >=1.1.0`, it upgraded `lib` before failing. Without `--version`, selection rejects the same contract and nothing changes.
8. An update within one major switches the binary before the contract is cached, and the state does not repair itself. Step 9 of §10.1 renames the new binary over the one `current` points at, and only step 10 writes its contract (§10.3), so a failure in between leaves the new binary running under the old contract, against README's "A failure at any step leaves the previous version installed and working". With `cache/contracts` read-only, updating `lib` from v1.0.0 to v1.1.0 failed with `failed to create temp contract file`, after which `current` reported v1.1.0 and the cached contract still said v1.0.0; with the permissions restored, `install lib` printed `Plugin 'lib' is already at v1.1.0, nothing to do (use --force to reinstall).` and kept the old contract, so only `--force` repairs it. A full disk, or a kill between the two steps, has the same effect. The switch of step 6 writes the contract first for exactly this reason.
9. `list` and `d8 dist status` count leftovers of failed installs. README defines installed as having a `current` link (§11.3), which `install --all` and command registration follow, but `fetchInstalledPlugins` ([plugins/list.go](../internal/plugins/list.go)) lists every entry of `<dir>/plugins`, and `d8 dist status` uses the link only to choose the root before it lists the same way. After the failed install of item 5, `list` printed `bar  ERROR  run plugin binary: fork/exec …/plugins/bar/current: no such file or directory` and `Total: 1 plugin(s) installed`, while `d8 dist status` reported none; next to an installed `lib`, `d8 dist status` showed the leftover of item 7 as `top` with the version `ERROR`, the latest `v1.0.0` and `up to date`.
10. The home root replaces only a root that cannot be created, and commands disagree on the root. `EnsureInstallRoot` falls back only when creating `<dir>/plugins` fails (§11.1), which an existing directory never does. With `<dir>/plugins` present and read-only, as after a `sudo d8 dist plugins install` at the default root, the install failed with `failed to create plugin directory: … permission denied`, against README's "if it is not writable, installs fall back to `~/.deckhouse-cli`". With `<dir>/plugins` present but holding no plugin, and the plugins in the home root, `install --all`, `d8 dist status` and `d8 system` used the home root, while `list` showed `No plugins installed`, `versions lib` marked no current version and `remove lib` printed `Plugin 'lib' is not installed.`. When `<dir>` was writable, `install lib` then put a second copy into it, after which the home root was ignored and `d8 system` ran the built-in again; when it was read-only, the plugins in the home root could be neither updated by name nor removed.
11. A denied cluster read is reported as an unreachable cluster, and no grant documentation mentions the reads. The module documentation says that `cli-download` "grants permissions required to self-update Deckhouse CLI (`d8 cli`) and download plugins (`d8 plugins`)", but a plugin with any cluster requirement makes all three requests of §9.1, which need `get` on the Deployment `d8-system/deckhouse` and `list` on `modules.deckhouse.io` (§4, item 2). A plugin declaring only `kubernetes: >=1.28` failed to install with `cannot reach the cluster to select a compatible version (use --skip-cluster-checks to pick the latest regardless): read deckhouse deployment to determine version: deployments.apps "deckhouse" is forbidden`; installed with `--skip-cluster-checks` as `system`, it failed every run with `Error: cannot reach the cluster to verify "system" requirements (…): … is forbidden` until `D8_PLUGINS_SKIP_CLUSTER_CHECKS=1` was set.
12. `d8 dist status` reports updates that the command it suggests does not install. Its `LATEST` column is the newest stable version of any major, regardless of cluster requirements and dependencies (`LatestVersion` in [plugins/validators.go](../internal/plugins/validators.go), [self-update.md](self-update.md#101-d8-dist-status) §10.1), although the comment of `LatestVersion` claims "the same notion of "latest" that install selection uses", and it suggests `d8 dist plugins install <name>` or `install --all`, which stay within the installed major and check requirements (§7.2). With `lib` v1.1.0 installed and v2.0.0 published, `status` showed `lib` with `v2.0.0` and `update available`, and `install --all` printed `Plugin 'lib' is already at v1.1.0, nothing to do (use --force to reinstall).`; with a v1.2.0 that requires Kubernetes `>=1.40` instead, `status` offered v1.2.0, and `install --all` skipped it.
13. Dependency names from a contract are not checked. §2.1 promises that names are checked before they reach a file path, but `ContractToDomain` checks module groups only, and the names in `plugins.mandatory` and `plugins.conditional` go straight into `<dir>/plugins/<name>/current` (`checkInstalled` in [plugins/run.go](../internal/plugins/run.go), reached through `effectiveVersion` in [plugins/planner.go](../internal/plugins/planner.go) and `validatePluginRequirementMandatory` in [plugins/validators.go](../internal/plugins/validators.go)), which d8 then runs with `--version`. A contract requiring `{name: "../../outside", constraint: ">=1.0"}` made selection and the mandatory check run `<dir>/../outside/current --version`, and the dependency counted as satisfied. With `--source` such a name also reaches the registry path (`pluginClient` in [plugins/source_legacy.go](../internal/plugins/source_legacy.go)); the proxy client rejects it (`PluginImage`). Verified with a test injected into the package.
14. Contract text reaches the terminal unfiltered. The code treats contract text as untrusted, `printable` in [plugins/install.go](../internal/plugins/install.go) strips control characters "so a malicious image cannot smuggle ANSI escapes into the user's terminal", but only the install banner and the dependency names in diagnostics are filtered. `list` prints the cached description as it is (`printInstalledPlugins` in [plugins/cmd/list.go](../internal/plugins/cmd/list.go)), and the wrapper takes its help text, the declared flags and the env names from the cached contract as they are (`NewPluginCommand`, `withContractHelp` in [plugins/cmd/plugin.go](../internal/plugins/cmd/plugin.go)): a description `"\x1b[31mRED\a"` and a flag `"--x\x1b[2J"` came back unchanged from `list` and in `d8 help system`.
15. A dangling `current` drops the major pin. README says that "The major is read from disk (the `current` symlink), so a broken binary cannot drop the pin", but `inheritInstalledMajor` ([plugins/install.go](../internal/plugins/install.go)) asks `checkInstalled`, which resolves the link, and treats a plugin whose binary is gone as a fresh install, while `install --all` and command registration still count it (§11.3). With the tags v1.0.0, v1.1.0 and v2.0.0 and `v1/foo` deleted, `install --all` installed v2.0.0 and repointed `current` to `v2/foo`; the reinstall of the wrapper (§12.2) did the same for `system`.
16. The planner misses conflicts between dependents. It checks each requirement only against the versions known when it reaches it (§8): a dependent already satisfied by the installed version is not checked again when a later one upgrades that dependency, a planned dependent is no reverse conflict, and the conditional entries of a contract are checked before its mandatory ones plan anything (`resolveInto` in [plugins/planner.go](../internal/plugins/planner.go)). With `b` v1.0.0 installed and `a` needing `c` and `d`, where `c` needs `b <1.5.0` and `d` needs `b >=1.5.0`, the plan `c v1.0.0, b v1.5.0, d v1.0.0` was accepted, `c` was installed, and the install failed with `install dependency b v1.5.0: … conflicts with existing plugin c which requires b <1.5.0`, leaving `c` behind. With `a` having the conditional `b >=2.0.0` and the mandatory `c`, which needs `b <2.0.0`, d8 installed `b` v1.5.0 and `c`, then failed with `conditional plugin requirement not satisfied: plugin b v1.5.0 installed but a requires >=2.0.0`. Verified with tests injected into the package.
17. Every entry of the contract cache is read, the old contract of the plugin itself included. Reverse conflicts (§8.1) and the gate read each entry E of `cache/contracts/` as `<E without .json>.json` (`reverseConflictReason` in [plugins/planner.go](../internal/plugins/planner.go), `validatePluginConflicts` in [plugins/validators.go](../internal/plugins/validators.go)), and a read that fails stops the command. A leftover `system.json.tmp-123456` made every gated run of `d8 system` fail with `plugin conflicts: failed to get installed plugin contract: failed to read contract file: open …/system.json.tmp-123456.json: no such file or directory`, and a leftover `lib.json.tmp-999` made `install lib --force` fail with `read installed contract "lib.json.tmp-999": …`. A corrupt `system.json` made `d8 system` advise `reinstall with 'd8 dist plugins install system --force'`, and that reinstall failed with `read installed contract "system": failed to unmarshal contract: …`; only `remove system` recovered. The old contract of a plugin being upgraded constrains its dependencies too: with `a` v1.0.0 installed requiring `b ~1.4.0`, `b` v1.4.0 installed, and `a` v1.1.0 requiring `b ~1.5.0`, `install a` printed `Selected v1.0.0 (newer version(s) skipped: v1.1.0 (dependency "b" (via a -> b): no compatible version (needs ~1.5.0)))`, because every candidate of `b` conflicted with the installed `a`, and kept v1.0.0; `install a --use-major 2` failed with `no version of required plugin "b" satisfies >=2.0.0` and the advice to publish one, although `b` v2.0.0 was published.
18. Exit statuses differ from the documented ones. `execute` ([cmd/d8/root.go](../cmd/d8/root.go)) exits with the `ExitCode()` of any error in the chain, and the version probes wrap the `*exec.ExitError` of the plugin binary (`pluginVersionProbe` in [plugins/install.go](../internal/plugins/install.go)), so a failed smoke test exits with the status of the binary rather than 1, as [self-update.md](self-update.md#14-known-deviations) §14.13 describes for `d8 dist`: the smoke test of a binary that prints `boom` and exits 7 fails with `installed "foo" binary failed its smoke test: run plugin binary: exit status 7 (stderr: boom)`, whose `ExitCode()` is 7, and a binary killed at 10 s gives 255. Failed version reads of installed dependencies do the same. For the wrapper, README promises that "the plugin's exact exit code is propagated", but a plugin killed by a signal makes d8 exit 1, and a plugin that exits 0 after a forwarded SIGTERM makes d8 print `Error: plugin run: context canceled` and exit 1; a second SIGTERM terminated d8 with status 143 and left the plugin running, never killed.
19. An executable `kubectl-<name>` on `PATH` replaces d8's own command. Command registration builds `d8 k` with kubectl's plugin handler and d8's own arguments (`NewKubectlCommand` in [cmd/commands/kubectl.go](../cmd/commands/kubectl.go), `PluginHandler` and `Arguments: os.Args`), so for any first argument that is not a kubectl command, kubectl looks for `kubectl-<arg>[-<arg>…]` on `PATH` and executes it in place of d8. With `kubectl-system` on `PATH`, `d8 system status` ran it although a `system` plugin was installed; `d8 mirror --help` ran a `kubectl-mirror`; and `d8 -h system status` failed with `Error: flags cannot be placed before plugin name: -h`. This affects every top-level command of d8, not only plugins.
