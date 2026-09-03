# d8 mirror Command

## Overview

The `d8 mirror` command facilitates the distribution of Deckhouse Kubernetes Platform to air-gapped or restricted network environments. It enables users to download (pull) the complete Deckhouse distribution bundle from an external registry and upload (push) it to a private container registry.

This functionality is exclusively available to users holding a valid license for any commercial version of the Deckhouse Kubernetes Platform.

**Official Documentation:** https://deckhouse.io/products/kubernetes-platform/documentation/latest/installing/#manual-loading-of-dkp-images-and-vulnerability-db-into-a-private-registry

## License Note

© Flant JSC 2025

The `d8 mirror` functionality requires a valid commercial Deckhouse Kubernetes Platform license.

## Commands

- [`d8 mirror pull`](#d8-mirror-pull) - Download Deckhouse distribution to local filesystem
- [`d8 mirror push`](#d8-mirror-push) - Upload Deckhouse distribution to a third-party registry

---

## d8 mirror pull

### Description

Downloads the Deckhouse Kubernetes Platform distribution bundle to the local filesystem. The bundle contains specific platform releases, modules, and security databases that can later be pushed to an air-gapped container registry.

### Synopsis

```bash
d8 mirror pull <images-bundle-path> [flags]
```

### Arguments

- `<images-bundle-path>` - Path to the directory where the bundle will be stored

### Flags

#### Authentication

| Flag | Short | Environment Variable | Description |
|------|-------|---------------------|-------------|
| `--source` | | | Source registry to pull from (default: `registry.deckhouse.ru/deckhouse/ee`) |
| `--source-login` | | `D8_MIRROR_SOURCE_LOGIN` | Source registry login |
| `--source-password` | | `D8_MIRROR_SOURCE_PASSWORD` | Source registry password |
| `--license` | `-l` | `D8_MIRROR_LICENSE_TOKEN` | Deckhouse license key (shortcut for `--source-login=license-token --source-password=<token>`) |

#### Version Selection

| Flag | Description |
|------|-------------|
| `--since-version` | Minimal Deckhouse release to pull (lower bound, inclusive). Ignored if above current Rock Solid release. Conflicts with `--deckhouse-tag` and `--include-platform` |
| `--include-platform` | Select platform releases by semver constraint (e.g. `">=1.64 <=1.68"`). Uses the same constraint dialect as `--include-module`. Conflicts with `--deckhouse-tag` and `--since-version`. See [Platform Version Filtering](#platform-version-filtering) |
| `--deckhouse-tag` | Specific Deckhouse build to pull. Accepts a **release channel name** (`alpha`, `beta`, `early-access`, `stable`, `rock-solid`, `lts`), a **semver version** (`v1.69.3`), or **any custom tag** (`pr12345`, dev builds). Conflicts with `--since-version` and `--include-platform`. If the registry contains a release channel image for the value, all release channels in the bundle will point to it. See [Release Channel Selection](#release-channel-selection) |
| `--ignore-suspend` | Mirror even when a release channel the request resolves to is marked suspended upstream. Use with caution. See [Suspended Release Channels](#suspended-release-channels) |

#### Module Filtering

| Flag | Short | Description |
|------|-------|-------------|
| `--include-module` | `-i` | Whitelist specific modules. Use one flag per module. Disables `--exclude-module`. See [Module Filtering](#module-filtering) for format details |
| `--exclude-module` | `-e` | Blacklist specific modules. Format: `module-name[@version]`. Use one flag per module. Overridden by `--include-module` |
| `--modules-path-suffix` | | Registry path suffix to read modules from, relative to the source repo. Use `/` to read modules from the repo root (default: `/modules`) |

#### Package Filtering

Packages are mirrored with the same `name[@version-constraint]` dialect as modules (see [Module Filtering](#module-filtering)). See [Package Mirroring](#package-mirroring) for the full behavior.

| Flag | Description |
|------|-------------|
| `--include-package` | Whitelist specific packages. Use one flag per package. Disables `--exclude-package` |
| `--exclude-package` | Blacklist specific packages. Format: `package-name[@version]`. Use one flag per package. Overridden by `--include-package` |

#### Plugin Selection

d8 CLI plugins that the mirrored modules need are selected **automatically** (see [Plugin Mirroring](#plugin-mirroring)); the flag adds more on top.

| Flag | Description |
|------|-------------|
| `--include-plugin` | Mirror a specific plugin in addition to the automatic selection. Format: `plugin-name[@constraint]`, same dialect as `--include-module`. Use one flag per plugin |

#### Component Selection

| Flag | Description |
|------|-------------|
| `--no-platform` | Do not pull Deckhouse Kubernetes Platform into bundle |
| `--no-security-db` | Do not pull security databases into bundle. No effect on editions that ship no security databases. See [Security Databases](#security-databases) |
| `--no-modules` | Do not pull Deckhouse modules into bundle. Overridden by `--include-module` (a whitelist wins; a warning is printed to stderr) |
| `--no-packages` | Do not pull Deckhouse packages into bundle. Note: `package-versions.tar` is produced regardless. See [Package Mirroring](#package-mirroring) |
| `--only-extra-images` | Pull only the extra images (scanners, etc.) of modules **and packages**, without their main images. Also re-enables the modules/packages phase even when `--no-modules`/`--no-packages` is set |
| `--no-installer` | Do not pull the standalone Deckhouse installer (`installer.tar`). Does not affect the installer images embedded in `platform.tar`, which are governed by `--no-platform` |
| `--installer-tag` | Specific Deckhouse installer build tag to pull (default: `latest`) |
| `--skip-vex-images` | Do not pull VEX vulnerability-attestation (`.att`) images. Applies to platform, modules and packages. See [VEX Images](#vex-vulnerability-attestation-images) |

#### Bundle Options

| Flag | Short | Description |
|------|-------|-------------|
| `--images-bundle-chunk-size` | `-c` | Split resulting bundle into chunks of at most N gigabytes (decimal GB = 1000³ bytes; default: 0 = no splitting). Chunk files are named `<name>.tar.NNNN.chunk` |
| `--gost-digest` | | Calculate GOST R 34.11-2012 Streebog-256 digest for each downloaded `.tar` and `.chunk` (writes `.gostsum` files) |
| `--force` | | Overwrite existing bundle packages if they conflict with current pull operation |
| `--no-pull-resume` | | Do not continue last unfinished pull operation; start from scratch. Otherwise an unfinished pull is resumed automatically for 24 hours (resume state is kept per source registry) |
| `--dry-run` | | Print what would be pulled without downloading any images or writing a bundle. See [Dry Run](../../docs/mirror-pull-dry-run.md) |
| `--verbose-summary` | | List every module and package (with resolved versions and VEX counts) in the end-of-pull summary instead of only totals. Output only; does not change what is pulled |

#### Proxy/Cache Registry Discovery

| Flag | Description |
|------|-------------|
| `--proxy-registry` | Pull from a proxy/caching registry that does not implement the registry catalog API. Discovers tags by probing individual semver versions instead of listing. Requires `--include-platform` and/or `--include-module`. See [Proxy Registry Mode](#proxy-registry-mode) for a summary and [PROXY-REGISTRY.md](./PROXY-REGISTRY.md) for the full reference. Cannot be combined with `--deckhouse-tag` or `--since-version` |

#### Connection Options

| Flag | Description |
|------|-------------|
| `--tls-skip-verify` | Disable TLS certificate validation |
| `--insecure` | Interact with registries over HTTP |
| `--tmp-dir` | Path to temporary directory for processing. Ensure sufficient disk space for the entire bundle |

### Platform Version Filtering

The `--include-platform` flag accepts a semver constraint expression to pull only a specific window of platform releases. It uses exactly the same constraint dialect as the version part of `--include-module`.

#### When to use

| Goal | Recommended flag |
|------|-----------------|
| Pull all releases from a given minimum upward | `--since-version 1.64.0` |
| Pull a bounded range (e.g. for an incremental update) | `--include-platform ">=1.64 <=1.68"` |
| Pull every release of a single minor | `--include-platform "~1.65.0"` |
| Pin a single exact release (same as `--deckhouse-tag`) | `--include-platform "=v1.65.3"` |

#### Constraint syntax

The same rules apply as for `--include-module` version constraints (see [Module Filtering](#module-filtering)):

- **Latest-patch-per-minor collapsing** — for semver ranges, only the highest patch in each `(major, minor)` bucket is pulled. Inclusive boundary operators (`>=`, `<=`) always preserve the named version as an anchor even when a newer patch exists.
- **Channel filtering** — release channels whose current snapshot points outside the constraint window are dropped from the bundle so no entry references an image that was not downloaded.
- **Exact-tag form (`=`)** — operationally identical to `--deckhouse-tag`: a single tag is pulled and all default release channels in the bundle are pointed at it.

#### Examples

```bash
# Pull releases v1.64.x through v1.68.x (incremental update scenario)
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --include-platform ">=1.64 <=1.68"

# Pull only the latest patch of v1.65.x
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --include-platform "~1.65.0"

# Pull every minor from v1.65 onward that the registry exposes
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --include-platform "^1.65.0"

# Pin an exact release (all channels point to v1.65.3)
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --include-platform "=v1.65.3"
```

---

### Release Channel Selection

`--deckhouse-tag` is described above as a way to pin a specific build, but the value it accepts has three forms — and two of them are how you mirror **by release channel** instead of by version.

| Value form | Example | What is pulled |
|------------|---------|----------------|
| Release channel name | `--deckhouse-tag stable` | Only the version the named channel currently points at. Valid channels: `alpha`, `beta`, `early-access`, `stable`, `rock-solid`, `lts` |
| Semver version | `--deckhouse-tag v1.69.3` | Only that exact release |
| Any other tag | `--deckhouse-tag pr12345` | That tag as-is (dev/PR builds); it is matched against no channel |

#### Pull a single release channel

To mirror only the current `stable` release — the smallest platform bundle for a single channel:

```bash
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --deckhouse-tag stable
```

A default pull (no version flags) discovers every release between the `rock-solid` and `alpha` channels — see [Platform Release Discovery](#platform-release-discovery) — and downloads all of them. Selecting a channel by name downloads the images of one version only. Channel metadata is still read over the network for every channel, but only the selected channel's images are pulled.

#### Behaviour of tag-pinned pulls

Any `--deckhouse-tag <value>` (and the exact-tag form `--include-platform "=vX.Y.Z"`) changes two things beyond narrowing the version set:

- **All release channels in the bundle are re-pointed to the pinned build.** If the source registry serves a release-channel image for the value, every default channel (`alpha`, `beta`, `early-access`, `stable`, `rock-solid`) in the resulting bundle is aliased to it. After push, a cluster on any channel receives the pinned version. This aliasing only re-tags an already-downloaded image — it pulls no extra data. For a channel-name value this is what makes "`stable` only" work end to end; for a custom tag it happens only when such a release-channel image exists.
- **No `deckhousereleases.yaml` is generated.** The per-version `DeckhouseRelease` manifests are produced only for range/discovery pulls. A tag-pinned bundle relies on the channel aliases above instead of the release manifest.

> The `+channel` suffix of the exact-tag form (e.g. `--include-platform "=v1.65.3+stable"`) restricts channel propagation **for modules**, but has no effect for the platform: a pinned platform tag is always propagated to all default channels, exactly like `--deckhouse-tag`.

#### Suspended Release Channels

A release channel can be marked `suspend: true` in its `version.json` on the source registry (for example while a release is being retracted). By default:

- A pull that **resolves to** a suspended channel is refused with an error, so you never mirror a version upstream has pulled back.
- A channel that is suspended but that your request does **not** resolve to is ignored. A suspended `alpha` does not block `--deckhouse-tag stable`, and a specific `--deckhouse-tag vX.Y.Z` (or a custom tag) that matches no channel is unaffected.

Pass `--ignore-suspend` to mirror regardless. Use with caution — you may distribute a version that is actively being retracted upstream.

#### LTS Channel and CSE Editions

`lts` is a valid release channel in addition to the five default channels, and is the channel the CSE edition ships:

- `--deckhouse-tag lts` mirrors the current LTS release, exactly like any other channel name.
- On editions that ship only `lts` (CSE), the absence of the standard channels (`alpha` … `rock-solid`) is not an error. Registry access validation also falls back from `stable` to `lts` automatically, so a plain `d8 mirror pull` works against a CSE source without extra flags.

#### VEX (Vulnerability Attestation) Images

For each platform image, the pull looks up a matching VEX attestation — an `.att` tag carrying Vulnerability-Exploitability-eXchange data — and, when present, adds it to the bundle. This happens by default. Pass `--skip-vex-images` to exclude these attestations and produce a smaller bundle.

---

### Proxy Registry Mode

`--proxy-registry` adapts the pull command to source registries that act as a transparent proxy or cache in front of another registry and do **not** implement the registry catalog API (`/v2/_catalog`, `/v2/<name>/tags/list`). Instead of calling `ListTags`, the pull walks individual semver tags forward from a starting point you supply via `--include-platform` / `--include-module` and probes each one with a single manifest `HEAD` request.

Minimal example:

```bash
d8 mirror pull /tmp/d8-bundle \
  --source proxy.internal.company.com/deckhouse/ee \
  --license $LICENSE_TOKEN \
  --proxy-registry \
  --include-platform ">=1.64.0 <=1.68.0" \
  --include-module prometheus@^1.0.0
```

Full documentation — end-to-end pipeline diagram, walk algorithm, worked example with a step-by-step HEAD-request trace, HTTP response handling, per-component behaviour table, required flag combinations, performance characteristics, known limitations and more examples — lives in a dedicated document:

**→ [PROXY-REGISTRY.md](./PROXY-REGISTRY.md)**

---

### Module Filtering

The `--include-module` and `--exclude-module` flags support version constraints for fine-grained control over which module versions to include.

#### Filter Format

```
module-name[@version-constraint]
```

#### Version Constraint Types

1. **No version specified** - Includes all versions from all release channels
   ```bash
   --include-module mymodule
   ```

2. **Bare version / caret (^)** - a version given without an operator, or with a caret
   ```bash
   --include-module mymodule@1.3.0    # bare version: >=1.3.0 <2.0.0 (same major line)
   --include-module mymodule@^1.3.0   # caret: >=1.3.0 <2.0.0 for major >= 1
   ```
   - Example: For available versions `v1.0.0, v1.1.0, v1.2.0, v1.3.0, v1.3.3, v1.4.1`, both include `v1.3.3, v1.4.1`.
   - **The bare form is NOT identical to caret for `0.x`:** `mymodule@0.4.0` expands to `>=0.4.0 <1.0.0` (the whole `0.x` line), whereas `^0.4.0` locks the minor (`>=0.4.0 <0.5.0`). For major ≥ 1 the two coincide. Prefer the bare form for step-by-step upgrades that must capture every intermediate minor.
   - Only the highest patch in each `(major, minor)` series is kept, matching the platform release discovery rule. Use the exact-tag form (`=`) to pin a specific older patch.
   - Versions currently pinned by release channels (alpha, beta, early-access, stable, rock-solid, lts) are pulled in addition, regardless of the patch filter.
   - Also pulls current versions from all release channels

3. **Semver tilde constraint (~)** - Patch-level changes only
   ```bash
   --include-module mymodule@~1.3.0
   ```
   - Example: For available versions `v1.0.0, v1.1.0, v1.2.0, v1.3.0, v1.3.3, v1.4.1`
   - Constraint `~1.3.0` (equivalent to `>=1.3.0 <1.4.0`) includes: `v1.3.3` (latest patch of 1.3.x)
   - Also pulls current versions from all release channels

4. **Semver range constraint** - Explicit version range
   ```bash
   --include-module mymodule@">=1.1.0 <1.3.0"
   ```
   - Example: For available versions `v1.0.0, v1.1.0, v1.1.2, v1.2.0, v1.2.4, v1.3.0`
   - Constraint `>=1.1.0 <1.3.0` includes: `v1.1.0, v1.1.2, v1.2.4`
   - The latest patch in each `(major, minor)` is kept; **inclusive boundary operators (`>=`, `<=`) always preserve the named version**, even when a newer patch exists in the same minor. `>=1.1.0` literally means "v1.1.0 OR newer" — the equality is part of the operator.
   - Strict bounds (`>`, `<`) do NOT preserve the named version; they exclude it by definition.
   - Caret (`^`) and tilde (`~`) are syntactic shorthand for a range: their lower bounds are NOT anchors and the latest-patch-per-minor filter applies in full.
   - Anchors that are not present in the registry are silently skipped — this command never invents a tag.
   - Also pulls current versions from all release channels

5. **Exact tag match (=)** - Single specific version
   ```bash
   --include-module mymodule@=v1.3.0
   ```
   - Includes only `v1.3.0`
   - Publishes to all release channels (alpha, beta, early-access, stable, rock-solid, **and `lts`**)

6. **Exact tag with custom build identifier**
   ```bash
   --include-module mymodule@=bobV1
   ```
   - Includes only the `bobV1` tag
   - Publishes to all release channels

7. **Exact tag with specific channel**
   ```bash
   --include-module mymodule@=v1.3.0+stable
   ```
   - Includes only `v1.3.0`
   - Publishes only to the `stable` channel
   - An unrecognized channel in the suffix (e.g. `+bogus`) is ignored: the pin falls back to publishing to all channels, as in items 5–6.

#### Pinning several versions at once

Repeating `--include-module` for the **same** module OR-combines the constraints, so you can pin multiple versions in one pull:

```bash
--include-module mymodule@=v1.3.0 --include-module mymodule@=v1.4.0   # pulls both
```

When more than one exact tag is pinned for the same module, the "publish to all channels" behavior of a single pin no longer applies — only tags that name their own `+channel` suffix are aliased to a channel; the rest are pulled as plain tags. The same rule applies to `--include-package`.

#### Examples

```bash
# Download all modules
d8 mirror pull /tmp/d8-bundle --license $LICENSE_TOKEN

# Download a bounded range of platform releases (e.g. v1.64.x – v1.68.x)
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --include-platform ">=1.64 <=1.68"

# Download only specific modules
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --include-module neuvector \
  --include-module prometheus

# Download module with version constraint
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --include-module neuvector@^1.2.0

# Combine platform range with a module filter
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --include-platform ">=1.64 <=1.68" \
  --include-module neuvector

# Exclude specific modules
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --exclude-module module1 \
  --exclude-module module2

# Download only platform and security databases (no modules)
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --no-modules

# Download specific Deckhouse version
d8 mirror pull /tmp/d8-bundle \
  --license $LICENSE_TOKEN \
  --deckhouse-tag v1.59.0
```

---

### Package Mirroring

Packages are a component mirrored much like modules, but under the `packages/` registry segment. They are selected with `--include-package` / `--exclude-package` (same `name[@version-constraint]` dialect as [Module Filtering](#module-filtering)) and skipped with `--no-packages`. A default pull mirrors **all** packages the source registry exposes.

Registry layout (under the edition root, e.g. `registry.deckhouse.ru/deckhouse/fe`):

| Path | Contents |
|------|----------|
| `<root>/packages/<name>:<version>` | Main package image |
| `<root>/packages/<name>/version:<channel\|vX.Y.Z>` | Release-channel and per-version release metadata (segment is `version`, not modules' `release`) |
| `<root>/packages/<name>/extra/<image>:<tag>` | Extra images (listed in the release image's `extra_images.json`) |

**Bundle output:** one `package-<name>.tar` per package that pulled at least one image, plus a single `package-versions.tar`.

**`package-versions.tar` is produced on every pull** — regardless of `--no-packages` or the include/exclude filter. It clones the full package release-metadata catalog (all package names, every `version:` tag) so the bundle's package release metadata stays in sync. Do not rely on `--no-packages` to suppress it.

Differences from modules:

- There is no `--packages-path-suffix`; the `packages/` segment is fixed.
- `--only-extra-images`, `--skip-vex-images`, `--dry-run` and `--verbose-summary` apply to packages exactly as to modules.
- Exact-tag pins and multi-version pinning follow the same channel-alias rules as modules (see [Pinning several versions at once](#pinning-several-versions-at-once)).

If the source registry has no `packages` repository (some public/CE registries), the package phase and `package-versions.tar` are skipped with a warning instead of failing the pull.

---

### Plugin Mirroring

d8 CLI plugins are standalone binaries the CLI installs through the in-cluster registry-packages-proxy. A plugin declares its requirements (Deckhouse modules, other plugins, platform versions) in a contract - a base64-JSON annotation on its image manifest. The plugins phase runs **last**, after modules and packages, because it resolves against what the earlier phases actually put into the bundle.

Selection principle: **nothing extra**, with one standing exception. A plugin enters the bundle when:

- **the platform is mirrored** - `package` and `system` ship with the platform rather than with any module, so mirroring the platform mirrors them too, unconditionally and regardless of `--include-plugin`. A bundle without them can install the platform but not operate it. The version still has to be one the bundled platform can run (its `deckhouse` constraint is checked like any other), and a registry that does not publish them, or denies access, yields a warning rather than a failed pull. The list lives in `PlatformPlugins` (`internal/mirror/plugins/resolver.go`);
- a mirrored module needs it - the plugin's contract names that module in its `mandatory` or `anyOf` requirements. For **each bundled version** of the module, the newest plugin version whose contract the bundle satisfies is picked (so a bundle carrying module v1.0.0 and v1.5.0 may get two plugin versions, deduplicated);
- it is a mandatory plugin dependency of another selected plugin (resolved recursively; a version already picked is shared when it satisfies the constraint). A dependency whose name matches a **built-in d8 command** (`delivery-kit`, `package`) is mirrored too when the registry publishes it - the built-in is only the fallback, because an air-gapped cluster cannot fetch the plugin later and, once installed, it takes the command over. When it is not published the built-in satisfies the dependency and only a note is recorded, so the dependent is never dropped over it;
- the user names it with `--include-plugin` (additive; an unmet explicit include fails the pull, unlike the automatic selection which skips with a reason in the summary).

Registry layout (at the **bare root**, outside the edition segment - like the installer):

| Path | Contents |
|------|----------|
| `<root>/deckhouse-cli/plugins` | Plugin catalog; its tags are plugin names |
| `<root>/deckhouse-cli/plugins/<name>:<vX.Y.Z>` | One plugin version - a multi-platform OCI index (linux/darwin/windows) |

**Bundle output:** one `plugin-<name>.tar` per plugin. Multi-platform indexes are stored whole, so every platform binary and the contract annotation reach the target registry exactly as published.

What is checked at mirror time vs install time:

- Mirror verifies that each pulled plugin version's module requirements are satisfiable by **at least one bundled version** of each required module, and its `deckhouse` constraint by a bundled platform version.
- `kubernetes` and `noneOf` requirements are cluster-side: `d8 plugins install` enforces them on the target cluster as usual.
- Conditional requirements never gate mirroring; conflicts show up as warnings.

After `d8 mirror push`, `d8 plugins install <name>` works in the air-gapped cluster through the registry-packages-proxy. Note the proxy serves plugins **by exact name** - listing the plugin catalog through it is not supported (a registry-packages-proxy limitation, not a bundle one; `crane ls <target>/deckhouse-cli/plugins` shows the names).

If the source registry has no `deckhouse-cli/plugins` catalog, or denies access to it (HTTP 401/403 - token-auth registries answer this way for any path outside the license's scope, published or not), the automatic selection is skipped quietly. Explicit `--include-plugin` entries still resolve against their own repositories, and a denied explicit include fails the pull.

---

### Security Databases

The security phase mirrors four Trivy databases into `security.tar`, each pinned at a fixed schema tag under `<root>/<edition>/security/`:

| Database | Tag | Contents |
|----------|-----|----------|
| `trivy-db` | `2` | Main Trivy vulnerability database |
| `trivy-bdu` | `1` | Russian BDU (FSTEC) vulnerability feed |
| `trivy-java-db` | `1` | Java package index |
| `trivy-checks` | `0` | Misconfiguration policy checks |

These tags are rolling schema pointers, not release versions: a pull always fetches their current contents, and the set is **not** affected by `--deckhouse-tag`, `--include-platform`, `--since-version` or any release channel. Security-database freshness is therefore independent of which platform versions you mirror.

**Edition availability.** Availability is detected automatically by probing `trivy-db:2`. Editions that do not ship security databases (e.g. CE, BE, SE) return "not found", so the whole phase is skipped silently — no `security.tar` is written, no error is raised, and `--no-security-db` has nothing to skip. EE/FE ship the databases.

**Partial sets.** Only `trivy-db` gates the phase; the other three are best-effort, so a missing `trivy-bdu`/`trivy-java-db`/`trivy-checks` tag is skipped rather than failing the pull. The end-of-pull summary reports `N/4 databases` and flags the line when `N < 4`.

---

### Environment Variables

Additional configuration options are available through environment variables:

| Variable | Description |
|----------|-------------|
| `SSL_CERT_FILE` | Path to SSL certificate. If set, system certificates are not used |
| `SSL_CERT_DIR` | Colon-separated list of directories to search for SSL certificate files. If set, system certificates are not used. More info: https://docs.openssl.org/1.0.2/man1/c_rehash/ |
| `HTTP_PROXY` | URL of proxy server for HTTP requests to hosts not listed in `NO_PROXY` |
| `HTTPS_PROXY` | URL of proxy server for HTTPS requests to hosts not listed in `NO_PROXY` |
| `NO_PROXY` | Comma-separated list of hosts to exclude from proxying. Supports IP addresses, CIDR notations, domains, and asterisk. Domains match name and subdomains. A single `*` disables all proxying |
| `D8_MIRROR_TIMEOUT` | Per-request timeout for registry operations, as a Go duration (e.g. `30s`, `5m`). Applies to all pull operations, including platform. A negative or unset value leaves the client default in place |

### Bundle Structure

The pull command creates a bundle with the following structure:

```
<images-bundle-path>/
├── platform.tar              # Platform images incl. embedded install/install-standalone (if not --no-platform)
├── installer.tar             # Standalone Deckhouse installer (if not --no-installer)
├── security.tar              # Security databases: trivy-db/bdu/java-db/checks (if available for the edition)
├── module-<name>.tar         # One archive per module (if not --no-modules)
├── package-<name>.tar        # One archive per package (if not --no-packages)
├── package-versions.tar      # Package release-metadata catalog (always produced)
├── plugin-<name>.tar         # One archive per d8 CLI plugin the bundle needs (see Plugin Mirroring)
└── <name>.tar.NNNN.chunk     # Chunk parts, when --images-bundle-chunk-size is set (NNNN = 0000, 0001, …)
```

When a component is chunked, its `<name>.tar` is written as a sequence of `<name>.tar.NNNN.chunk` parts (4-digit, zero-padded, starting at `0000`) instead of a single `.tar`; `d8 mirror push` reassembles them transparently.

If `--gost-digest` is specified, a GOST R 34.11-2012 Streebog-256 `.gostsum` file is written next to every `.tar` **and** every `.chunk`.

### Summary

After a pull, `d8 mirror` prints a framed summary of what was pulled (platform, installer, security databases, modules, packages) and the bundle size.

When `--modules-path-suffix` moves modules off the default `modules/` and modules were actually pulled, the summary warns about the non-default modules path, highlighting it with a hint of the standard path so a non-standard layout is easy to spot. A default path, or a moved path that no module went through, produces no warning.

---

## d8 mirror push

### Description

Uploads a previously downloaded Deckhouse Kubernetes Platform distribution bundle to a third-party container registry. This is typically used to populate an air-gapped registry with Deckhouse images.

Before uploading, push runs a write-access check against the target repository (15-second timeout, or `D8_MIRROR_TIMEOUT`); set `MIRROR_BYPASS_ACCESS_CHECKS=1` to proceed even if that check fails. Chunked packages (`<name>.tar.NNNN.chunk`) are reassembled transparently, and each image upload is retried up to 4 times. After the layouts are pushed, small discovery-index tags are created at `<repo>/modules:<module>` and `<repo>/packages:<package>` so the mirrored modules and packages can be enumerated by tag listing.

### Synopsis

```bash
d8 mirror push [images-bundle-path] <registry> [flags]

# Push individual package files instead of a bundle directory:
d8 mirror push <registry> --file platform.tar --file module-foo.tar [flags]
```

### Arguments

- `<registry>` - Target registry address, always the **last** positional argument (format: `registry-host[:port]/path`, e.g. `registry.corp.local:5000/deckhouse`). `http://`/`https://` prefixes are stripped; a bare registry root with no path is rejected; the repository path must be 2–255 characters.
- `[images-bundle-path]` - **Optional** path to the bundle directory (or a single tar/chunked package) created by `d8 mirror pull`. May be omitted when packages are supplied via `--file`.

### Flags

#### Authentication

| Flag | Short | Environment Variable | Description |
|------|-------|---------------------|-------------|
| `--registry-login` | `-u` | `D8_MIRROR_REGISTRY_LOGIN` | Username to log into the target registry |
| `--registry-password` | `-p` | `D8_MIRROR_REGISTRY_PASSWORD` | Password to log into the target registry. Requires `--registry-login` to be set as well |

#### Connection Options

| Flag | Description |
|------|-------------|
| `--tls-skip-verify` | Disable TLS certificate validation |
| `--insecure` | Interact with registries over HTTP |
| `--tmp-dir` | Path to temporary directory for processing. Ensure sufficient disk space for the entire bundle |

#### Package Selection

| Flag | Description |
|------|-------------|
| `--file` | Path to a single tar or chunked package to push. May be repeated. Can replace the `[images-bundle-path]` argument or be combined with it |

#### Other Options

| Flag | Description |
|------|-------------|
| `--modules-path-suffix` | Registry path suffix to push modules to, relative to the target repo. Use `/` to push modules to the repo root (default: `/modules`) |

### Examples

```bash
# Push bundle to private registry with authentication
d8 mirror push /tmp/d8-bundle registry.company.com/deckhouse \
  --registry-login admin \
  --registry-password secretpassword

# Push to registry over HTTP (insecure)
d8 mirror push /tmp/d8-bundle registry.local:5000/deckhouse \
  --insecure

# Push with custom temporary directory
d8 mirror push /tmp/d8-bundle registry.company.com/deckhouse \
  --registry-login admin \
  --registry-password secretpassword \
  --tmp-dir /mnt/large-disk/tmp
```

### Summary

After a push, `d8 mirror` prints a framed summary of what was written to the target registry (platform, installer, security databases, module and package counts).

When `--modules-path-suffix` moves modules off the default `modules/` and modules were actually pushed, the summary warns about the non-default modules path, highlighting it with a hint of the standard path. A default path, or a moved path that no module went through, produces no warning.

### Environment Variables

The same environment variables used by `d8 mirror pull` are also supported:

| Variable | Description |
|----------|-------------|
| `SSL_CERT_FILE` | Path to SSL certificate |
| `SSL_CERT_DIR` | Colon-separated list of directories for SSL certificates |
| `HTTP_PROXY` | HTTP proxy server URL |
| `HTTPS_PROXY` | HTTPS proxy server URL |
| `NO_PROXY` | Comma-separated list of hosts to exclude from proxying |
| `D8_MIRROR_TIMEOUT` | Per-request timeout for registry operations, as a Go duration (e.g. `30s`, `5m`). Also caps the pre-push access check |
| `MIRROR_BYPASS_ACCESS_CHECKS` | Set to `1` to continue the push even when the registry write-access pre-check fails |

---

## Complete Workflow Example

### Scenario: Air-Gapped Installation

1. **On a machine with internet access**, download the bundle:
   ```bash
   # Full pull from a minimum version
   d8 mirror pull /tmp/deckhouse-bundle \
     --license $DECKHOUSE_LICENSE_TOKEN \
     --since-version 1.59.0 \
     --include-module prometheus \
     --include-module ingress-nginx \
     --images-bundle-chunk-size 10

   # Or pull only a specific version window for an incremental update
   d8 mirror pull /tmp/deckhouse-bundle \
     --license $DECKHOUSE_LICENSE_TOKEN \
     --include-platform ">=1.64 <=1.68" \
     --include-module prometheus \
     --include-module ingress-nginx
   ```

2. **Transfer the bundle** to the air-gapped environment (USB drive, secure file transfer, etc.)

3. **On a machine in the air-gapped environment**, push to the internal registry:
   ```bash
   d8 mirror push /tmp/deckhouse-bundle registry.internal.company.com/deckhouse \
     --registry-login admin \
     --registry-password admin_password
   ```

4. **Configure Deckhouse** to use the internal registry during installation

---

## Troubleshooting

### Common Issues

#### Insufficient Disk Space

**Problem:** Pull operation fails due to lack of disk space.

**Solution:** 
- Use `--tmp-dir` to specify a location with more space. By default the temp directory is `<images-bundle-path>/.tmp`, i.e. on the **same volume as the bundle** — point `--tmp-dir` elsewhere if that volume is small
- Use `--images-bundle-chunk-size` to split the bundle into smaller chunks
- Use component flags (`--no-modules`, `--no-security-db`) to reduce bundle size

#### TLS Certificate Errors

**Problem:** TLS certificate validation failures when connecting to registries.

**Solution:**
- Use `--tls-skip-verify` to disable certificate validation (not recommended for production)
- Set `SSL_CERT_FILE` or `SSL_CERT_DIR` to provide custom certificates
- Ensure system certificates are up to date

#### Authentication Failures

**Problem:** Unable to authenticate to source or target registry.

**Solution:**
- Verify credentials are correct
- For Deckhouse registry, use `--license` flag with your license token
- Check that the license is valid and not expired
- Ensure network access to the registry

#### Pull Resume Issues

**Problem:** Pull operation keeps retrying from scratch.

**Solution:**
- The tool automatically resumes incomplete pulls within 24 hours
- Use `--no-pull-resume` to force starting from scratch
- Use `--force` to overwrite conflicting bundle packages

### Debug Logging

Enable debug logging to troubleshoot issues via the `MIRROR_DEBUG_LOG` environment variable (an integer, higher = more verbose):

| Level | Output |
|-------|--------|
| `0` | No debug logging (default) |
| `1` | Image-transfer progress to stderr |
| `2` | Level 1 + registry-client warnings to stderr |
| `3` | Level 2 + mirror debug messages to stdout |
| `4` | Level 3 + full registry requests/responses to stderr |

```bash
export MIRROR_DEBUG_LOG=3

d8 mirror pull /tmp/bundle --license $LICENSE_TOKEN
```

> Note: `D8_LOG_LEVEL` does **not** control mirror verbosity — use `MIRROR_DEBUG_LOG`.

---

## Best Practices

### Performance Optimization

1. **Use Local Storage:** Ensure `--tmp-dir` points to fast local storage (SSD preferred)
2. **Network Bandwidth:** Large bundles can be several gigabytes; ensure adequate bandwidth
3. **Parallel Operations:** The tool automatically parallelizes image pulling
4. **Chunking:** For very large bundles or size-constrained transfers, use `--images-bundle-chunk-size`

### Security Considerations

1. **Protect Credentials:** Use environment variables for sensitive values
2. **Secure Transfer:** Use encrypted channels when transferring bundles
3. **Verify Integrity:** Use `--gost-digest` to generate checksums for bundle verification
4. **TLS Validation:** Avoid `--tls-skip-verify` in production environments

### Version Management

1. **Use `--since-version`:** To pull all releases from a given minimum upward, reducing bundle size relative to a full pull
2. **Use `--include-platform`:** To pull a bounded window of releases (e.g. `">=1.64 <=1.68"`) — useful for staged incremental updates where you want to move up a few minors at a time without pulling the entire channel history
3. **Specific Versions:** Use `--deckhouse-tag` (or `--include-platform "=vX.Y.Z"`) for fully controlled, single-release deployments
4. **Module Versioning:** Leverage semver constraints in `--include-module` for flexible module version management; `--include-platform` speaks the same dialect
5. **Release Channels:** Pulling respects all configured release channels (alpha, beta, early-access, stable, rock-solid, lts); channels whose snapshot falls outside an `--include-platform` constraint are automatically excluded from the bundle

### Bundle Management

1. **Regular Updates:** Periodically pull new bundles to stay current with security patches
2. **Version Control:** Track which bundle versions are deployed in which environments
3. **Storage Planning:** Account for bundle size growth over time (multiple versions, modules)
4. **Cleanup:** Remove old bundles after successful deployment to free space

---

## Architecture Notes

### Bundle Components

The mirror system handles these component types, each packed into its own archive(s):

1. **Platform images** (`platform.tar`) — core platform containers, plus the edition-scoped `install` and `install-standalone` installer images.
2. **Standalone installer** (`installer.tar`) — the non-edition-scoped `installer` image, controlled by `--installer-tag` / `--no-installer`.
3. **Modules** (`module-<name>.tar`) — optional modules with versioned releases; see [Module Filtering](#module-filtering).
4. **Packages** (`package-<name>.tar`, plus the always-present `package-versions.tar`) — see [Package Mirroring](#package-mirroring).
5. **Security databases** (`security.tar`) — four Trivy databases; see [Security Databases](#security-databases).

For each module and package version, the pull additionally fetches release-metadata images, images referenced by the release image's `images_digests.json`, extra images listed in `extra_images.json`, and (unless `--skip-vex-images` is set) `.att` VEX attestation images.

### Platform Release Discovery

By default, the platform service discovers releases between the current `rock-solid` channel version (lower bound) and the current `alpha` channel version (upper bound), keeping the highest patch per `(major, minor)`. To narrow this to a single channel, a single version, or a custom build, use `--deckhouse-tag` — see [Release Channel Selection](#release-channel-selection). Suspended channels, the `lts`/CSE special cases, and VEX attestation images are covered there as well.

`--since-version` raises the lower bound above `rock-solid` when the user wants to skip older minors.

`--include-platform` replaces this window with a user-supplied semver constraint, applying the same latest-patch-per-minor and inclusive-anchor rules as module version filtering. Channel snapshots outside the constraint are pruned so the bundle stays internally consistent.

`--proxy-registry` replaces the catalog-based `ListTags` call entirely with a sequential forward-probe walk seeded from the constraint's lower bound. The latest-patch-per-minor and inclusive-anchor rules still apply to the result; only the way the candidate tag list is obtained changes. Full reference: [PROXY-REGISTRY.md](./PROXY-REGISTRY.md).

### Module Filtering Logic

- **Whitelist Mode:** Only specified modules are included
- **Blacklist Mode:** All modules except specified ones are included (default)
- **Version Constraints:** Semver-based filtering for granular control (same dialect as `--include-platform`)
- **Release Channels:** Each module can have multiple release channel versions

### Editions

The last path segment of `--source` selects the Deckhouse edition and is stripped to form the *edition root* that scopes the deckhouse, modules, packages and security repositories. Recognized editions: `ee`, `fe`, `se`, `se-plus`, `be`, `ce`. A source whose last segment is not one of these is treated as a plain path with no edition (the end-of-pull summary then omits the Edition line). The standalone `installer` image is always pulled from the non-edition root. Editions differ in which components exist — for example, only EE/FE ship [security databases](#security-databases).

### Storage Layout

Internally, the tool uses OCI-compliant image layouts for organizing container images and manifests. This ensures compatibility with standard container tools and registries.

---

## Additional Resources

- [Deckhouse Official Documentation](https://deckhouse.io/products/kubernetes-platform/documentation/)
- [Air-Gapped Installation Guide](https://deckhouse.io/products/kubernetes-platform/guides/airgapped-update.html)
- [Deckhouse Licensing](https://deckhouse.io/products/kubernetes-platform/pricing.html)

---

## Support

For issues, questions, or feature requests:
- GitHub Issues: https://github.com/deckhouse/deckhouse-cli/issues
- Community Support: https://deckhouse.io/community/
- Commercial Support: Available with Deckhouse commercial licenses
