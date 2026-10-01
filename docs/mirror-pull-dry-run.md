# `d8 mirror pull --dry-run`

This document specifies the dry-run mode of `d8 mirror pull`: which steps of a pull it runs and where it stops, what it reads from the source registry, what it leaves on disk, what it prints, and how its plan relates to the bundle the same command writes without `--dry-run`. It describes the implementation as of 2026-10-01, and every rule names the code that implements it. Where the implementation breaks the contract, §11 lists the deviation.

The key words MUST NOT and SHOULD are used as in RFC 2119. They state what a consumer of the dry-run output, a person or a script, can rely on; the other rules describe what `d8 mirror pull --dry-run` does.

Flags, and which platform, module, package and plugin versions get selected, are described in [internal/mirror/README.MD](../internal/mirror/README.MD). The bundle a real pull writes is specified in [mirror-bundle-layout.md](mirror-bundle-layout.md), cited below as "bundle format §N". This document covers what a dry-run does differently.

- [1. Terms](#1-terms)
- [2. The dry-run invariant](#2-the-dry-run-invariant)
- [3. Invocation](#3-invocation)
- [4. Phases](#4-phases)
- [5. Registry access](#5-registry-access)
- [6. Files](#6-files)
- [7. The plan](#7-the-plan)
- [8. The summary](#8-the-summary)
- [9. Exit status and what a dry-run proves](#9-exit-status-and-what-a-dry-run-proves)
- [10. Tests](#10-tests)
- [11. Known deviations](#11-known-deviations)

## 1. Terms

| Term | Meaning |
|---|---|
| Dry-run | `d8 mirror pull --dry-run <dir>` with any other pull flags. |
| Real pull | The same command line without `--dry-run`. |
| Plan | The image references a dry-run prints under its `[dry-run]` headings (§7). |
| Metadata image | An image the dry-run reads to select what to pull: a release-channel image of the platform, a module or a package, or a platform install image (§5). |
| Scaffolding | An empty OCI layout: `oci-layout`, an `index.json` without descriptors and an empty `blobs/` directory, as `NewImageLayout` in [pkg/registry/image/layout.go](../pkg/registry/image/layout.go) creates it. |
| `<dir>`, `<tmp>` | The bundle directory argument, and `--tmp-dir`, by default `<dir>/.tmp` (bundle format [§7.2](mirror-bundle-layout.md#72-working-directory)). |
| E, Rs, M | The edition root, the source root and the modules path, derived from `--source` and `--modules-path-suffix` (bundle format [§7.1](mirror-bundle-layout.md#71-source-repositories)). |

## 2. The dry-run invariant

A dry-run is a real pull that stops every phase before its first image download:

- it validates the command line exactly as a real pull does, the bundle directory check included (§3);
- it runs the phases of a real pull in the same order and honours the same skip flags (§4);
- in every phase it makes the same access checks and the same selection of platform versions, release channels, modules, packages, versions and plugins, so the versions it reports are the versions a real pull selects;
- it reads tag lists, manifests and metadata images, and downloads no other image (§5);
- it writes nothing into `<dir>` and leaves scaffolding in `<tmp>` (§6);
- it prints the plan (§7) and a summary (§8).

The plan is what selection produced, not a rehearsal of the download: it omits every image a real pull discovers only while downloading, and it lists references that were never checked against the registry (§7.10). Consumers MUST NOT use the plan as the list, the count or the size of what a real pull downloads, and MUST NOT take a successful dry-run as proof that the real pull succeeds (§9).

## 3. Invocation

`d8 mirror pull --dry-run [flags] <dir>` accepts every flag of a real pull (`AddFlags` in [cmd/pull/flags/flags.go](../internal/mirror/cmd/pull/flags/flags.go)).

### 3.1. Validation

`parseAndValidateParameters` in [cmd/pull/validation.go](../internal/mirror/cmd/pull/validation.go) runs before any phase, the same in both modes:

- it rejects what a real pull rejects: the flag pairs `NewCommand` in [cmd/pull/pull.go](../internal/mirror/cmd/pull/pull.go) marks mutually exclusive, `--since-version` with `--deckhouse-tag`, `--only-extra-images` with `--include-plugin`, a `--proxy-registry` pull without the bounds it needs, a negative `--images-bundle-chunk-size`, and a `--source` without a path after the host;
- it creates `<dir>` when it does not exist, and then requires it to be empty or to hold nothing but a `.tmp` directory, unless `--force` is given (`validateImagesBundlePathArg`); a dry-run against the directory of an earlier pull therefore fails with `<dir> is not empty, use --force to override`;
- it creates `<tmp>` (`validateTmpPath`).

### 3.2. Flags without effect

| Flag | In a dry-run |
|---|---|
| `--gost-digest` | no effect: `Puller.Execute` returns before `computeGOSTDigests` |
| `--images-bundle-chunk-size` | no effect beyond validation: no archive is written |
| `--force` | relaxes the bundle directory check; files in `<dir>` are not touched |

Every other flag acts as in a real pull. `--verbose-summary` lists the modules and packages in the summary (§8). `--no-pull-resume`, or a `<tmp>/mirror/pull/<md5 of --source>` older than 24 hours, deletes that directory (`cleanupWorkingDirectory`), as in a real pull.

## 4. Phases

`PullService.Pull` ([pull.go](../internal/mirror/pull.go)) runs the phases of bundle format [§7.3](mirror-bundle-layout.md#73-phases) in the same order and with the same skip flags; `NewPullService` passes `PullServiceOptions.DryRun` to every service. Each phase does the work in the "Runs" column and returns before the work in the "Stops before" column:

| # | Phase | Runs | Stops before |
|---|---|---|---|
| 1 | platform: `PullPlatform`, then `pullDeckhousePlatformDryRun` in [platform/platform_dryrun.go](../internal/mirror/platform/platform_dryrun.go) | access check, release discovery, download list, component list read from the install images (`extractImageDigestsFromRemote`) | downloading, VEX lookup, `deckhousereleases.yaml`, channel aliases, sorting, `platform.tar` |
| 2 | installer: `PullInstaller` in [installer/installer.go](../internal/mirror/installer/installer.go) | tag check | downloading, `installer.tar` |
| 3 | security: `PullSecurity` in [security/security.go](../internal/mirror/security/security.go) | `trivy-db:2` check | downloading, sorting, `security.tar` |
| 4 | modules: `pullModules` and `pullSingleModule` in [modules/modules.go](../internal/mirror/modules/modules.go) | catalog listing, filter, scaffolding; per module the channel list, the `lts` check, the versions read from the channel images, and the tag listing for a semver constraint | downloading channel, version, `release:<version>`, internal, extra and VEX images; sorting; exact-pin aliases; `module-<name>.tar` |
| 5 | packages: `pullPackages` and `pullSinglePackage` in [packages/packages.go](../internal/mirror/packages/packages.go) | as modules, with `version` in place of `release` | as modules |
| 6 | package versions: `PullPackageVersions` in [packages/packages.go](../internal/mirror/packages/packages.go) | catalog listing | per-package listing and downloading, `package-versions.tar` |
| 7 | d8 CLI: `PullCLI` in [dist/cli.go](../internal/mirror/dist/cli.go) | version resolution (`resolveCLITag`) | downloading, `deckhouse-cli.tar` |
| 8 | plugins: `PullPlugins` in [dist/plugins.go](../internal/mirror/dist/plugins.go) | catalog and contract resolution against the versions phases 1 and 4 selected (`pluginsInput` in [pull.go](../internal/mirror/pull.go)) | downloading, `plugin-<name>.tar` |

After the phases, `Puller.Execute` ([cmd/pull/pull.go](../internal/mirror/cmd/pull/pull.go)) prints the summary and returns. It collects no bundle statistics, computes no GOST checksums and does not run the final cleanup of `<tmp>`.

## 5. Registry access

These are all the requests a dry-run sends to the source. Tag lists and `HEAD` requests are small; reading a metadata image transfers its layers.

| Phase | Requests |
|---|---|
| platform | the access check: `HEAD E:<tag>` for a pinned tag (`--deckhouse-tag`, or an exact `--include-platform`), `HEAD E/release-channel:<channel>` for a pinned channel name and for a discovery pull (`stable` first); the manifest and layers of every published `E/release-channel:<channel>`, `<channel>` being `alpha`, `beta`, `early-access`, `stable`, `rock-solid` and `lts`; the tag list of `E/release-channel` when discovery needs it; the manifest and layers of `E/install:<version>` for every selected version (below) |
| installer | `HEAD Rs/installer:<tag>` |
| security | `HEAD E/security/trivy-db:2` |
| modules | the tag list of `E/M`; per module `HEAD E/M/<name>/release:lts`, a manifest request for each of the five default channels and for `lts` when it exists, the layers of the channel images found, and the tag list of `E/M/<name>` for a semver constraint |
| packages | the same under `E/packages`, with `version` in place of `release` |
| package versions | the tag list of `E/packages` |
| d8 CLI | `HEAD Rs/deckhouse-cli:<tag>` for `--deckhouse-cli-tag`, otherwise the tag list of `Rs/deckhouse-cli` |
| plugins | the tag lists of `Rs/deckhouse-cli/plugins` and of every candidate plugin, and the manifests of the candidate versions, which carry the contracts |

With `--proxy-registry`, the tag lists that select versions are replaced by `HEAD` probes of version tags (README "Proxy Registry Mode").

To list the platform components, `extractImageDigestsFromRemote` reads `E/install:<version>` with `ExtractFileFromImage` ([pkg/libmirror/images/extract_file.go](../pkg/libmirror/images/extract_file.go)), which walks the layers from the top and reads every layer that does not hold the file to its end. It looks for `deckhouse/candi/images_tags.json` first and, when no layer holds it, walks again from the top for `deckhouse/candi/images_digests.json`. The install images deckhouse/deckhouse builds carry `images_digests.json`, which its `.gitlab/scripts/promote-image.sh` reads, and nothing in that repository writes `images_tags.json`, so a dry-run downloads every selected install image in full, and the layers from the top down to the metadata file a second time (§11, item 2).

Nothing else is requested: no `E:<version>`, no component image, no `E/install-standalone:<version>`, no `E/release-channel:<version>`, no security database beyond the `trivy-db:2` check, no module or package version image, no `release:<version>` or `version:<version>` image, no extra image, no VEX attestation, and no layer of the d8 CLI or of a plugin.

## 6. Files

| Path | In a dry-run |
|---|---|
| `<dir>` | created when missing (§3.1); nothing is written into it apart from the default `<tmp>` |
| `<tmp>` | created (§3.1) |
| `<tmp>/installer/installer/` | scaffolding, created at start even with `--no-installer` (`NewService` in [installer/installer.go](../internal/mirror/installer/installer.go)) |
| `<tmp>/security/<db>/` for the four databases | scaffolding, created at start even with `--no-security-db` (`createOCIImageLayoutsForSecurity` in [security/security.go](../internal/mirror/security/security.go)) |
| `<tmp>/modules/<name>/`, `<tmp>/modules/<name>/release/` | scaffolding for every module that passed the filter (`createOCIImageLayoutsForModules`) |
| `<tmp>/packages/<name>/`, `<tmp>/packages/<name>/version/` | scaffolding for every package that passed the filter (`createOCIImageLayoutsForPackages`) |
| `<tmp>/platform/`, `<tmp>/package-versions/`, `<tmp>/deckhouse-cli/`, `<tmp>/plugins/` | not created: `NewService` in [platform/platform.go](../internal/mirror/platform/platform.go) skips its layouts in dry-run, and the others are created on the first download |
| archives, chunks, `.tmp` staging files, `.gostsum` files, `deckhousereleases.yaml` | never written |

Nothing removes the scaffolding: a dry-run runs no cleanup, and the final cleanup of a real pull keeps `<tmp>` (bundle format [§7.2](mirror-bundle-layout.md#72-working-directory)). A real pull that uses the same `<tmp>`, which it does by default when it pulls into the same `<dir>`, packs the leftovers into its `security.tar` (§11, item 1). A dry-run SHOULD therefore get a `--tmp-dir` of its own, or `<tmp>` SHOULD be deleted before the real pull.

## 7. The plan

### 7.1. Output format

The plan is part of the log `d8 mirror pull` writes to standard output (`SLogger` in [pkg/libmirror/util/log](../pkg/libmirror/util/log/)). Every line carries a timestamp, a level and ANSI colour codes, and lines logged inside a progress block (`╔ Pull Modules` … `╚`) are prefixed with `║`. A plan block is a heading that starts with `[dry-run]`, followed by indented references, `<repository>:<tag>` or `<repository>@sha256:<hex>`; other log lines, such as `Module found: <name>` and warnings, are interleaved.

Within a block, the references are sorted byte-wise in each platform group, follow the selection order for module and package versions and for plugins, and come in an unspecified order that changes between runs for the security databases and for the module and package channels (Go map iteration in `pullSecurityDatabases` and in the `printDryRunPlan` functions of the modules and packages services).

The plan is a log, not an interface: tools MUST NOT rely on its format or its order.

### 7.2. Platform

Printed by `pullDeckhousePlatformDryRun`. The phase first logs `Deckhouse releases to pull: [<versions>]` and, for every selected version, `[dry-run] Streaming installer metadata for <version> from registry`, followed by `Deckhouse digests found: <n>` or by the warning `[dry-run] Could not extract images from installer "<version>": <error>` when the install image or its metadata file cannot be read. Then:

```
[dry-run] Platform images that would be pulled:
  Deckhouse components: <n> images
    <E>:<version>                       one per selected version
    <E>@sha256:<hex>                    one per image the install metadata lists
  Release channels: <n>
    <E>/release-channel:<channel>       every published channel that is not suspended
    <E>/release-channel:<version>       one per selected version
  Installer: <n>
    <E>/install:<version>
  Standalone installer: <n>
    <E>/install-standalone:<version>
  Total: <n> platform images
```

- The selected versions are those of bundle format [§6.1](mirror-bundle-layout.md#61-platformtar): the discovery result, or the pinned version or custom tag of a tag-pinned pull.
- The component references are the values of `images_digests.json` in digest form, or those of `images_tags.json` as `<E>:<value>` when an install image has that file, merged over all versions without duplicates. `Deckhouse digests found: <n>` counts the references not already found for an earlier version (§11, item 8).
- The channels are those the source publishes among `alpha`, `beta`, `early-access`, `stable`, `rock-solid` and `lts`, without the suspended ones unless `--ignore-suspend` is given. A suspended channel that the request resolves to fails the dry-run as it fails a real pull.
- `<n>` is the number of distinct references in the group and `Total` their sum. It counts references, not images: `release-channel:stable` and `release-channel:<version>` name one manifest and count twice.
- Not listed: the VEX attestations of the components, which a real pull adds unless `--skip-vex-images` is given (§11, item 3), and the channel aliases of `install/` and the re-pointed channels of `release-channel/`, which are descriptors rather than downloads (bundle format [§6.1](mirror-bundle-layout.md#61-platformtar)).
- Of these references only the channel images and the install images are requested (§5).

### 7.3. Installer

```
[dry-run] Installer images that would be pulled:
  <Rs>/installer:<tag>
```

`<tag>` is `--installer-tag`, by default `latest`. The block is printed only when `HEAD Rs/installer:<tag>` succeeds. Otherwise the phase logs the warning `installer access: <error>`, prints no block and is reported as `not pulled` (`accessSkipped` in [installer/installer.go](../internal/mirror/installer/installer.go)), as in a real pull.

### 7.4. Security databases

```
[dry-run] Security database images that would be pulled:
  <E>/security/trivy-db:2
  <E>/security/trivy-bdu:1
  <E>/security/trivy-java-db:1
  <E>/security/trivy-checks:0
```

The block is printed when `<E>/security/trivy-db:2` exists, with the four lines in unspecified order. The other three databases are not checked, although a real pull skips a missing one (bundle format [§6.3](mirror-bundle-layout.md#63-securitytar)). When `trivy-db:2` does not exist the phase logs the warning `Security databases are not available in this edition, skipping` and prints no block, as in a real pull.

### 7.5. Modules

The phase logs `Module found: <name>` for every module that passes the filter and `Repo contains <n> modules to pull`, then, inside a `Pull Modules` progress block (`Pull Extra Images` with `--only-extra-images`), prints one block per module in catalog order (`printDryRunPlan` in [modules/modules.go](../internal/mirror/modules/modules.go)):

```
[dry-run] Module '<name>' images that would be pulled:
  <E>/<M>/<name>/release:<channel>
  <E>/<M>/<name>:<version>
  (extra images discovery requires a real pull)
```

- Channel lines: `alpha`, `beta`, `early-access`, `stable` and `rock-solid`, whether the module publishes them or not, plus `lts` when it exists; none for an exact pin (`@=`) or with `--only-extra-images` (`discoverChannelVersions`).
- Version lines: every selected version in selection order, that is the versions the existing channel images name in `version.json`, then those the constraint adds (README "Module Filtering").
- The last line is printed when at least one version is selected.
- Not listed, although a real pull downloads them (bundle format [§6.4](mirror-bundle-layout.md#64-module-nametar)): `release:<version>` for every version, the images each version's `images_digests.json` lists, the extra images, and the VEX attestations of all of them (§11, item 3). With `--only-extra-images` the version lines name images a real pull does not download (§11, item 4).
- A module that publishes nothing is listed with its five channel lines, while a real pull writes no archive for it (§11, item 5).

### 7.6. Packages

As §7.5, with `Package found: <name>`, `Repo contains <n> packages to pull`, a `Pull Packages` progress block and (`printDryRunPlan` in [packages/packages.go](../internal/mirror/packages/packages.go)):

```
[dry-run] Package '<name>' images that would be pulled:
  <E>/packages/<name>/version:<channel>
  <E>/packages/<name>:<version>
  (extra images discovery requires a real pull)
```

Not listed: `version:<version>`, the internal, extra and VEX images (bundle format [§6.5](mirror-bundle-layout.md#65-package-nametar)). With `--only-extra-images` the phase runs even when `--no-packages` is given (bundle format [§7.3](mirror-bundle-layout.md#73-phases)).

### 7.7. Package versions

```
[dry-run] package-versions archive would contain release images for: <name>, <name>, …
```

The line names every tag of `<E>/packages` in listing order, whatever `--no-packages`, `--include-package` and `--exclude-package` say. It is not printed when the catalog is missing or empty; any other listing error gives the warning `Skipping package release images (package-versions): <error>`. The line names packages, not images, and does not tell whether a real pull writes `package-versions.tar` (bundle format [§6.6](mirror-bundle-layout.md#66-package-versionstar), §11, item 6).

### 7.8. d8 CLI

```
[dry-run] Deckhouse CLI that would be pulled:
  <Rs>/deckhouse-cli:<version>
```

`<version>` is `--deckhouse-cli-tag`, whose absence from the source fails the dry-run as it fails a real pull, or the newest stable semver tag of `<Rs>/deckhouse-cli` (`resolveCLITag`). When no version can be resolved the phase logs the warning `d8 CLI not mirrored: <reason>` instead of the block. The phase is skipped with `--only-extra-images`.

### 7.9. Plugins

The resolver's warnings and the `Skipping plugin <name>: <reason>` lines come first, then:

```
[dry-run] Plugins that would be pulled:
  <Rs>/deckhouse-cli/plugins/<name>:<version>
```

with one line per selected plugin version in resolution order, or the line `No plugins to mirror`. The selection equals that of a real pull (`TestPullE2E_DryRun_ResolutionParityNoFiles`). The phase is skipped with `--only-extra-images`.

### 7.10. Plan and bundle

The images a real pull with the same flags downloads (bundle format [§6](mirror-bundle-layout.md#6-archive-contents)), and how the plan covers them:

| Images a real pull downloads | In the plan | Requested by the dry-run |
|---|---|---|
| `E:<version>` and the component images | yes | no |
| VEX attestations of the components | no | no |
| `E/release-channel:<channel>` | yes | yes |
| `E/release-channel:<version>` | yes | no |
| `E/install:<version>` | yes | yes, in full (§5) |
| `E/install-standalone:<version>` | yes | no |
| `Rs/installer:<tag>` | yes | `HEAD` |
| security databases | all four | `HEAD` of `trivy-db:2` only |
| module and package channel images | the five default channels, published or not, and `lts` when it exists | yes; a missing one stays in the plan |
| module and package version images | yes, also with `--only-extra-images`, when a real pull skips them | no |
| `release:<version>`, `version:<version>` | no | no |
| images listed by a version's `images_digests.json` | no | no |
| extra images | no | no |
| VEX attestations of modules and packages | no | no |
| images of `package-versions.tar` | package names only | no |
| `Rs/deckhouse-cli:<version>` | yes | the tag list, or `HEAD` of the pinned tag |
| plugin versions | yes | the manifest |

### 7.11. Example

A dry-run of `--source registry.example.com/deckhouse/ee --verbose-summary` with no filters, against a registry that publishes three platform versions, module `foo` with v1.2.0 on `stable` and v1.3.0 on `alpha`, module `bar` with nothing, package `pkg1` with v0.3.0 on `stable`, package `pkg2` with nothing, two of the four security databases, the installer, the d8 CLI and the plugin `foo-tool`, prints (timestamps and colours removed, digests shortened, the address of the test registry replaced by `registry.example.com`):

```
INFO   d8 version: dev
INFO   Creating OCI Image Layouts
INFO   Creating OCI Image Layouts for Security
INFO   Creating OCI Image Layouts for Modules
INFO   Creating OCI Image Layouts for Packages
INFO   Creating OCI Image Layouts for Installer
INFO   Deckhouse releases to pull: [1.70.3 1.72.1 1.71.2]
INFO   Searching for Deckhouse built-in modules digests
INFO   [dry-run] Streaming installer metadata for v1.70.3 from registry
INFO   Deckhouse digests found: 2
INFO   [dry-run] Streaming installer metadata for v1.72.1 from registry
INFO   Deckhouse digests found: 1
INFO   [dry-run] Streaming installer metadata for v1.71.2 from registry
INFO   Deckhouse digests found: 0
INFO   [dry-run] Platform images that would be pulled:
INFO     Deckhouse components: 6 images
INFO       registry.example.com/deckhouse/ee:v1.70.3
INFO       registry.example.com/deckhouse/ee:v1.71.2
INFO       registry.example.com/deckhouse/ee:v1.72.1
INFO       registry.example.com/deckhouse/ee@sha256:786d7c93…
INFO       registry.example.com/deckhouse/ee@sha256:afb1e921…
INFO       registry.example.com/deckhouse/ee@sha256:da3535c9…
INFO     Release channels: 8
INFO       registry.example.com/deckhouse/ee/release-channel:alpha
INFO       registry.example.com/deckhouse/ee/release-channel:beta
INFO       registry.example.com/deckhouse/ee/release-channel:early-access
INFO       registry.example.com/deckhouse/ee/release-channel:rock-solid
INFO       registry.example.com/deckhouse/ee/release-channel:stable
INFO       registry.example.com/deckhouse/ee/release-channel:v1.70.3
INFO       registry.example.com/deckhouse/ee/release-channel:v1.71.2
INFO       registry.example.com/deckhouse/ee/release-channel:v1.72.1
INFO     Installer: 3
INFO       registry.example.com/deckhouse/ee/install:v1.70.3
INFO       registry.example.com/deckhouse/ee/install:v1.71.2
INFO       registry.example.com/deckhouse/ee/install:v1.72.1
INFO     Standalone installer: 3
INFO       registry.example.com/deckhouse/ee/install-standalone:v1.70.3
INFO       registry.example.com/deckhouse/ee/install-standalone:v1.71.2
INFO       registry.example.com/deckhouse/ee/install-standalone:v1.72.1
INFO     Total: 20 platform images
INFO   [dry-run] Installer images that would be pulled:
INFO     registry.example.com/deckhouse/installer:latest
INFO   [dry-run] Security database images that would be pulled:
INFO     registry.example.com/deckhouse/ee/security/trivy-bdu:1
INFO     registry.example.com/deckhouse/ee/security/trivy-java-db:1
INFO     registry.example.com/deckhouse/ee/security/trivy-checks:0
INFO     registry.example.com/deckhouse/ee/security/trivy-db:2
INFO   Module found: bar
INFO   Module found: foo
INFO   Repo contains 2 modules to pull
INFO  ╔ Pull Modules
INFO  ║ [1/2] Processing module: bar
INFO  ║ [dry-run] Module 'bar' images that would be pulled:
INFO  ║   registry.example.com/deckhouse/ee/modules/bar/release:rock-solid
INFO  ║   registry.example.com/deckhouse/ee/modules/bar/release:alpha
INFO  ║   registry.example.com/deckhouse/ee/modules/bar/release:beta
INFO  ║   registry.example.com/deckhouse/ee/modules/bar/release:early-access
INFO  ║   registry.example.com/deckhouse/ee/modules/bar/release:stable
INFO  ║ [2/2] Processing module: foo
INFO  ║ [dry-run] Module 'foo' images that would be pulled:
INFO  ║   registry.example.com/deckhouse/ee/modules/foo/release:alpha
INFO  ║   registry.example.com/deckhouse/ee/modules/foo/release:beta
INFO  ║   registry.example.com/deckhouse/ee/modules/foo/release:early-access
INFO  ║   registry.example.com/deckhouse/ee/modules/foo/release:stable
INFO  ║   registry.example.com/deckhouse/ee/modules/foo/release:rock-solid
INFO  ║   registry.example.com/deckhouse/ee/modules/foo:v1.3.0
INFO  ║   registry.example.com/deckhouse/ee/modules/foo:v1.2.0
INFO  ║   (extra images discovery requires a real pull)
INFO  ╚ Pull Modules succeeded in 618.294608ms
INFO   Package found: pkg1
INFO   Package found: pkg2
INFO   Repo contains 2 packages to pull
INFO  ╔ Pull Packages
INFO  ║ [1/2] Processing package: pkg1
INFO  ║ [dry-run] Package 'pkg1' images that would be pulled:
INFO  ║   registry.example.com/deckhouse/ee/packages/pkg1/version:early-access
INFO  ║   registry.example.com/deckhouse/ee/packages/pkg1/version:stable
INFO  ║   registry.example.com/deckhouse/ee/packages/pkg1/version:rock-solid
INFO  ║   registry.example.com/deckhouse/ee/packages/pkg1/version:alpha
INFO  ║   registry.example.com/deckhouse/ee/packages/pkg1/version:beta
INFO  ║   registry.example.com/deckhouse/ee/packages/pkg1:v0.3.0
INFO  ║   (extra images discovery requires a real pull)
INFO  ║ [2/2] Processing package: pkg2
INFO  ║ [dry-run] Package 'pkg2' images that would be pulled:
INFO  ║   registry.example.com/deckhouse/ee/packages/pkg2/version:alpha
INFO  ║   registry.example.com/deckhouse/ee/packages/pkg2/version:beta
INFO  ║   registry.example.com/deckhouse/ee/packages/pkg2/version:early-access
INFO  ║   registry.example.com/deckhouse/ee/packages/pkg2/version:stable
INFO  ║   registry.example.com/deckhouse/ee/packages/pkg2/version:rock-solid
INFO  ╚ Pull Packages succeeded in 576.587299ms
INFO   [dry-run] package-versions archive would contain release images for: pkg1, pkg2
INFO   [dry-run] Deckhouse CLI that would be pulled:
INFO     registry.example.com/deckhouse/deckhouse-cli:v0.13.1
WARN   plugin package ships with the platform but is not available in this registry; the bundle will not contain it
WARN   plugin system ships with the platform but is not available in this registry; the bundle will not contain it
INFO   [dry-run] Plugins that would be pulled:
INFO     registry.example.com/deckhouse/deckhouse-cli/plugins/foo-tool:v1.0.0
INFO
╔══ Pull plan (dry-run) ════════════════════════════════
║ Edition:    EE
║ Platform:   v1.72.1, v1.71.2, v1.70.3 (5 channels)
║ Installer:  latest
║ Security:   4/4 databases
║ Modules:    2
║     bar
║     foo                             [v1.3.0, v1.2.0]
║ Packages:   2
║     pkg1                            [v0.3.0]
║     pkg2
║ d8 dist:    v0.13.1
║ d8 plugins: 1  ·  1 for modules
║     foo
║       foo-tool                        [v1.0.0]
║     warning: plugin package ships with the platform but is not available in this registry; the bundle will not contain it
║     warning: plugin system ships with the platform but is not available in this registry; the bundle will not contain it
║
║ No images were downloaded (dry-run).
║ Elapsed: 3s
╚═══════════════════════════════════════════════════════
```

A real pull with the same flags against the same registry stored 45 descriptors; §11, items 3 and 5, compare them with these 50 planned references.

## 8. The summary

`renderPullSummary` in [cmd/pull/summary.go](../internal/mirror/cmd/pull/summary.go) prints the framed block of a real pull under the title `Pull plan (dry-run)`, or `Pull failed` after an error. The block never contains `Bundle artifacts`, and its state line is `No images were downloaded (dry-run).`, or `Pull failed; …` or `Pull was cancelled; …`. The component lines read:

| Line | In a dry-run | In a real pull |
|---|---|---|
| `Platform` | the selected versions and the number of selected channels | the same |
| `Installer` | the tag, or `not pulled` when the tag check failed | the same |
| `Security` | `4/4 databases` whenever `trivy-db:2` exists | the number of databases that received an image |
| `Modules`, `Packages` | every item that passed the filter, including one that publishes nothing; never a VEX count | the items that received at least one image, with their VEX count |
| `d8 dist` | the resolved version, or `not mirrored - <reason>` | the same, and `not mirrored - could not be pulled at <tag>` when the download of an automatically selected version fails |
| `d8 plugins` | the selected plugins and their provenance | the same |

With `--verbose-summary` the module and package entries list their selected versions, the same in both modes. The planned image counts the services' `Stats` methods compute in dry-run are not printed.

## 9. Exit status and what a dry-run proves

The exit status is 0 when every phase completes, and when the run is cancelled, that is when the returned error wraps `context.Canceled` (`classifyPullOutcome` in [cmd/pull/pull.go](../internal/mirror/cmd/pull/pull.go)). Any other error exits with 1 (`execute` in [cmd/d8/root.go](../cmd/d8/root.go)); validation errors (§3.1) do so before any phase runs.

| Condition | Dry-run | Real pull |
|---|---|---|
| conflicting flags; a non-empty `<dir>` without `--force` | exit 1 | exit 1 |
| source unreachable, credentials rejected | exit 1 | exit 1 |
| `--deckhouse-tag` not in the source | exit 1 | exit 1 |
| the request resolves to a suspended channel | exit 1 | exit 1 |
| `--deckhouse-cli-tag` not in the source | exit 1 | exit 1 |
| `Rs/installer:<tag>` missing | warning, installer skipped | the same |
| `E/security/trivy-db:2` missing | warning, security skipped | the same |
| `E/install:<version>` missing | warning, exit 0 | crash (bundle format [§11](mirror-bundle-layout.md#11-known-deviations), item 10) |
| `E:<version>`, a component image or, in a discovery pull, `E/release-channel:<version>` missing | exit 0, nothing logged | exit 1 |
| a persistent registry error on a module or package version image, which a real pull reads for `extra_images.json` | exit 0 | exit 1 |
| a download, disk or packing failure | not reached | exit 1 |

A dry-run that exits 0 shows that the flags are valid, that the source answers with the given credentials and that selection succeeds. It does not show that the planned images exist, that the real pull succeeds or how large the bundle gets.

## 10. Tests

The dry-run tests run offline against fake registries:

```
go test -count=1 -run DryRun ./internal/mirror/...
```

| Test | Rule it pins |
|---|---|
| `TestDryRunFlagRegistered` in [cmd/pull/pull_dryrun_test.go](../internal/mirror/cmd/pull/pull_dryrun_test.go) | `--dry-run` exists and defaults to `false` |
| `TestDryRunNoBundleOutput`, `TestDryRunNoBundleWithNoPlatform`, `TestDryRunWithDeckhouseTag`, `TestDryRunExitsZeroOnSuccess` in the same file | no `.tar`, `.chunk` or `.gostsum` file in `<dir>`, exit 0 (§6, §9) |
| `TestPullerExecute_DryRun_PluginsNoFiles` in [cmd/pull/pull_plugins_stub_test.go](../internal/mirror/cmd/pull/pull_plugins_stub_test.go) | the plugins phase writes nothing |
| `TestDryRun_NoBundleFilesWritten` in the platform, installer, security, modules and packages packages | the phase writes nothing into the bundle directory |
| `TestDryRun_NoOCILayoutCreated` in [platform/platform_dryrun_test.go](../internal/mirror/platform/platform_dryrun_test.go) | no `<tmp>/platform` layout (§6) |
| `TestDryRun_WorkingDirHasLayouts` in the installer, security and modules packages | scaffolding in `<tmp>` (§6) |
| `TestPullPlatform_DryRun_*` in [platform/pull_platform_test.go](../internal/mirror/platform/pull_platform_test.go) | platform selection: suspended channels, custom tags, `--since-version`, `--include-platform`, LTS-only registries |
| `TestPullPackageVersions_DryRun` in [packages/packages_test.go](../internal/mirror/packages/packages_test.go) | the package versions phase writes nothing |
| `TestStats_DryRun` in the installer and packages packages, `TestStats_DryRun_AccessFailure_ReportsNotAttempted` | planned counts, and `not pulled` after a failed installer check (§8) |
| `TestPullCLI_DryRun` in [dist/cli_test.go](../internal/mirror/dist/cli_test.go), `TestPullPlugins_DryRun` in [dist/pull_plugins_test.go](../internal/mirror/dist/pull_plugins_test.go) | the version is recorded, no image is pulled, no file is written |
| `TestPullE2E_DryRun_ResolutionParityNoFiles` in [pull_plugins_e2e_test.go](../internal/mirror/pull_plugins_e2e_test.go) | plugin selection equals that of a real pull (§7.9) |

`TestDryRunRealRegistry` in [cmd/pull/pull_realregistry_test.go](../internal/mirror/cmd/pull/pull_realregistry_test.go) runs a tag-pinned dry-run against a real registry and is skipped unless both variables are set:

```
D8_TEST_REGISTRY=registry.deckhouse.io/deckhouse/fe D8_TEST_LICENSE_TOKEN=<token> \
  go test -count=1 -run TestDryRunRealRegistry -v -timeout 300s ./internal/mirror/cmd/pull/
```

No test compares a plan with the bundle a real pull writes from the same registry, so none of the items of §11 is caught by the suite.

## 11. Known deviations

Verified on 2026-10-01 by running `PullService` with and without `DryRun` against an in-memory OCI registry served over HTTP (go-containerregistry `pkg/registry`) through the production registry client with every request logged, by running the `d8 mirror pull` command code (`NewCommand`) against the same registry and against the stub registry (`STUB_REGISTRY_CLIENT=true`), and by pushing the resulting bundles with `PushService`. Each item contradicts the `--dry-run` help text ("Print what would be pulled without downloading any images. Useful for fast validation of flags and filters."), README ("Print what would be pulled without downloading any images or writing a bundle"), a rule above, or the expectation that a dry-run changes nothing a later pull depends on.

1. A dry-run corrupts the next real pull that shares its `<tmp>`. The scaffolding of §6 stays in `<tmp>`, and a real pull packs the whole `<tmp>` into `security.tar` before its modules phase starts (bundle format [§11](mirror-bundle-layout.md#11-known-deviations), item 1), so the archive carries an empty layout for every module and package the dry-run saw. Push skips the empty layouts but writes a discovery tag for every directory under `modules/` and `packages/` (bundle format [§8.8](mirror-bundle-layout.md#88-discovery-tags)). Since `<tmp>` defaults to `<dir>/.tmp`, `d8 mirror pull --dry-run bundle/` followed by `d8 mirror pull bundle/` is enough. Verified with modules `foo` and `bar` and packages `pkg1` and `pkg2`, where `bar` and `pkg2` publish nothing: after a dry-run, the real pull with the same flags wrote a `security.tar` holding `modules/{bar,foo}/…` and `packages/{pkg1,pkg2}/…`, and its push gave the target the tags `modules:bar` and `packages:pkg2` without any `modules/bar` or `packages/pkg2` repository; the same pull without the dry-run gave only `modules:foo` and `packages:pkg1`. A real pull with `--include-module foo --include-package pkg1` after an unfiltered dry-run gave the same two extra tags. A real pull that writes no `security.tar` (`--no-security-db`, or no `trivy-db:2` in the source) leaves the scaffolding in place for the next one.
2. A dry-run downloads every selected install image (§5). For each selected version `extractImageDigestsFromRemote` reads `E/install:<version>` layer by layer for `deckhouse/candi/images_tags.json`, reading each layer that lacks it to its end, and, when no layer has it, again from the top for `deckhouse/candi/images_digests.json`. The install images deckhouse/deckhouse builds have no `images_tags.json` (§5), so each one is transferred once in full and its layers down to the metadata file a second time. Verified with three versions: every install layer was requested twice. The release-channel images of the platform, the modules and the packages are downloaded too; selection needs them.
3. The plan omits images a real pull downloads (§7.10): the VEX attestations of the platform components; for every module and package version its `release:<version>` or `version:<version>` image, the images its `images_digests.json` lists, its extra images and the VEX attestations of all of them; and the images of `package-versions.tar`. The only hint, `(extra images discovery requires a real pull)`, names extra images alone. Verified with the registry of §7.11: of the 45 descriptors the real pull stored, 13 are not in the plan. Five are the channel aliases of `install/`, which are not downloads; the other eight are seven downloaded images, namely the platform VEX attestation, the internal image, the VEX attestation, the extra image, `release:v1.2.0` and `release:v1.3.0` of `foo`, and `version:v0.3.0` of `pkg1`, which is stored both in `package-pkg1.tar` and in `package-versions.tar`.
4. `--only-extra-images` inverts the module and package plan. In this mode a real pull downloads only the extra images of the selected versions and their VEX attestations, while the plan lists the version images `<E>/<M>/<name>:<version>`, which the real pull does not download, and no extra image. Verified with `--only-extra-images --include-module foo@~1.2.0 --no-packages --no-platform --no-installer --no-security-db`: the plan named `modules/foo:v1.2.0`, and the real pull stored only `modules/foo/extra/scanner:v3`. The packages phase ran despite `--no-packages` (§7.6) and listed `pkg1` and `pkg2` with empty blocks, so the summary read `Packages: 2` where the real pull's read `Packages: 0`.
5. The plan and the summary count references nobody checked (§5). The five default channels of every module and package are listed whether published or not, the four security databases whenever `trivy-db:2` exists, and items that publish nothing are listed and counted. Verified with the registry of §7.11: 19 of the 50 planned references did not exist (every reference of `bar` and `pkg2`, three channels of `foo`, four of `pkg1`, `trivy-java-db:1` and `trivy-checks:0`), and the dry-run summary read `Security: 4/4 databases`, `Modules: 2` and `Packages: 2`, where the real pull's read `2/4`, `1` and `1`.
6. The package versions line (§7.7) names every package of the catalog, including one whose `version` repository is empty. Verified: the line named `pkg1, pkg2`, and the real `package-versions.tar` held only `pkg1`.
7. The install metadata is matched by raw entry name. `ExtractFileFromImage` compares each tar entry name with `deckhouse/candi/images_digests.json` as it is, while a real pull reads the flattened image through `mutate.Extract`, which cleans entry names with `filepath.Clean`. For install images that store `./deckhouse/candi/images_digests.json`, the dry-run logged `Could not extract images from installer` for every version and planned no component image, while the real pull downloaded all three.
8. Two log lines misreport. `Creating OCI Image Layouts` is printed although the platform service creates no layout in dry-run (`NewService` in [platform/platform.go](../internal/mirror/platform/platform.go)), and `Deckhouse digests found: <n>` counts only the references not found for an earlier version: three installers listing 2, 3 and 3 images logged 2, 1 and 0 (§7.11), where a real pull logs the number each installer lists.
