# `d8 mirror pull --dry-run`

This document specifies the dry-run mode of `d8 mirror pull`: which steps of a pull it runs and where each phase stops, what it requests from the source registry, what it leaves on disk, what it prints, and how its plan relates to the bundle that the same command writes without `--dry-run`. It describes the implementation as of 2026-10-02, and every rule names the code that implements it. Where the implementation breaks the contract, §11 lists the deviation.

The key words MUST NOT and SHOULD are used as in RFC 2119 and bind the operator: whoever runs a dry-run or acts on its output, a person or a script. The other rules describe what `d8 mirror pull --dry-run` does.

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
| Operator | Whoever runs a dry-run or acts on its output, a person or a script. |
| Read | Request a manifest or a layer of an image from the source. A real pull reads what it stores, and a dry-run reads only metadata images (§5). |
| Store | Write an image into a layout of the bundle. A dry-run stores nothing. |
| Metadata image | An image the dry-run reads to select what to pull: a release-channel image of the platform, a module or a package, or a platform install image (§5). |
| Catalog | A repository whose tags name the items to mirror: `E/M` for modules, `E/packages` for packages, `Rs/deckhouse-cli/plugins` for plugins. |
| Discovery pull, tag-pinned pull | A pull that selects platform versions from the release channels, possibly narrowed by `--since-version` or an `--include-platform` range; and a pull with `--deckhouse-tag` or an exact `--include-platform "=…"` (bundle format [§6.1](mirror-bundle-layout.md#61-platformtar)). |
| Selected version | A version that the selection rules of README pick for the platform, a module, a package, a plugin or the d8 CLI; for the platform also a custom tag. |
| Access check | The existence check a phase makes before it selects anything (§5). |
| Plan | The image references a dry-run prints under its `[dry-run]` headings (§7). |
| Scaffolding | An empty OCI layout as `NewImageLayout` in [pkg/registry/image/layout.go](../pkg/registry/image/layout.go) writes it: `oci-layout`, an `index.json` without descriptors and a `blobs/` directory. On an existing layout it rewrites both files and keeps `blobs/`. |
| `<dir>`, `<tmp>` | The bundle directory argument, and `--tmp-dir`, by default `<dir>/.tmp` (bundle format [§7.2](mirror-bundle-layout.md#72-working-directory)). |
| E, Rs, M | The edition root, the source root and the modules path, derived from `--source` and `--modules-path-suffix` (bundle format [§7.1](mirror-bundle-layout.md#71-source-repositories)). |

## 2. The dry-run invariant

A dry-run is a real pull that stops every phase before it stores its first image:

- it validates the command line with the checks of a real pull, in the same order (§3);
- it runs the phases of a real pull in the same order and honours the same skip flags (§4);
- in every phase it makes the same access checks and the same selection of platform versions, release channels, modules, packages and their versions, so these versions are the ones a real pull selects; the plugin selection is the same as long as every module with a selected version stores an image in the real pull (§7.9);
- it reads tag lists, manifests and metadata images, and stores nothing (§5);
- it writes nothing into `<dir>` outside `<tmp>`, and leaves scaffolding in `<tmp>` (§6);
- it prints the plan (§7) and a summary (§8).

The plan is what selection produced, not a rehearsal of the real pull: it omits every image a real pull finds only by reading the images it stores, and it lists references whose existence the dry-run never establishes (§7.10). An operator MUST NOT use the plan as the list, the count or the size of what a real pull stores, and MUST NOT take a successful dry-run as proof that the real pull succeeds (§9).

## 3. Invocation

`d8 mirror pull --dry-run [flags] <dir>` accepts every flag of a real pull (`AddFlags` in [cmd/pull/flags/flags.go](../internal/mirror/cmd/pull/flags/flags.go)).

### 3.1. Validation

A dry-run validates exactly as a real pull does, in this order:

1. `parseAndValidateParameters` in [cmd/pull/validation.go](../internal/mirror/cmd/pull/validation.go), run as cobra's `PreRunE`:
   - `validateSourceRegistry`: a `--source` other than the default needs a path after the host and must parse as a repository;
   - `parseAndValidateVersionFlags`: rejects `--since-version` with `--deckhouse-tag`, `--include-platform` with either of them, and an unparsable `--since-version` or `--include-platform`;
   - `resolveModuleFlags`: `--include-module` overrides `--no-modules`, with a warning on standard error;
   - `validatePluginFlags`: rejects `--only-extra-images` with `--include-plugin`;
   - `validateProxyRegistryFlag`: rejects `--proxy-registry` with `--deckhouse-tag` or `--since-version`, with nothing to pull, without `--include-platform` while the platform is pulled, without `--include-module` entries that all carry a version constraint while modules are pulled, and with an `--include-plugin` that does not pin an exact version; it does not require `--include-package` (§7.6);
   - `validateImagesBundlePathArg`: requires exactly one argument, creates `<dir>` when it does not exist, and requires a directory that is empty or holds nothing but a `.tmp` directory unless `--force` is given, failing with `<dir> is not empty, use --force to override` otherwise;
   - `validateTmpPath`: creates `<tmp>`;
   - `validateChunkSizeFlag`: rejects a negative `--images-bundle-chunk-size`.
2. Cobra's `ValidateFlagGroups`, which runs after `PreRunE`: rejects the pairs that `NewCommand` in [cmd/pull/pull.go](../internal/mirror/cmd/pull/pull.go) marks mutually exclusive, that is `--include-module` with `--exclude-module`, `--include-package` with `--exclude-package`, `--include-platform` with `--deckhouse-tag` or `--since-version`, and `--proxy-registry` with `--deckhouse-tag` or `--since-version`.
3. `Puller.Execute` in [cmd/pull/pull.go](../internal/mirror/cmd/pull/pull.go): `cleanupWorkingDirectory` (§3.2), then `buildPullService`, which rejects a malformed constraint in `--include-module`, `--exclude-module`, `--include-package`, `--exclude-package` or `--include-plugin`.

A failure at any step exits 1 and prints no summary (§8). A failure after `validateTmpPath` leaves `<dir>` and `<tmp>` created.

### 3.2. Flags

In a dry-run these flags do no more than the table says:

| Flag | In a dry-run |
|---|---|
| `--gost-digest` | no effect: `Puller.Execute` returns before `computeGOSTDigests` |
| `--images-bundle-chunk-size` | validation only (§3.1): no archive is written |
| `--force` | relaxes the bundle directory check only; no file in `<dir>` is touched |
| `--skip-vex-images` | only the warning `The skip-vex-images flag was detected: Vulnerability scanning may not work correctly when this flag is used.` (`PullService.Pull` in [pull.go](../internal/mirror/pull.go)); the plan lists no VEX attestation anyway (§7.10) |

Every other flag acts as in a real pull. `--verbose-summary` changes the summary as in a real pull (§8). `--no-pull-resume` deletes `<tmp>/mirror/pull/<md5 of --source>`, and so does any pull when that directory was last modified more than 24 hours ago (`cleanupWorkingDirectory`, `lastPullWasTooLongAgoToRetry` in [cmd/pull/pull.go](../internal/mirror/cmd/pull/pull.go)). No current code writes that directory, and the scaffolding of §6, which lives directly in `<tmp>`, is not touched.

## 4. Phases

`PullService.Pull` ([pull.go](../internal/mirror/pull.go)) runs the phases of bundle format [§7.3](mirror-bundle-layout.md#73-phases) in the same order and with the same skip flags; `NewPullService` passes `PullServiceOptions.DryRun` to every service. Each phase does the work in the "Runs" column and returns before the work in the "Stops before" column:

| # | Phase | Runs | Stops before |
|---|---|---|---|
| 1 | platform: `PullPlatform` in [platform/platform.go](../internal/mirror/platform/platform.go), ending in `pullDeckhousePlatformDryRun` in [platform/platform_dryrun.go](../internal/mirror/platform/platform_dryrun.go) | access check (`validatePlatformAccess`), release selection (`findTagsToMirror`), download list (`FillDeckhouseImages`, `FillForChannels`), component list read from the install images (`extractImageDigestsFromRemote`) | reading the listed images, the VEX lookup, `deckhousereleases.yaml`, channel aliases, sorting, `platform.tar` |
| 2 | installer: `PullInstaller` in [installer/installer.go](../internal/mirror/installer/installer.go) | access check (`validateInstallerAccess`) | reading, `installer.tar` |
| 3 | security: `PullSecurity` in [security/security.go](../internal/mirror/security/security.go) | access check (`securityDatabasesAvailable`) | reading, sorting, `security.tar` |
| 4 | modules: `PullModules` in [modules/modules.go](../internal/mirror/modules/modules.go), that is `validateModulesAccess`, `pullModules` and `pullSingleModule` | catalog listing, filter, scaffolding; per module the channel list, the `lts` check, the versions named by the channel images, and the tag list for a non-exact constraint | reading channel, version, `release:<version>`, internal, extra and VEX images; sorting; exact-pin aliases; `module-<name>.tar` |
| 5 | packages: `PullPackages` in [packages/packages.go](../internal/mirror/packages/packages.go), that is `validatePackagesAccess`, `pullPackages` and `pullSinglePackage` | as modules, with `version` in place of `release` | as modules; `package-<name>.tar` |
| 6 | package versions: `PullPackageVersions` in [packages/packages.go](../internal/mirror/packages/packages.go) | catalog listing | per-package listing and reading, `package-versions.tar` |
| 7 | d8 CLI: `PullCLI` in [dist/cli.go](../internal/mirror/dist/cli.go) | version selection (`resolveCLITag`) | reading, `deckhouse-cli.tar` |
| 8 | plugins: `PullPlugins` in [dist/plugins.go](../internal/mirror/dist/plugins.go) | catalog and contract resolution (`Resolve` in [dist/resolver.go](../internal/mirror/dist/resolver.go)) against the versions that phases 1 and 4 selected (`pluginsInput` in [pull.go](../internal/mirror/pull.go)) | reading, `plugin-<name>.tar` |

Before the phases `Puller.Execute` ([cmd/pull/pull.go](../internal/mirror/cmd/pull/pull.go)) runs `cleanupWorkingDirectory` and `buildPullService` (§3.1); after them it prints the summary and returns. It collects no bundle statistics, computes no GOST checksums and skips `finalCleanup`.

## 5. Registry access

These are the requests a dry-run sends to the source. Every one is preceded by a `GET /v2/` ping, and under token authentication by a token request, because each client call builds its own transport (`transport.NewWithContext` in go-containerregistry). An existence check is a `HEAD` (`CheckImageExists` in the deckhouse registry client); a `HEAD` that fails with anything but 401, 403 or 404 is repeated as a manifest `GET`.

| Phase | Requests |
|---|---|
| platform | Access check (`validatePlatformAccess`, `validateReleaseChannelAccess`): `HEAD E:<tag>` for a pinned tag that is not a channel name; `HEAD E/release-channel:<name>` for a pinned channel name, or `HEAD E/release-channel:stable` for a discovery pull; when that channel is missing, a `HEAD` of the other channels, all of them for a pinned name and up to the first one found for a discovery pull. Channels (`fetchReleaseChannels`, `getReleaseChannelInfoFromRegistry`): a manifest `GET` of `E/release-channel:<channel>` for `lts`, `alpha`, `beta`, `early-access`, `stable` and `rock-solid` in that order, published or not; for each published one, suspended included, a second manifest `GET` and its layers (`GetMetadata` in [pkg/registry/service/deckhouse_service.go](../pkg/registry/service/deckhouse_service.go)). Selection: the tag list of `E/release-channel` in a discovery pull while `alpha` is published, and for an `--include-platform` range (`expandVersionRange`, `discoverConstrainedPlatformVersions`). Components: the manifest and layers of `E/install:<tag>` for every selected tag (below). |
| installer | `HEAD Rs/installer:<tag>` (`validateInstallerAccess`) |
| security | `HEAD E/security/trivy-db:2` (`securityDatabasesAvailable`) |
| modules | The tag list of `E/M`, twice (`validateModulesAccess`, `discoverModuleNames`). Per module, unless every `--include-module` entry for it is an exact pin or `--only-extra-images` is given: `HEAD E/M/<name>/release:lts`, twice (`discoverChannelVersions`, `extractVersionsFromReleaseChannels`); a manifest `GET` of each of the five default channels, and of `lts` when its `HEAD` succeeded; the layers of every channel image found. The tag list of `E/M/<name>` for a non-exact constraint (`listTagsIfConstrained`). |
| packages | The same under `E/packages`, with `version` in place of `release` (`validatePackagesAccess`, `discoverPackageNames`, `discoverChannelVersions`, `extractVersionsFromVersionChannels`, `listTagsIfConstrained`). |
| package versions | The tag list of `E/packages` once more (`discoverPackageNames`). |
| d8 CLI | `HEAD Rs/deckhouse-cli:<tag>` for `--deckhouse-cli-tag`, otherwise the tag list of `Rs/deckhouse-cli` (`resolveCLITag`). |
| plugins | When platform versions were selected, the tag lists of the platform plugins `Rs/deckhouse-cli/plugins/package` and `…/system`, and the manifests of their candidate versions (`resolvePlatform`). When module versions were selected, the tag list of `Rs/deckhouse-cli/plugins` and, for every plugin it names, relevant or not, its tag list and the manifest of its newest version (`resolveAuto`). The tag lists of ranged `--include-plugin` entries and of dependencies, the manifests of further versions tried, and the first child manifest of an index whose own manifest carries no contract (`ContractAnnotation` in [pkg/registry/service/plugin_service.go](../pkg/registry/service/plugin_service.go)). Each request at most once. |

With `--proxy-registry` no tag list is requested ([README "Proxy Registry Mode"](../internal/mirror/README.MD#proxy-registry-mode)). The platform range is resolved by a `HEAD E/release-channel:v<X.Y.Z>` per probed version (`releaseTagExists`); module and package names come from `--include-module` and `--include-package`, and a non-exact constraint is resolved by `HEAD E/M/<name>:v<X.Y.Z>` and `HEAD E/packages/<name>:v<X.Y.Z>` probes (`probeModuleTags`, `probePackageTags`); the package versions phase requests nothing; the d8 CLI is checked only when `--deckhouse-cli-tag` pins it; plugins are read only as the manifests of exact `--include-plugin` pins. Channel requests are unchanged.

`extractImageDigestsFromRemote` reads `E/install:<tag>` with `ExtractFileFromImage` ([pkg/libmirror/images/extract_file.go](../pkg/libmirror/images/extract_file.go)): it walks the layers from the top, opens each one as gzip, reads every layer that does not hold the file to its end, matches tar entry names exactly and ignores whiteouts; it never closes a layer response, and the config blob is not read. It walks for `deckhouse/candi/images_tags.json` first, and from the top again for `deckhouse/candi/images_digests.json` when the first walk fails or finds an empty file. The install images of deckhouse/deckhouse (main at cf83913cd9, 2026-10-01) carry `/deckhouse/candi/images_digests.json` and no `images_tags.json`: `.werf/defines/installer.tmpl` imports the former `before: setup`, so the layers of later build stages lie above it, and `.gitlab/scripts/promote-image.sh` reads it; nothing in that repository writes the latter, and its only reader there, the registry-syncer (`modules/038-registry/images/registry-syncer/src/internal/fill/release.go`), looks for it first, as d8 does. A dry-run therefore reads every layer of every selected install image, and the layers from the top down to the one holding `images_digests.json` a second time (§11.2).

Apart from the access check of a pinned tag (`HEAD E:<tag>`) and the `--proxy-registry` probes, no request concerns `E:<version>`, a component image, `E/install-standalone:<version>`, `E/release-channel:<version>`, or a module or package version image; and no request concerns a `release:<version>` or `version:<version>` image, an extra image, a VEX attestation, a security database other than `trivy-db:2`, or a layer of the d8 CLI or of a plugin.

## 6. Files

| Path | In a dry-run |
|---|---|
| `<dir>` | created when missing (§3.1); nothing is written into it outside `<tmp>` |
| `<tmp>` | created (§3.1) |
| `<tmp>/installer/installer/` | scaffolding, written at start even with `--no-installer` (`NewImageLayouts` from `NewService` in [installer/installer.go](../internal/mirror/installer/installer.go)); a failure to write it is only a warning |
| `<tmp>/security/<db>/` for the four databases | scaffolding, written at start even with `--no-security-db` (`createOCIImageLayoutsForSecurity` from `NewService` in [security/security.go](../internal/mirror/security/security.go)); a failure to write it is only a warning |
| `<tmp>/modules/<name>/`, `<tmp>/modules/<name>/release/` | scaffolding for every module that passed the filter (`createOCIImageLayoutsForModules`); a failure to write it fails the dry-run |
| `<tmp>/packages/<name>/`, `<tmp>/packages/<name>/version/` | scaffolding for every package that passed the filter (`createOCIImageLayoutsForPackages`); a failure to write it fails the dry-run |
| `<tmp>/platform/`, `<tmp>/package-versions/`, `<tmp>/deckhouse-cli/`, `<tmp>/plugins/` | not written: `NewService` in [platform/platform.go](../internal/mirror/platform/platform.go) skips its layouts in dry-run, and the others are written only right before a read |
| archives, chunks, `.tmp` staging files, `.gostsum` files, `deckhousereleases.yaml` | never written |

No cleanup removes the scaffolding: a dry-run runs `cleanupWorkingDirectory` (§3.2), which does not touch it, and skips `finalCleanup`, and the `finalCleanup` of a real pull keeps `<tmp>` (bundle format [§7.2](mirror-bundle-layout.md#72-working-directory)). A later real pull that shares `<tmp>`, which by default is any pull into the same `<dir>`, consumes it instead. Its security phase, which runs before the modules phase, packs every file of `<tmp>` into `security.tar` and deletes it (§11.1); without a `security.tar`, the modules and packages phases pack and delete the layouts of the items that store images and leave the rest. A real pull leaves scaffolding too, for every module and package that stored no image (bundle format [§7.2](mirror-bundle-layout.md#72-working-directory)). An operator SHOULD therefore start every real pull with a `<tmp>` that no earlier pull or dry-run used, for example by deleting `<tmp>` first or by giving the dry-run a `--tmp-dir` of its own.

## 7. The plan

### 7.1. Output format

The plan is part of the log that `d8 mirror pull` writes to standard output (`SLogger` in [pkg/libmirror/util/log](../pkg/libmirror/util/log/)). Every line carries a timestamp, a level and ANSI colour codes, and the lines logged inside a progress block (`╔ Pull Modules` … `╚`) are prefixed with `║`. A plan block is a heading that starts with `[dry-run]`, followed by indented references, `<repository>:<tag>` or `<repository>@sha256:<hex>`; other log lines, such as `Module found: <name>` and warnings, are interleaved.

The order within a block is:

- platform groups: sorted byte-wise (`slices.Sorted` in `pullDeckhousePlatformDryRun`);
- module and package versions: first the versions the channel images name, in the channel order `alpha`, `beta`, `early-access`, `stable`, `rock-solid`, `lts`, then the versions a constraint adds, in an unspecified order (Go map iteration in `filterOnlyLatestPatches` in [modules/filter.go](../internal/mirror/modules/filter.go)) with restored `>=` and `<=` anchors last; each version once (`mergeAndDedupeVersions`);
- plugins: by name, and each plugin's versions newest first (`result` in [dist/resolver.go](../internal/mirror/dist/resolver.go));
- security databases, and module and package channel lines: unspecified (Go map iteration in `pullSecurityDatabases` and in the `printDryRunPlan` functions of the modules and packages services).

An unspecified order changes between runs. The plan is a log, not an interface: an operator MUST NOT rely on its format or its order.

### 7.2. Platform

Printed by `pullDeckhousePlatformDryRun`. A tag-pinned pull first logs `Skipped releases range discovery as tag "<tag>" is specifically requested with --deckhouse-tag`, also for an exact `--include-platform` (`versionsToMirror`). The phase then logs `Deckhouse releases to pull: [<versions>]`, which lists the selected semver versions without `v` and leaves custom tags out (`findTagsToMirror`), then `Searching for Deckhouse built-in modules digests`, then for every selected tag `[dry-run] Streaming installer metadata for <tag> from registry`, followed by `Deckhouse digests found: <n>` or by the warning `[dry-run] Could not extract images from installer "<tag>": <error>`. The selected versions come out of a Go map (`deduplicateVersions`), so their order in these lines, and with it the counts of `Deckhouse digests found`, changes between runs. Then:

```
[dry-run] Platform images that would be pulled:
  Deckhouse components: <n> images
    <E>:<tag>                           one per selected tag
    <E>@sha256:<hex>                    one per image the install metadata lists
  Release channels: <n>
    <E>/release-channel:<channel>       every published channel that is not suspended
    <E>/release-channel:<tag>           one per selected tag
  Installer: <n>
    <E>/install:<tag>
  Standalone installer: <n>
    <E>/install-standalone:<tag>
  Total: <n> platform images
```

- The selected tags are those of bundle format [§6.1](mirror-bundle-layout.md#61-platformtar): the discovery result, or in a tag-pinned pull the pinned version or custom tag, or the version a pinned channel name points to.
- The component references are the values of `images_digests.json` as `<E>@<value>`, or those of `images_tags.json` as `<E>:<value>` when that file is found and not empty (§5), merged over all tags without duplicates; when no install image can be read, there are none (§11.7). `Deckhouse digests found: <n>` counts the references not already found for an earlier tag (§11.8).
- The channels are those the source publishes among `lts`, `alpha`, `beta`, `early-access`, `stable` and `rock-solid`, without the suspended ones unless `--ignore-suspend` is given (`getReleaseChannelInfoFromRegistry`). A suspension fails the dry-run as it fails a real pull (`checkSuspendedChannels`, which runs before any narrowing): a discovery pull fails on any suspended channel, also on one that `--since-version` or `--include-platform` would exclude, and a tag-pinned pull fails only when the name or the current version of a suspended channel equals the tag.
- `<n>` is the number of distinct references in the group and `Total` their sum. It counts references, not images: `release-channel:stable` and `release-channel:<version>` name one manifest and count twice.
- Not listed: the VEX attestations of the components, which a real pull adds unless `--skip-vex-images` is given (§11.3), and the channel aliases of `install/` and the re-pointed channels of `release-channel/`, which are descriptors rather than images (bundle format [§6.1](mirror-bundle-layout.md#61-platformtar)).
- Of these references the dry-run reads the `release-channel:<channel>` images and the install images. It also sends `HEAD E:<tag>` for a pinned tag that is not a channel name and, with `--proxy-registry`, a `HEAD` of every `release-channel:<version>` it probes. It requests none of the others (§5).

### 7.3. Installer

```
[dry-run] Installer images that would be pulled:
  <Rs>/installer:<tag>
```

`<tag>` is `--installer-tag`, by default `latest`. The block is printed only when `HEAD Rs/installer:<tag>` succeeds (`validateInstallerAccess`). On any failure of that check, a missing tag, a denied access or a network error, the phase logs the warning `installer access: <error>`, prints no block and reports itself as `not pulled` (`accessSkipped` in [installer/installer.go](../internal/mirror/installer/installer.go)), as in a real pull.

### 7.4. Security databases

```
[dry-run] Security database images that would be pulled:
  <E>/security/trivy-db:2
  <E>/security/trivy-bdu:1
  <E>/security/trivy-java-db:1
  <E>/security/trivy-checks:0
```

The block is printed when `<E>/security/trivy-db:2` exists (`securityDatabasesAvailable`), with the four lines in unspecified order (`pullSecurityDatabases`). The other three databases are not checked, although a real pull skips a missing one (bundle format [§6.3](mirror-bundle-layout.md#63-securitytar)). When the check answers not found, the phase logs the warning `Security databases are not available in this edition, skipping` and prints no block; any other failure of the check fails the dry-run. A real pull behaves the same.

### 7.5. Modules

The phase logs `Module found: <name>` for every module that passes the filter and `Repo contains <n> modules to pull`, then, inside a `Pull Modules` progress block (`Pull Extra Images` with `--only-extra-images`), prints one block per module in catalog order (`printDryRunPlan` in [modules/modules.go](../internal/mirror/modules/modules.go)):

```
[dry-run] Module '<name>' images that would be pulled:
  <E>/<M>/<name>/release:<channel>
  <E>/<M>/<name>:<version>
  (extra images discovery requires a real pull)
```

- Channel lines: `alpha`, `beta`, `early-access`, `stable` and `rock-solid`, whether the module publishes them or not, plus `lts` when its `HEAD` succeeds. There are none when every `--include-module` entry for the module is an exact pin (`ShouldMirrorReleaseChannels` in [modules/filter.go](../internal/mirror/modules/filter.go)), or with `--only-extra-images` (`discoverChannelVersions`); then no channel image is read, and the versions come from the constraint alone.
- Version lines: every selected version once, in the order of §7.1: the versions the channel images name in `version.json`, then those the constraint adds ([README "Module Filtering"](../internal/mirror/README.MD#module-filtering-1)).
- The last line is printed when at least one version is selected.
- Not listed, although a real pull stores them (bundle format [§6.4](mirror-bundle-layout.md#64-module-nametar)): `release:<version>` for every version, the images each version's `images_digests.json` lists, the extra images its `extra_images.json` names, and, unless `--skip-vex-images` is given, the VEX attestations a real pull looks up in the module's own repository (`pullVexImages`, `findVexImage`): `<tag>.att` for a version image, `sha256-<hex>.att` for an internal image, and `<extra-tag>.att` for an extra image, which is not looked up in the extra image's repository; `release:` images get none (§11.3).
- With `--only-extra-images` the version lines name images a real pull reads for `extra_images.json` but does not store (§11.4).
- A module that publishes nothing is listed with its five channel lines and counted in the summary, while a real pull writes no archive for it (§11.5).

### 7.6. Packages

As §7.5, with `Package found: <name>`, `Repo contains <n> packages to pull`, a `Pull Packages` progress block and this block (`printDryRunPlan` in [packages/packages.go](../internal/mirror/packages/packages.go)):

```
[dry-run] Package '<name>' images that would be pulled:
  <E>/packages/<name>/version:<channel>
  <E>/packages/<name>:<version>
  (extra images discovery requires a real pull)
```

Not listed: `version:<version>` and the internal, extra and VEX images (bundle format [§6.5](mirror-bundle-layout.md#65-package-nametar)). With `--only-extra-images` the phase runs even when `--no-packages` is given (bundle format [§7.3](mirror-bundle-layout.md#73-phases)). With `--proxy-registry` it takes the package names from `--include-package`, and fails without that flag (`discoverPackageNames`; §9).

### 7.7. Package versions

```
[dry-run] package-versions archive would contain release images for: <name>, <name>, …
```

The line names every tag of `<E>/packages` in listing order, whatever `--no-packages`, `--include-package` and `--exclude-package` say (`discoverPackageNames`). With `--proxy-registry` it names the `--include-package` names, sorted, without any request; without `--include-package` the phase logs `Skipping package release images (package-versions): --proxy-registry requires a whitelist of packages (--include-package)` instead. The line is not printed when the catalog is missing or empty, and any other listing error gives the warning `Skipping package release images (package-versions): <error>`. It names packages, not images, and does not tell whether a real pull writes `package-versions.tar` (bundle format [§6.6](mirror-bundle-layout.md#66-package-versionstar); §11.6).

### 7.8. d8 CLI

```
[dry-run] Deckhouse CLI that would be pulled:
  <Rs>/deckhouse-cli:<version>
```

`<version>` is the pinned `--deckhouse-cli-tag`, checked with a `HEAD`, and any failure of that check, absent, denied or unreachable, fails the dry-run as it fails a real pull. Without a pin it is the highest tag of `<Rs>/deckhouse-cli` that Masterminds semver parses, leniently, so that `1.0` counts, and whose pre-release does not start with `alpha`, `beta`, `rc`, `pre`, `preview` or `snapshot`, so that `v0.14.0-main` counts (`resolveCLITag` in [dist/cli.go](../internal/mirror/dist/cli.go), `stableVersions` and `sortedSemverDesc` in [dist/catalog.go](../internal/mirror/dist/catalog.go), `IsGenuinePrerelease` in [internal/plugins/requirements/checks.go](../internal/plugins/requirements/checks.go)). When no version is selected, the phase logs the warning `d8 CLI not mirrored: <reason>` instead of the block, `<reason>` being `not available in this registry` after any listing error, `no published versions`, or, with `--proxy-registry` and no pin, `no version listing over a proxy registry; pin it with --deckhouse-cli-tag <version>`. The phase is skipped with `--only-extra-images`.

### 7.9. Plugins

The resolver's warnings and the `Skipping plugin <name>: <reason>` lines come first, then:

```
[dry-run] Plugins that would be pulled:
  <Rs>/deckhouse-cli/plugins/<name>:<version>
```

with one line per selected plugin version, the plugins in name order and each plugin's versions newest first (`result` in [dist/resolver.go](../internal/mirror/dist/resolver.go)), or the line `No plugins to mirror`. The resolver takes the platform versions of phase 1 and the module versions the modules phase reports (`pluginsInput` in [pull.go](../internal/mirror/pull.go), `Stats` in [modules/stats.go](../internal/mirror/modules/stats.go)): in a dry-run every module that passed the filter, with its selected versions, and in a real pull only the modules that stored at least one image. The two selections agree when every module with a selected version stores an image in the real pull; otherwise the dry-run can plan plugins that the real pull does not mirror (§11.9). The phase is skipped with `--only-extra-images`.

### 7.10. Plan and bundle

The images a real pull with the same flags stores (bundle format [§6](mirror-bundle-layout.md#6-archive-contents)), and how the plan covers them:

| Images a real pull stores | In the plan | Requested by the dry-run |
|---|---|---|
| `E:<version>` | yes | `HEAD E:<tag>` for a pinned tag that is not a channel name, otherwise no |
| component images | when an install image was read (§11.7) | no |
| VEX attestations of the components | no | no |
| `E/release-channel:<channel>` | yes | yes |
| `E/release-channel:<version>` | yes | `HEAD` of the probed versions with `--proxy-registry`, otherwise no |
| `E/install:<version>` | yes | yes, every layer, the upper ones twice (§5) |
| `E/install-standalone:<version>` | yes | no |
| `Rs/installer:<tag>` | yes | `HEAD` |
| security databases | all four | `HEAD` of `trivy-db:2` only |
| module and package channel images | the five default channels, published or not, and `lts` when it exists; none for an all-exact pin or with `--only-extra-images` | yes, and a missing one stays in the plan |
| module and package version images | yes, also with `--only-extra-images`, where a real pull reads them without storing them | `HEAD` of the probed versions with `--proxy-registry`, otherwise no |
| `release:<version>`, `version:<version>` | no | no |
| images listed by a version's `images_digests.json` | no | no |
| extra images | no | no |
| VEX attestations of the version, internal and extra images | no | no |
| images of `package-versions.tar` | the `version:<channel>` images of the packages the packages phase lists, as their channel lines; otherwise only the package names | the channel lines as above |
| `Rs/deckhouse-cli:<version>` | yes | the tag list, or `HEAD` of the pinned tag |
| plugin versions | yes, when the two selections agree (§7.9) | the manifest |

### 7.11. Example

A dry-run with `--source <registry>/deckhouse/ee --insecure --no-pull-resume --verbose-summary` and no filters, against a test registry that publishes:

- `E/release-channel` tags `alpha` and `beta` naming v1.72.1, `early-access` and `stable` naming v1.71.2 and `rock-solid` naming v1.70.3, no `lts`, and the three version tags;
- `E:<version>`, `E/install:<version>` and `E/install-standalone:<version>` for the three versions, the install images being single-layer and holding an `images_digests.json` that lists two component images for v1.70.3 and three for the other two versions, all stored by digest in `E`, one of them with the VEX attestation `E:sha256-<hex>.att`;
- module `foo`: `release:stable` naming v1.2.0, `release:alpha` naming v1.3.0, `release:v1.2.0`, `release:v1.3.0`, `foo:v1.3.0`, and `foo:v1.2.0` with an `images_digests.json` that lists one internal image stored by digest, an `extra_images.json` of `{"scanner":"v3"}` and the VEX attestation `foo:v1.2.0.att`; `foo/extra/scanner:v3`; module `bar` with nothing but its catalog tag;
- package `pkg1`: `version:stable` naming v0.3.0, `version:v0.3.0` and `pkg1:v0.3.0`; package `pkg2` with nothing but its catalog tag;
- `E/security/trivy-db:2` and `E/security/trivy-bdu:1`, `Rs/installer:latest`, `Rs/deckhouse-cli:v0.13.1`, and the plugin `foo-tool` v1.0.0 with its catalog tag and a contract that requires module `foo` `>=1.0.0`;

printed in one run (timestamps and colours removed, digests shortened, the address of the test registry replaced by `registry.example.com`):

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

Other runs print the platform versions in another order, with other digest counts (§7.2), and the security and channel lines in another order (§7.1). A real pull with the same flags against the same registry stored 45 descriptors; §11.3 and §11.5 compare them with these 50 planned references.

## 8. The summary

After the phases `Puller.Execute` prints the framed block of `renderPullSummary` ([cmd/pull/summary.go](../internal/mirror/cmd/pull/summary.go)), titled `Pull plan (dry-run)`, or `Pull failed` when a phase returned an error. A failure in validation, `cleanupWorkingDirectory` or `buildPullService` prints no block (§3.1). The block never contains `Bundle artifacts`, and its state line is `No images were downloaded (dry-run).`, `Pull failed; the above reflects what completed before the error.` or `Pull was cancelled; the above reflects what completed.`. Unlike a real pull, a cancelled dry-run does not log `Operation cancelled by user`. The component lines read:

| Line | In a dry-run | In a real pull |
|---|---|---|
| `Platform` (`writeComponent`) | the selected versions and the number of selected channels | the same |
| `Installer` (`writeComponent`) | the tag, or `not pulled` when the access check failed | the same |
| `Security` (`writeSecurity`) | `4/4 databases` whenever `trivy-db:2` exists: the number of planned database sets (`Stats` in [security/stats.go](../internal/mirror/security/stats.go)) | the number of databases that stored an image |
| `Modules`, `Packages` (`writeModules`, `writePackages`) | every item that passed the filter, also one that publishes nothing; never a VEX count | the items that stored at least one image, with their VEX count |
| `d8 dist` (`writeDeckhouseCLI`) | the selected version, or `not mirrored - <reason>` | the same, and `not mirrored - could not be pulled at <tag>` when the download of an automatically selected version fails |
| `d8 plugins` (`writePlugins`) | the number of selected plugins with their provenance breakdown, then the skipped plugins and the warnings | the same, for the selection of the real pull (§7.9) |

With `--verbose-summary` the module and package lines are followed by every item with its selected versions, and the plugin line by the plugins grouped by module (`writePluginsTree`), the same in both modes. Of the planned counts that the services' `Stats` methods compute in dry-run, only the security database count is printed.

## 9. Exit status and what a dry-run proves

The exit status is 0 when the run completes, and when it is cancelled, that is when the returned error wraps `context.Canceled` (`classifyPullOutcome` in [cmd/pull/pull.go](../internal/mirror/cmd/pull/pull.go)). Any other returned error, a validation error included (§3.1), exits 1 (`execute` in [cmd/d8/root.go](../cmd/d8/root.go)). Nothing in d8 recovers a panic, which therefore exits 2. A real pull also exits 1 when computing the GOST checksums or the final cleanup fails.

| Condition | Dry-run | Real pull |
|---|---|---|
| a validation error, a malformed filter constraint, a non-empty `<dir>` without `--force` (§3.1) | exit 1 | exit 1 |
| the source unreachable or rejecting the credentials while the platform, security, modules or packages phase runs (for modules and packages, not with `--proxy-registry`) | exit 1 | exit 1 |
| the same while only the installer, package versions, d8 CLI and plugins phases run | warnings, exit 0 | the same |
| a pinned platform tag (`--deckhouse-tag`, an exact `--include-platform`) not in the source, unless `--no-platform` is given (`validatePlatformAccess`) | exit 1 | exit 1 |
| a suspended channel the request resolves to, without `--ignore-suspend` (§7.2) | exit 1 | exit 1 |
| the modules catalog `E/M` missing | the warning `Skipping pull of modules: <error>`, then exit 1 (`validateModulesAccess`, `discoverModuleNames`) | the same |
| `--proxy-registry` without `--include-package` while the packages phase runs (§7.6) | exit 1 | exit 1 |
| `--deckhouse-cli-tag` not in the source, unless `--only-extra-images` is given | exit 1 | exit 1 |
| a failed access check of `Rs/installer:<tag>` | warning, installer skipped | the same |
| `E/security/trivy-db:2` not found | warning, security skipped | the same |
| `E/install:<tag>` missing | warning, exit 0 | panic, exit 2 (bundle format [§11.10](mirror-bundle-layout.md#11-known-deviations)) |
| an install image with neither metadata file | warning, exit 0 | exit 1 (`both files is not found in installer`) |
| an install image whose metadata entry is `./`-prefixed or whose layers are not all gzip (§11.7) | warning, exit 0, no component planned | the components stored |
| `E:<version>` of a version the dry-run did not pin, a component image, or, in a discovery pull, `E/release-channel:<version>` missing | exit 0, nothing logged | exit 1 |
| a lookup of a platform VEX attestation failing with anything but not found (`FindVexImage`) | exit 0, no lookup | exit 1 |
| a persistent registry error on a module or package version image, which a real pull reads for `extra_images.json` | exit 0 | exit 1 |
| a disk failure creating `<dir>`, `<tmp>` or module or package scaffolding | exit 1 | exit 1 |
| a read, disk or packing failure while storing | not reached | exit 1 |

The table lists the common failures and every condition known to differ between the two modes; it is not exhaustive. A dry-run that exits 0 shows that the flags are valid and that selection succeeds, and, when the platform, security, modules or packages phase ran, that the source answers with the given credentials. It does not show that the planned images exist, that the real pull succeeds, or how large the bundle gets.

## 10. Tests

`go test -count=1 -run DryRun ./internal/mirror/...` runs, offline and against fake registries, the tests whose names contain `DryRun`; it passes. Tests with other names pin dry-run rules as well:

| Test | What it pins |
|---|---|
| `TestDryRunFlagRegistered` in [cmd/pull/pull_dryrun_test.go](../internal/mirror/cmd/pull/pull_dryrun_test.go) | `--dry-run` exists and defaults to `false` |
| `TestDryRunNoBundleOutput` (no `.tar`, `.chunk` or `.gostsum` in `<dir>`), `TestDryRunNoBundleWithNoPlatform` (no `.tar` or `.chunk`), `TestDryRunWithDeckhouseTag` (no `.tar`) and `TestDryRunExitsZeroOnSuccess` (no error), in the same file | a dry-run through `Puller.Execute`, which bypasses validation (§3.1) and the exit status (§9); all four turn `--gost-digest` off |
| `TestPullerExecute_DryRun_PluginsNoFiles` in [cmd/pull/pull_plugins_stub_test.go](../internal/mirror/cmd/pull/pull_plugins_stub_test.go) | no `.tar` or `.chunk` in `<dir>` after a dry-run that runs the plugins phase; it does not check that a plugin was selected |
| `TestDryRun_NoBundleFilesWritten` in the platform, installer and packages packages | the phase writes nothing into the bundle directory. The tests of that name in the security and modules packages give the stub registry an edition its root already contains, find nothing under `…/fe/fe/` and return before the dry-run branch |
| `TestDryRun_WorkingDirHasLayouts` in the installer and security packages | their scaffolding in the working directory (§6); the modules test of that name checks only the bundle directory |
| `TestDryRun_NoOCILayoutCreated` in [platform/platform_dryrun_test.go](../internal/mirror/platform/platform_dryrun_test.go) | nothing: it builds `Service` without `NewService` and checks a directory the service is never given, so the platform row of §6 is not covered |
| `TestPullPlatform_DryRun_*`, `TestPullPlatform_ErrorWhen*` and `TestPullPlatform_IgnoreSuspend_SucceedsWithSuspendedChannel` in [platform/pull_platform_test.go](../internal/mirror/platform/pull_platform_test.go) | platform selection in dry-run: suspended channels, pinned, channel and custom tags, `--since-version`, `--include-platform`, LTS-only registries, missing tags (§7.2, §9) |
| `TestPlatformDownloadList_FilledCorrectly`, `TestInstallerDownloadList_FilledCorrectly`, `TestSecurityDownloadList_FilledCorrectly`, `TestModulesReleaseChannelDownloadList_FilledCorrectly`, `TestPackageVersionChannelDownloadList_FilledCorrectly`, and the `Test*Service_RootURL_*` tests of the installer, security, modules and packages packages | the references a dry-run puts into the download lists, rooted at E, or at Rs for the installer |
| `TestPullPackageVersions_DryRun` in [packages/packages_test.go](../internal/mirror/packages/packages_test.go) | the package versions phase writes nothing |
| `TestStats_DryRun` in [installer/stats_test.go](../internal/mirror/installer/stats_test.go) and [packages/stats_test.go](../internal/mirror/packages/stats_test.go), `TestStats_DryRun_AccessFailure_ReportsNotAttempted` | the planned image count of the installer; the names and versions of the packages; `Attempted` false after a failed installer check, which the summary renders as `not pulled` (§8) |
| `TestPullCLI_DryRun` in [dist/cli_test.go](../internal/mirror/dist/cli_test.go), `TestPullPlugins_DryRun` in [dist/pull_plugins_test.go](../internal/mirror/dist/pull_plugins_test.go) | the version is recorded, no image is stored, no file is written |
| `TestPullE2E_DryRun_ResolutionParityNoFiles` in [pull_plugins_e2e_test.go](../internal/mirror/pull_plugins_e2e_test.go) | on the fixture of `TestPullE2E_ModuleVersionsReachPluginResolver`, which runs a real pull, the dry-run selects the same plugin versions; the provenance is only checked to be non-empty |
| the subtests `dry-run, security unavailable` and `cancelled during dry-run shows cancellation, not the dry-run footer` of `TestRenderPullSummary` in [cmd/pull/summary_test.go](../internal/mirror/cmd/pull/summary_test.go) | the title, the state lines and the missing `Bundle artifacts` block (§8) |

`TestDryRunRealRegistry` in [cmd/pull/pull_realregistry_test.go](../internal/mirror/cmd/pull/pull_realregistry_test.go) runs a tag-pinned dry-run against a real registry and is skipped unless both variables are set:

```
D8_TEST_REGISTRY=registry.deckhouse.io/deckhouse/fe D8_TEST_LICENSE_TOKEN=<token> \
  go test -count=1 -run TestDryRunRealRegistry -v -timeout 300s ./internal/mirror/cmd/pull/
```

No test compares a plan with the bundle a real pull writes from the same registry, and none covers the module or package scaffolding or the skipped platform layouts, so no item of §11 is caught by the suite.

## 11. Known deviations

Verified on 2026-10-01 and 2026-10-02 by running `PullService` with and without `DryRun` against an in-memory OCI registry served over HTTP (go-containerregistry `pkg/registry`) through the production registry client with every request logged, by running the `d8 mirror pull` command code (`NewCommand`) against the same registry and against the stub registry (`STUB_REGISTRY_CLIENT=true`), and by pushing the resulting bundles with `PushService`. Each item contradicts the `--dry-run` help text ("Print what would be pulled without downloading any images. Useful for fast validation of flags and filters."), README ("Print what would be pulled without downloading any images or writing a bundle"), the summary line `No images were downloaded (dry-run).`, a rule above, or the expectation that a dry-run changes nothing a later pull depends on.

1. A dry-run leaves scaffolding that corrupts the next real pull sharing its `<tmp>`. Any earlier run that shares `<tmp>` does this: a real pull leaves the layouts of every module and package that stored no image (bundle format [§7.2](mirror-bundle-layout.md#72-working-directory)), and a dry-run leaves them for every module and package that passed its filter (§6). The next real pull that writes `security.tar` packs them into it before its modules phase starts (bundle format [§11.1](mirror-bundle-layout.md#11-known-deviations)). Push skips the empty layouts (bundle format [§8.6](mirror-bundle-layout.md#86-layouts-and-destinations)) but writes a discovery tag for every directory under `modules/` and `packages/` (bundle format [§8.8](mirror-bundle-layout.md#88-discovery-tags)). Since `<tmp>` defaults to `<dir>/.tmp`, `d8 mirror pull --dry-run bundle/` followed by `d8 mirror pull bundle/` is enough. Verified with modules `foo` and `bar` and packages `pkg1` and `pkg2`, where `bar` and `pkg2` publish nothing: after a dry-run, the real pull with the same flags wrote a `security.tar` holding `modules/{bar,foo}/…` and `packages/{pkg1,pkg2}/…`, and its push gave the target the tags `modules:bar` and `packages:pkg2` without any `modules/bar` or `packages/pkg2` repository; the same pull with a fresh `<tmp>` gave only `modules:foo` and `packages:pkg1`. After an unfiltered dry-run, a real pull with `--include-module` and `--include-package` filters gave a tag for every module and package the dry-run had passed, also for one that publishes images but that the real pull filtered out. Two real pulls in a row with the same `<tmp>` and no dry-run gave the tags of the items that publish nothing as well, and a real pull that writes no `security.tar` (`--no-security-db`, or no `trivy-db:2` in the source) leaves the scaffolding it did not pack itself to the next one.
2. A dry-run reads every selected install image (§5). For each selected tag, `extractImageDigestsFromRemote` walks `E/install:<tag>` from the top for `deckhouse/candi/images_tags.json`, reading each layer that lacks it to its end, and, when that walk fails or finds the file empty, walks again from the top for `deckhouse/candi/images_digests.json`. The install images of deckhouse/deckhouse carry no `images_tags.json` and hold `images_digests.json` below the layers of later build stages (§5), so each one is read once in full and its layers from the top down to the file a second time. The second walk never closes its responses, so more than that crosses the wire. Verified: with single-layer install images every layer was requested twice; with three-layer ones holding the file in the bottom, middle or top layer, the layers from the top down to that one were requested twice and those below it once; an `images_tags.json` in the top layer was found with a single partial read; and in one probe the server sent a 1.1 MB layer in full a second time while the client read 98 KB of it. The release-channel images of the platform, the modules and the packages are read too; selection needs them. This makes the summary line `No images were downloaded (dry-run).` untrue as well.
3. The plan omits images a real pull stores (§7.10): the VEX attestations of the platform components; for every module and package version, its `release:<version>` or `version:<version>` image, the images its `images_digests.json` lists and its extra images; the VEX attestations of the version, internal and extra images; and the images of `package-versions.tar` that no package block lists. The only hint, `(extra images discovery requires a real pull)`, names extra images alone. Verified with the registry of §7.11: of the 45 descriptors the real pull stored, 13 are not in the plan. Five are the channel aliases of `install/`, which are descriptors rather than images; the other eight are seven images, namely the platform VEX attestation, the internal image, the VEX attestation, the extra image, `release:v1.2.0` and `release:v1.3.0` of `foo`, and `version:v0.3.0` of `pkg1`, which is stored both in `package-pkg1.tar` and in `package-versions.tar`. The remaining 32 descriptors are 31 planned references, `version:stable` of `pkg1` being stored in the same two archives.
4. For every module and package with a version constraint, `--only-extra-images` inverts the plan. A real pull in this mode stores only the extra images of the selected versions and their VEX attestations, reading each version image for `extra_images.json` without storing it, while the plan lists these version images `<E>/<M>/<name>:<version>` and no extra image. Without a constraint no version is selected in this mode: the block is empty, and the real pull stores nothing for the module. Verified with `--only-extra-images --include-module foo@~1.2.0 --no-packages --no-platform --no-installer --no-security-db`: the plan named `modules/foo:v1.2.0`, and of the module images the real pull stored only `modules/foo/extra/scanner:v3`, besides which it wrote only `package-versions.tar`. The packages phase ran despite `--no-packages` (§7.6) and listed `pkg1` and `pkg2` with empty blocks, so the dry-run summary read `Packages:   2  ·  extra images only` where the real pull's read `Packages:   0  ·  extra images only`.
5. The plan and the summary present references whose existence the dry-run does not establish (§5): it requests every default channel image of a module or package but keeps a missing one, checks no version image and only `trivy-db:2` of the four databases, and lists and counts items that publish nothing. Verified with the registry of §7.11: 19 of the 50 planned references did not exist (every reference of `bar` and `pkg2`, three channels of `foo`, four of `pkg1`, `trivy-java-db:1` and `trivy-checks:0`), and the dry-run summary read `Security:   4/4 databases`, `Modules:    2` and `Packages:   2`, where the real pull's read `Security:   2/4 databases`, `Modules:    1  ·  1 VEXes` and `Packages:   1`.
6. The package versions line (§7.7) names every package of the catalog, including one whose `version` repository is empty. Verified: the line named `pkg1, pkg2`, and the real `package-versions.tar` held only `pkg1`.
7. The install metadata is read differently from a real pull. `ExtractFileFromImage` compares each tar entry name with the file name as it is and opens every layer as gzip, while a real pull reads the flattened image through `mutate.Extract`, which cleans entry names with `filepath.Clean` and reads uncompressed, gzip and zstd layers. For install images that store `./deckhouse/candi/images_digests.json`, the dry-run logged `Could not extract images from installer` for every version, read every install layer twice and planned no component image, while the real pull stored all three component images. For an install image with a zstd top layer the dry-run logged `unzip layer: gzip: invalid header`, while `mutate.Extract` found the file. `ExtractFileFromImage` also ignores whiteouts: with `images_tags.json` deleted by a whiteout in an upper layer, the dry-run planned the references of the deleted file, while the real pull used `images_digests.json` and stored a different set of images.
8. Log lines misreport. `Creating OCI Image Layouts` is printed although the platform service writes no layout in dry-run (`NewService` in [platform/platform.go](../internal/mirror/platform/platform.go)), and `Creating OCI Image Layouts for Modules` and `Creating OCI Image Layouts for Packages` are printed even with `--no-modules` or `--no-packages`, when no layout is ever written (`NewService` in [modules/modules.go](../internal/mirror/modules/modules.go) and [packages/packages.go](../internal/mirror/packages/packages.go)). `Deckhouse digests found: <n>` counts only the references not found for an earlier tag, in an order that changes between runs (§7.2): for the three installers of §7.11, listing 2, 3 and 3 images, 20 runs logged 2, 1 and 0 seven times and 3, 0 and 0 thirteen times, where a real pull, which reads each distinct install image once, logs the number of images each one lists, twice, also in an order that changes between runs.
9. The plugin plan can differ from the plugins a real pull mirrors (§7.9). The dry-run passes every module that passed the filter, with its selected versions, to the plugin resolver, and a real pull passes only the modules that stored an image (`Stats` in [modules/stats.go](../internal/mirror/modules/stats.go)). Verified with `--include-module foo@=v9.9.9`, a tag the source does not publish, and the plugin `foo-tool`, which requires `foo` `>=1.0.0`: the dry-run planned `modules/foo:v9.9.9` and `deckhouse-cli/plugins/foo-tool:v1.0.0`, read the plugin's manifest and summarised `Modules:    1` and `d8 plugins: 1  ·  1 for modules`, while the real pull logged `No plugins to mirror` and summarised `Modules:    0` and `d8 plugins: 0`; both exited 0.
