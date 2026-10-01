# `d8 mirror` bundle format

This document specifies the bundle that `d8 mirror pull` writes and `d8 mirror push` reads: the files of the bundle directory, the archive and OCI layout formats, the content of every archive, how pull produces it, and how push maps every image to a registry reference. It describes the implementation as of 2026-10-01, and every rule names the code that implements it. Where the implementation breaks the contract, §11 lists the deviation.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119. Rules for producers apply to anything that writes a bundle, `d8 mirror pull` or a hand-built archive; rules for the consumer describe what `d8 mirror push` does.

Flags, and which platform, module, package and plugin versions get selected, are described in [internal/mirror/README.MD](../internal/mirror/README.MD). This document covers structure and routing.

- [1. Terms](#1-terms)
- [2. The routing invariant](#2-the-routing-invariant)
- [3. Bundle directory](#3-bundle-directory)
- [4. Archive format](#4-archive-format)
- [5. Layout format](#5-layout-format)
- [6. Archive contents](#6-archive-contents)
- [7. How pull builds the bundle](#7-how-pull-builds-the-bundle)
- [8. How push maps the bundle to the registry](#8-how-push-maps-the-bundle-to-the-registry)
- [9. Writing a push-compatible archive by hand](#9-writing-a-push-compatible-archive-by-hand)
- [10. Example](#10-example)
- [11. Known deviations](#11-known-deviations)

## 1. Terms

| Term | Meaning |
|---|---|
| Bundle directory | The directory given to `d8 mirror pull <dir>`, and the usual input of `d8 mirror push <dir> <registry>`. |
| Archive | A tar file of the bundle, named `<name>.tar`, stored whole or as chunks (§4.3). |
| Layout | An [OCI image layout](https://github.com/opencontainers/image-spec/blob/main/image-layout.md): a directory holding `oci-layout`, `index.json` and `blobs/sha256/`. |
| Layout path | The slash-separated path of a layout directory relative to the archive root, empty for a layout at the root. |
| Short tag | The value of the `io.deckhouse.image.short_tag` annotation of an `index.json` descriptor: the tag the image gets in the target registry. |
| E, Rs | The edition root and the source root of pull, both derived from `--source` (§7.1). |
| T, R | The target repository of push and the root that receives `installer/` and `deckhouse-cli/` (§8.2). R equals T unless the target ends with an edition. |
| Edition | One of `ce`, `be`, `se`, `se-plus`, `ee`, `fe`, `cse` (`pkg.Edition` in [pkg/edition.go](../pkg/edition.go)). Matching is exact; repository paths are lowercase anyway (§8.1). |
| U | The unified tree: the directory into which push unpacks every archive before pushing (§8.5). |

## 2. The routing invariant

A bundle is a path-preserving copy of a registry tree, and push derives every destination from the bundle content alone:

- the repository of an image is `<base>/<layout path>`, where the base is T, or R for layout paths under `installer/` and `deckhouse-cli/` (§8.6);
- the tag of an image is its short tag (§5.3);
- `--modules-path-suffix` can rewrite the leading `modules` segment (§8.6);
- archive file names play no part, except for the legacy `module-<name>.tar` rule (§8.5).

Pull therefore writes the destination of every image into the archive, and push reads it back without a mapping table.

## 3. Bundle directory

### 3.1. Entries

| Entry | Kind | Present when | Read by push |
|---|---|---|---|
| `platform.tar` | archive | the platform phase ran (no `--no-platform`) | yes |
| `installer.tar` | archive | no `--no-installer`, and `<Rs>/installer:<tag>` exists | yes |
| `security.tar` | archive | no `--no-security-db`, and `<E>/security/trivy-db:2` exists | yes |
| `module-<name>.tar` | archive | one per module that yielded at least one image | yes |
| `package-<name>.tar` | archive | one per package that yielded at least one image | yes |
| `package-versions.tar` | archive | the source has a packages catalog with at least one version image; `--no-packages` and package filters do not apply, except with `--proxy-registry` (§6.6) | yes |
| `deckhouse-cli.tar` | archive | a d8 version was resolved and pulled; never with `--only-extra-images` | yes |
| `plugin-<name>.tar` | archive | one per selected plugin; never with `--only-extra-images` | yes |
| `<archive>.NNNN.chunk` | chunk | replaces `<archive>` when `--images-bundle-chunk-size` is above 0 (§4.3) | yes |
| `<file>.gostsum` | checksum | `--gost-digest`, one per `.tar` and `.chunk` file (§4.4) | no |
| `deckhousereleases.yaml` | YAML | the platform phase ran in discovery mode: no `--deckhouse-tag` and no exact `--include-platform "=…"` (§6.1) | no |
| `.tmp/` | directory | default working directory of pull and push (§7.2, §8.5) | no |
| `<archive>.tmp`, `<archive>.NNNN.chunk.tmp` | staging | while pull writes that archive (§7.6); left behind when pull is killed or renaming a part fails; ignored by `--gost-digest` too | no |

Push reads only regular files at the top level of the bundle directory whose names match §8.3, so the entries marked "no" are ignored by name.

### 3.2. Rules

- Pull requires the bundle directory to be empty or to hold nothing but a `.tmp` directory, unless `--force` is given (`validateImagesBundlePathArg` in [cmd/pull/validation.go](../internal/mirror/cmd/pull/validation.go)). With `--force`, files of earlier pulls stay, and are pushed and checksummed. An archive is replaced only when it is written in the same form: a stale plain `<name>.tar` shadows new `<name>.tar.NNNN.chunk` parts at push (§4.3), stale parts next to a new plain file are ignored by push but still checksummed, and stale higher-numbered parts of an earlier chunked pull are read after the end of the new archive and ignored.
- Monolithic components get one archive with a fixed name. Modules, packages and plugins get one archive per item, so adding or removing an item adds or removes one archive (several parts with chunking), plus, for a module, any plugin archives its contracts select.
- Pull writes archives in phase order (§7.3). The order carries no meaning for push.
- Every archive in the directory is a complete tar (§7.6), but a pull that exits 0 can still leave a partial bundle. A graceful cancellation (Ctrl+C, SIGTERM) exits 0 and the summary reports the pull as cancelled. A cancellation during the modules or packages download packs the items pulled so far, the interrupted one with partial content (§7.6); a cancellation while packing discards the archive being written and writes no further ones. A cancellation that hits a phase which turns listing errors into warnings (package versions, the automatic d8 CLI selection) is not even reported as such: the pull exits as successful and computes GOST checksums.

## 4. Archive format

### 4.1. Container

Producers write archives with Go's `archive/tar` in GNU format (`packFuncWithPrefix` in [pkg/libmirror/bundle/bundle.go](../pkg/libmirror/bundle/bundle.go)):

- an uncompressed tar with the GNU header magic `ustar  \0`, terminated by two zero blocks;
- regular-file entries only (typeflag `0`), no entries for directories, links or devices;
- entry names are slash-separated paths relative to the archive root, with no leading `/` and no `.` or `..` segments (except in `security.tar` with a `--tmp-dir` that is not a clean path, §11.1); names longer than 100 bytes use GNU long-name records (typeflag `L`, `././@LongLink`);
- normalized metadata: mode `0777`, uid and gid `0`, empty user and group names, mtime `0` (1970-01-01T00:00:00Z);
- entries follow a depth-first walk of the staging directory with lexically sorted directory entries (`filepath.Walk`); push does not depend on the order.

The consumer accepts any tar that Go's `archive/tar` reads (ustar, PAX, GNU), skips every entry that is not a regular file, re-roots absolute names under its extraction root, and aborts the archive at the first regular-file entry whose cleaned path leaves that root (§8.5).

### 4.2. Content

An archive is a file tree holding one or more layouts at arbitrary paths. Layouts MAY nest: `modules/foo/` and `modules/foo/release/` are two layouts, the second one inside the directory of the first. Files outside any layout are allowed and not pushed (§8.6), but a directory directly under `modules/`, `packages/` or `deckhouse-cli/plugins/` gets a discovery tag even when it holds no layout (§8.8). The top-level path `tmp/` is reserved: push extracts into `U/tmp` and deletes it afterwards (§8.5), so content under `tmp/` is discarded, or merged into a top-level path of the same name; producers MUST NOT use it.

### 4.3. Chunked archives

With `--images-bundle-chunk-size N` (N above 0, in decimal gigabytes, so the limit is L = N × 10⁹ bytes), pull stores each archive as parts instead of one file (`FileWriter` in [chunked/chunk_writer.go](../internal/mirror/chunked/chunk_writer.go)):

- part names are `<archive>.NNNN.chunk`, where `<archive>` is the full archive name (`platform.tar`) and NNNN the part index in decimal, zero-padded to at least four digits (`0000`, `0001`, …, `9999`, `10000`);
- the parts concatenated in index order are byte-for-byte the archive; the split points are arbitrary byte offsets, not tar entry boundaries;
- every part except the last is at least L and at most L + 524287 bytes long, because data is written in slices of up to 512 KiB and a part is closed once it reaches L; the last part is shorter than L and MAY be empty;
- parts are consecutive: the consumer reads `0000`, `0001`, … and stops at the first missing index (`Open` in [chunked/chunk_reader.go](../internal/mirror/chunked/chunk_reader.go)), so a missing part truncates the archive at that point.

The consumer recognises part names by `^(.+)\.tar\.\d{4,}\.chunk$` and collapses them to `<name>.tar` (§8.3). When a plain `<name>.tar` exists next to its parts, the plain file is used and the parts are ignored.

### 4.4. GOST checksums

With `--gost-digest`, once a pull has finished without failure or cancellation (`computeGOSTDigests` in [cmd/pull/pull.go](../internal/mirror/cmd/pull/pull.go)):

- for every file at the top level of the bundle directory whose extension is `.tar` or `.chunk`, files kept from earlier pulls included, pull writes `<file name>.gostsum` next to it (`platform.tar.gostsum`, `platform.tar.0000.chunk.gostsum`);
- the content is the GOST R 34.11-2012 256-bit (Streebog-256) hash of the file's bytes as 64 lowercase hexadecimal characters, with no file name and no trailing newline; the file mode is `0644`;
- a failure to compute or write a checksum fails the pull, and can leave an empty `.gostsum` behind.

The format is not a `sha256sum`-style list. `d8 tools gostsum <file>` prints `<file>: <hash>` with the same hash, for one file per invocation (§11.13). Push does not read checksum files.

## 5. Layout format

### 5.1. Files

A layout directory holds (`createEmptyImageLayout` in [pkg/registry/image/layout.go](../pkg/registry/image/layout.go)):

- `oci-layout`, containing `{"imageLayoutVersion": "1.0.0"}`;
- `index.json`, the image index (§5.2);
- `blobs/sha256/<hex>`: manifests, configs and layers, each named by the lowercase hex of its sha256 digest.

A layout MUST be self-contained: every blob its manifests and indexes reference is in its own `blobs/`. A nested layout shares no blobs with its parent. Archives that contribute to the same layout path share that layout's `blobs/` once unpacked.

### 5.2. `index.json`

- `schemaVersion` is `2`.
- `mediaType`: pull writes `application/vnd.oci.image.layout.v1+json`, which is the media type of the `oci-layout` file, not of an index (`application/vnd.oci.image.index.v1+json`). Consumers MUST NOT rely on this field; push ignores it.
- `manifests` lists the descriptors, or is `null` for an empty layout. An empty layout is valid, and push skips it.

### 5.3. Descriptors

| Field | Content |
|---|---|
| `mediaType` | The manifest media type as published by the source: an image manifest (`application/vnd.docker.distribution.manifest.v2+json`, `application/vnd.oci.image.manifest.v1+json`), or, in the d8 CLI and plugin layouts only, an image index (`application/vnd.oci.image.index.v1+json`, a Docker manifest list) for a multi-platform image (§7.4). |
| `digest`, `size` | Those of the manifest or index blob. |
| `annotations["io.deckhouse.image.short_tag"]` | The tag in the target registry. Required: push skips a descriptor without it. |
| `annotations["org.opencontainers.image.ref.name"]` | The full source reference the image came from, `<source repository>:<tag>` or `<source repository>@sha256:<hex>`; for an alias, the source repository with the alias tag, which need not exist in the source. Informational: unlike the OCI image-layout convention it is not a tag. Push does not route by it, but uses it as part of the merge key and as the merge sort key (§8.5), which can decide the winner among duplicate short tags. |
| `platform` | `{"architecture": "", "os": ""}` on images stored by pull, carrying no information; absent on indexes. |
| other fields | MAY be present (for example `artifactType`); ignored. |

- Several descriptors MAY point to one digest under different short tags. This is how channel aliases are stored (§6), and they cost no blobs.
- A short tag MAY occur more than once in a layout (tag-pinned platform pulls, §6.1). Push publishes the tag once, with the last such descriptor in `manifests` order.
- A descriptor whose `mediaType` is an index is pushed as a whole index, with all its children and the index annotations (such as a plugin contract); any other descriptor is pushed as one image.
- In the layouts pull post-processes (platform, security, modules, packages, package versions) descriptors are sorted by `ref.name` in byte order with Go's `sort.Slice`, which is not stable: descriptors with equal `ref.name` keep their relative order only in layouts of at most 12 descriptors. The exact-pin aliases of modules and packages follow the sorted list (§7.5). In the installer, d8 CLI and plugin layouts descriptors keep pull order. The order matters only for duplicate short tags.

### 5.4. Short tag forms

| Form | Example | Used in |
|---|---|---|
| version | `v1.72.1`, `v2.0.0-rc.1` | platform layouts, modules, packages, d8 CLI, plugins, as published |
| release channel | `alpha`, `beta`, `early-access`, `stable`, `rock-solid`, `lts` | `release-channel/`, `install/`, module `release/`, package `version/` |
| fixed | `latest`; `2`, `1`, `1`, `0` | installer (default of `--installer-tag`); trivy-db, trivy-bdu, trivy-java-db, trivy-checks |
| digest | 64 lowercase hex characters, the digest without `sha256:` | images the source references by digest (§6.1, §6.4) |
| VEX | `sha256-<hex>.att`, `<tag>.att` | vulnerability attestation of a digest- or tag-referenced image |
| other | `pr12345`, `v3` | custom `--deckhouse-tag` builds, tags from `extra_images.json`, tags listed in `images_tags.json` (platform root, taken verbatim) |

A digest-form image is pushed as `<repo>:<hex>`, which makes it addressable by tag (registries may garbage-collect untagged manifests). Push writes manifests unchanged, so `<repo>@sha256:<hex>`, the form the platform uses, resolves as well, provided the referenced digest is an image manifest and not an index (§7.4).

## 6. Archive contents

E and Rs are the edition root and the source root of pull, M the source modules path (§7.1). Every tag is classed by what a failure to pull it does:

- required: the pull fails;
- tolerated: a tag the registry does not resolve, for any reason other than cancellation (not found, access denied, a server error), is skipped with a `Not found in registry, skipping pull` warning; a download that fails after the tag resolved still fails the pull;
- best effort: any failure is logged only at debug level (`MIRROR_DEBUG_LOG=3`) and stops the remaining images of the same batch, so the layout can end up partial without a warning.

### 6.1. `platform.tar`

Staged in `<tmp>/platform` and packed without a prefix ([platform/platform.go](../internal/mirror/platform/platform.go)). The archive always holds all four layouts, some possibly empty.

| Layout path | Source repository | Short tags |
|---|---|---|
| *(root)* | `E` | `vX.Y.Z` of every selected version (required); every image listed in `deckhouse/candi/images_tags.json` of every pulled `install` image, or in `deckhouse/candi/images_digests.json` (digest form) when there is no tags file (required; an install image with neither file, or with both empty, fails the pull); VEX tags of those images that the source has (required once found), unless `--skip-vex-images` |
| `install/` | `E/install` | `vX.Y.Z` of every selected version (required: the download tolerates a missing tag, but pull then crashes, §11.10); channel aliases (below) |
| `install-standalone/` | `E/install-standalone` | `vX.Y.Z` of every selected version (tolerated) |
| `release-channel/` | `E/release-channel` | in discovery mode every release channel the source publishes, and a suspended one fails the pull unless `--ignore-suspend` is given; in a tag-pinned pull every published channel that is not suspended (every one with `--ignore-suspend`); in both cases whatever version the channel points to (§11.4); `vX.Y.Z` of every selected version (required in discovery mode, tolerated when tag-pinned); re-pointed channels (below) |

The selected versions are the discovery result described in README "Platform Release Discovery" or, in a tag-pinned pull (`--deckhouse-tag X`, `--include-platform "=X"`), X itself when it is a version or a custom tag, and the version the channel points to when X is a channel name.

Channel aliases in `install/` (`propagateChannelAliases`):

- in discovery mode, every channel that passes the `--include-platform` filter and does not point below the version of `rock-solid` gets a descriptor for the install image of the version it points to, so on a source where `lts` lags behind `rock-solid` the bundle carries `release-channel:lts` but no `install:lts`;
- in a tag-pinned pull, when `release-channel/` holds an image for the pinned tag and exactly one tag was selected, the five default channels (`alpha` … `rock-solid`, not `lts`) alias the install image of that tag.

Re-pointed channels in `release-channel/`: in a tag-pinned pull, when the source has `release-channel:<tag>`, the five default channels are appended as aliases of that image. They follow the upstream channel descriptors with the same short tags, so push publishes the re-pointed ones (§5.3). That order survives the sort of §7.5 only because the layout never holds more than 12 descriptors, the size up to which Go's `sort.Slice` keeps equal keys in place. The repository root and `install-standalone/` never get channel tags.

`deckhousereleases.yaml` is written in discovery mode only, next to the archives: a `---`-separated stream with one `DeckhouseRelease` object (`deckhouse.io/v1alpha1`) per selected version that has a `release-channel` image, built from that image's `version.json` and `changelog.yaml` ([platform/deckhouse_releases.go](../internal/mirror/platform/deckhouse_releases.go)). A failure to build it is a warning.

### 6.2. `installer.tar`

Staged with its layout at `<tmp>/installer/installer` and packed without a prefix from `<tmp>/installer` ([installer/installer.go](../internal/mirror/installer/installer.go)).

| Layout path | Source repository | Short tags |
|---|---|---|
| `installer/` | `Rs/installer` | `--installer-tag`, by default `latest` |

The phase checks first that the tag exists. When it does not, or the check fails, pull warns and writes no `installer.tar`, even for an explicit `--installer-tag`.

### 6.3. `security.tar`

Staged in `<tmp>/security/<db>` and packed without a prefix from `<tmp>` itself ([security/security.go](../internal/mirror/security/security.go)), see §11.1.

| Layout path | Source repository | Short tags |
|---|---|---|
| `security/trivy-db/` | `E/security/trivy-db` | `2` |
| `security/trivy-bdu/` | `E/security/trivy-bdu` | `1` |
| `security/trivy-java-db/` | `E/security/trivy-java-db` | `1` |
| `security/trivy-checks/` | `E/security/trivy-checks` | `0` |

Pull writes the archive only when the source has `trivy-db:2`. The other three databases are tolerated, so their layouts may be empty. The tags are fixed and do not depend on any version flag.

### 6.4. `module-<name>.tar`

`<name>` is a tag of the source modules catalog `E/M`, one tag per module; with `--proxy-registry` the names come from `--include-module` instead. Staged in `<tmp>/modules/<name>` and packed with the prefix `modules/<name>` (`packModules` in [modules/modules.go](../internal/mirror/modules/modules.go)). The prefix is always `modules`, whatever `--modules-path-suffix` says.

| Layout path | Source repository | Short tags |
|---|---|---|
| `modules/<name>/` | `E/M/<name>` | `vX.Y.Z` of every selected version (tolerated); every `sha256:<hex>` found in the `images_digests.json` of each version image, in digest form (best effort); VEX tags of the version images, internal images and extra images that the module repository has (best effort), unless `--skip-vex-images` |
| `modules/<name>/release/` | `E/M/<name>/release` | `alpha`, `beta`, `early-access`, `stable`, `rock-solid`, plus `lts` when published (tolerated); `vX.Y.Z` of every selected version (best effort); exact-pin aliases (below) |
| `modules/<name>/extra/<extra>/` | `E/M/<name>/extra/<extra>` | for every selected version, the tag its `extra_images.json` (`{"<extra>": "<tag>"}`, numbers allowed) gives `<extra>` (tolerated; an `extra_images.json` that cannot be read after 5 tries fails the pull) |

- A module whose every `--include-module` entry is an exact pin (`<name>@=<tag>`) pulls no channel tags. After the pull the `release/` image of `<tag>` is aliased as `alpha` … `rock-solid` and `lts` when it is the module's only pin and has no `+channel` suffix, or as the named channel for `=<tag>+<channel>`; other pins of the same module get no aliases. When exact pins of a module are mixed with a range, its channels are pulled and nothing is aliased.
- With `--only-extra-images` only the `extra/*` layouts and VEX tags are pulled, so `release/` and the root may be empty.
- A module whose layouts are all empty gets no archive.

### 6.5. `package-<name>.tar`

As §6.4, with these differences ([packages/packages.go](../internal/mirror/packages/packages.go)): the source catalog is `E/packages` with no path suffix, the prefix is `packages/<name>`, the channel layout is `packages/<name>/version/` from `E/packages/<name>/version`, and extra images are under `packages/<name>/extra/<extra>/`.

### 6.6. `package-versions.tar`

One archive for all packages, packed from the staging directories `<tmp>/package-versions/<name>/version`, each with the prefix `packages/<name>/version` (`PullPackageVersions`).

| Layout path | Source repository | Short tags |
|---|---|---|
| `packages/<name>/version/` for every tag `<name>` of `E/packages` | `E/packages/<name>/version` | every tag the repository lists, plus `alpha` … `rock-solid`, `lts` when published, and the versions their `version.json` names (all tolerated) |

The archive ignores `--no-packages`, `--include-package` and `--exclude-package`, except with `--proxy-registry`, where the package names come from `--include-package` and no archive is written without it. A package whose version images fail to download is left out with a warning, and so are packages without any version image. When no package is left, no archive is written: silently when the source has no packages catalog, with a warning when listing it fails for another reason. Its layouts overlap those of the `package-<name>.tar` archives, and push merges them (§8.5, §11.5).

### 6.7. `deckhouse-cli.tar`

Staged in `<tmp>/deckhouse-cli` and packed with the prefix `deckhouse-cli` ([dist/cli.go](../internal/mirror/dist/cli.go)).

| Layout path | Source repository | Short tags |
|---|---|---|
| `deckhouse-cli/` | `Rs/deckhouse-cli` | one version: `--deckhouse-cli-tag`, or the newest stable semver tag of the repository |

A multi-platform index stays an index with all its platform children (`pullTag` in [dist/image.go](../internal/mirror/dist/image.go)). The children are fetched by digest and stored byte for byte, but the top-level index is rebuilt and re-marshaled: its media type, annotations, subject and child descriptors are carried over, a child's `artifactType` is not, and its digest can differ from the source. Without `--deckhouse-cli-tag`, a failed listing of the repository, the absence of a stable version tag, `--proxy-registry` or a failed download yields a warning and no archive; a pinned tag that cannot be pulled fails the pull.

### 6.8. `plugin-<name>.tar`

Staged in `<tmp>/plugins/<name>` and packed with the prefix `deckhouse-cli/plugins/<name>` ([dist/plugins.go](../internal/mirror/dist/plugins.go)). `<name>` matches `^[a-z0-9]+(?:[._-][a-z0-9]+)*$`.

| Layout path | Source repository | Short tags |
|---|---|---|
| `deckhouse-cli/plugins/<name>/` | `Rs/deckhouse-cli/plugins/<name>` | every selected version of the plugin, as published (`v1.2.0`, `v2.0.0-rc.1`) (required) |

Each version is stored as in §6.7: an index stays an index with its children and the `contract` annotation, rebuilt the same way. Which plugins and versions are selected is described in README "Plugin Mirroring".

## 7. How pull builds the bundle

### 7.1. Source repositories

Pull splits `--source` (default `registry.deckhouse.ru/deckhouse/ee`) with `GetEditionFromRegistryPath` ([pkg/registry/service/service.go](../pkg/registry/service/service.go)): after a trailing `/` is removed, if the last path segment is an edition, E is `--source` and Rs is `--source` without that segment; otherwise E and Rs are both `--source`. Unlike push (§8.2), a source with a single path segment is split as well: `host/ee` gives Rs `host` (§11.7).

| Read from E | Read from Rs |
|---|---|
| the repository root, `install`, `install-standalone`, `release-channel`, `security/*`, `M/*` (modules), `packages/*` | `installer`, `deckhouse-cli`, `deckhouse-cli/plugins/*` |

M is `--modules-path-suffix` of pull with surrounding `/` removed, by default `modules`; `/` places modules directly in E. The bundle stores modules under `modules/` regardless (§6.4).

### 7.2. Working directory

`<tmp>` is `--tmp-dir`, by default `<bundle>/.tmp`. Pull stages every component in a fixed subdirectory:

| Component | Staging directory | Created |
|---|---|---|
| platform | `<tmp>/platform`, with `install`, `install-standalone`, `release-channel` inside | at start, even with `--no-platform` |
| installer | `<tmp>/installer/installer` | at start, even with `--no-installer` |
| security | `<tmp>/security/<db>` | at start |
| modules | `<tmp>/modules/<name>` | in the modules phase |
| packages | `<tmp>/packages/<name>` | in the packages phase |
| package versions | `<tmp>/package-versions/<name>/version` | in the package versions phase |
| d8 CLI | `<tmp>/deckhouse-cli` | on first use |
| plugins | `<tmp>/plugins/<name>` | on first use |

Packing deletes every file it adds to an archive (`packFuncWithPrefix`), so a packed staging directory remains as an empty tree. A component that wrote no archive keeps its staging files: a module or package without images, the d8 CLI after a failed automatic download, and the installer after a failed tag check or the platform with `--no-platform` unless the security phase swept their empty layouts into `security.tar` (§11.1). Pull never removes `<tmp>` itself: the final cleanup deletes it only when it holds nothing but a `mirror` directory, and the platform, installer and security staging directories exist from the start (`finalCleanup` in [cmd/pull/pull.go](../internal/mirror/cmd/pull/pull.go)).

### 7.3. Phases

`PullService.Pull` ([pull.go](../internal/mirror/pull.go)) runs the phases in this order, and each one writes its archives before the next starts:

| # | Phase | Skipped by | Writes |
|---|---|---|---|
| 1 | platform | `--no-platform` | `platform.tar`, `deckhousereleases.yaml` |
| 2 | installer | `--no-installer` | `installer.tar` |
| 3 | security | `--no-security-db` | `security.tar` |
| 4 | modules | `--no-modules`, unless `--include-module` or `--only-extra-images` is given | `module-<name>.tar` |
| 5 | packages | `--no-packages`, unless `--only-extra-images` is given | `package-<name>.tar` |
| 6 | package versions | never | `package-versions.tar` |
| 7 | d8 CLI | `--only-extra-images` | `deckhouse-cli.tar` |
| 8 | plugins | `--only-extra-images` | `plugin-<name>.tar` |

The plugins phase runs last because it selects plugins against the module and platform versions that phases 1 and 4 selected. The module versions are those recorded before any download, so a selected version that was never pulled still counts; a module without any image does not (`Stats` in [modules/stats.go](../internal/mirror/modules/stats.go)).

### 7.4. Download

- An image is resolved to a digest by tag, or taken from a digest reference without checking that it exists, and downloaded by digest; every download is tried up to 5 times, 10 s apart ([puller/puller.go](../internal/mirror/puller/puller.go)). The d8 CLI and plugins bypass this puller: they are fetched by tag, with index children by digest, under their own 5 tries 10 s apart, and the check below does not apply to them.
- Only the d8 CLI and plugin phases keep multi-platform images whole (`pullTag` in [dist/image.go](../internal/mirror/dist/image.go)). Every other phase stores single-platform images: when a source tag or digest names an image index, the registry client resolves it to the linux/amd64 child (`remote.Image`), and that child is stored under the short tag. For a digest-form short tag (§5.4) this means `<repo>@sha256:<hex>` does not resolve in the target, because the index itself is not pushed.
- Each tag is required, tolerated or best effort as listed in §6.
- After a download batch completes, pull checks that each image it resolved is in the layout under its short tag (`verifyPlannedImagesLanded`) and fails otherwise.
- Storing an image again under the same tag and digest adds no descriptor (`AddImage`, `AddIndex` in [pkg/registry/image/layout.go](../pkg/registry/image/layout.go)).

### 7.5. Post-processing

Before packing: the platform layouts get their channel aliases and are then sorted; the module and package layouts are sorted and then get their exact-pin aliases; the security and package versions layouts are only sorted. Sorting orders descriptors by `ref.name` (`SortIndexManifests` in [pkg/libmirror/layouts/indexes.go](../pkg/libmirror/layouts/indexes.go)).

### 7.6. Packing and atomicity

- The platform, installer and security layouts sit under their final layout paths in the staging directory and are packed without a prefix (`bundle.Pack`). Modules, packages, package versions, the d8 CLI and plugins are staged per item and packed with the prefix that makes their layout path (`bundle.PackWithPrefix`, `bundle.PackSourcesWithPrefix`).
- Every archive goes through `pack.Bundle` ([pack/pack.go](../internal/mirror/pack/pack.go)): it is written to `<archive>.tmp`, or to `<archive>.NNNN.chunk.tmp` parts, and renamed to its final name only after packing succeeded and the operation was not cancelled; on failure or cancellation the staged files are deleted. An archive in the bundle directory is therefore always a complete tar, and an interrupted pack leaves no truncated archive behind.
- A cancelled modules or packages phase stops pulling and then packs, with cancellation suppressed, every item that has at least one image, the item that was interrupted mid-pull included. That last archive is a complete tar with partial content, for example module channel tags without the version images they point to.

## 8. How push maps the bundle to the registry

`d8 mirror push [<bundle>] <registry> [--file <archive>]...` is implemented by [cmd/push](../internal/mirror/cmd/push/) and `PushService` in [push.go](../internal/mirror/push.go).

### 8.1. Target

The `<registry>` argument (`parseAndValidateRegistryURLArg` in [cmd/push/validation.go](../internal/mirror/cmd/push/validation.go)):

- has every occurrence of `http://` and `https://` removed;
- MUST have a non-empty path after the host (`host[:port]/path`), since pushing to a registry root is rejected;
- MUST be a valid repository name for go-containerregistry (`name.NewRepository`): the path, without its leading `/`, consists of lowercase letters, digits, `_`, `-`, `.` and `/`;
- is parsed as `docker://<registry>` into a host (`host[:port]`) and a path, and the path, its leading `/` included, MUST be 2 to 255 characters long, so the path proper is 2 to 254 characters (`reg.example.com/a` is rejected).

T is the host followed by the path.

### 8.2. Edition split

`SplitTargetEdition` ([push.go](../internal/mirror/push.go)) takes the path without trailing `/`, its last segment L and the parent P without trailing `/`. If L is an edition and P is not empty, R is the host followed by P, and T is `R/L`. Otherwise R is T.

| Target | T | R |
|---|---|---|
| `reg.example.com/deckhouse/ee` | `reg.example.com/deckhouse/ee` | `reg.example.com/deckhouse` |
| `reg.example.com/mirror/deckhouse/cse/` | `reg.example.com/mirror/deckhouse/cse` | `reg.example.com/mirror/deckhouse` |
| `reg.example.com/deckhouse` | `reg.example.com/deckhouse` | T |
| `reg.example.com/deckhouse/ee-mirror` | `reg.example.com/deckhouse/ee-mirror` | T |
| `reg.example.com/ee` | `reg.example.com/ee` | T, the edition has no parent |
| `reg.example.com/deckhouse/EE` | rejected by §8.1 | — |

### 8.3. Archive discovery

- A directory argument contributes its top-level entries that are regular files, symbolic links excluded, and whose names end in `.tar` or match `^(.+)\.tar\.\d{4,}\.chunk$`; a directory without such an entry fails the push even when `--file` adds archives. A file argument and every `--file` MUST match the same patterns. The positional argument and `--file` follow symbolic links, so a link to a bundle directory works.
- Every chunk name is replaced by its archive name, `<dir>/<name>.tar`, duplicate paths are dropped, and the list is sorted by path, byte-wise ([push.go](../internal/mirror/push.go), `unpackAllPackages`). Push fails when the list is empty.
- File names are not interpreted beyond this and the legacy rule of §8.5.

### 8.4. Pre-check

Before unpacking anything, push writes a random image, whose single layer holds one random 512-byte file, to `T:d8WriteCheck` and leaves it there (`ValidateWriteAccessForRepo` in [validation/registry_access.go](../internal/mirror/validation/registry_access.go)). A failure stops the push unless `MIRROR_BYPASS_ACCESS_CHECKS=1` is set. The check times out after 15 s, or after `D8_MIRROR_TIMEOUT`. R is not checked (§11.8).

### 8.5. Unpacking

Push unpacks into U, which is `<tmp>/push/unified`, where `<tmp>` is `--tmp-dir`, by default `<bundle>/.tmp/mirror` when the bundle argument is a directory and otherwise `<directory>/.tmp/mirror` for the first archive given on the command line (the bundle file, else the first `--file`). U is not emptied first (§11.12). For each archive, in the order of §8.3 (`bundle.Unpack` in [pkg/libmirror/bundle/bundle.go](../pkg/libmirror/bundle/bundle.go)):

1. Open `<name>.tar`, or concatenate its chunks when the file does not exist (§4.3).
2. Extract every regular-file entry to `U/tmp/<entry name>` and skip the other entry types. An absolute entry name is re-rooted under `U/tmp`; a regular-file entry whose cleaned path leaves `U/tmp` aborts the archive at that entry, and what was extracted before it stays (§11.3).
3. Move the extracted tree into U at the same relative paths. Files are renamed over existing ones, except `index.json`, which is merged into an existing one (below).
4. Legacy rule: for a file named `module-<x>.tar`, where `<x>` is everything between `module-` and `.tar`, when no regular-file entry name starts with the string `modules/<x>`, the tree is moved to `U/modules/<x>/` instead. This keeps archives from before modules carried their prefix working. The test is a plain string prefix: `module-foo-v2.tar` holding `modules/foo/…` is relocated to `U/modules/foo-v2/modules/foo/…`, and `module-foo.tar` holding `modules/foobar/…` is not relocated.

The `index.json` merge (`mergeIndexJSON`) takes the existing descriptors followed by the incoming ones, drops repeated (digest, `ref.name`) pairs keeping the first occurrence, and sorts the result by `ref.name` with a sort that is not stable. It keeps the `annotations` and `subject` of the existing file and takes `schemaVersion` and `mediaType` from the incoming file only where the existing one lacks them; every other top-level field of either file is dropped. Archives that cover one layout path thus combine their tags. Two descriptors with one short tag and different digests both survive, and which of them push publishes (§8.7) depends on archive order and on the sort, so producers MUST NOT give a tag of a layout different digests in different archives.

A failure in any of these steps is reported as `Failed to unpack <name>`, and push continues with the next archive (§11.3).

### 8.6. Layouts and destinations

Every directory of U that contains a file named `index.json`, at any depth, is a layout, and push processes the layouts sorted by path (`findLayouts`). For a layout at path P, in slash form and empty for U itself:

1. Skip the layout if its index cannot be read (logged only at debug level, `MIRROR_DEBUG_LOG=3`) or has no descriptors (silently).
2. The base is R when the first segment of P is `installer` or `deckhouse-cli`, and T otherwise (`clientFor`).
3. The registry path is P with a leading `modules` segment replaced by MP (`remapModulesSegment`). MP is `--modules-path-suffix` of push with surrounding `/` removed, by default `modules`; `/` makes it empty, which puts modules directly under T.
4. The destination repository is the base joined with the registry path; an empty registry path means the base itself.

The base is chosen from P before the modules rewrite, so `installer/` and `deckhouse-cli/` never move with `--modules-path-suffix`. A relative `<tmp>` whose path starts with `module-` breaks this mapping for every layout (§11.11).

### 8.7. Tags

For each layout (`PushLayout` in [pusher/pusher.go](../internal/mirror/pusher/pusher.go)):

- descriptors without a short tag are skipped, logged only at debug level;
- for a short tag that occurs more than once, the last descriptor is pushed, at the position of the first;
- every remaining descriptor is pushed to `<destination>:<short tag>`, as a whole index when its media type is an index and as an image otherwise; manifests are written unchanged, so digests are preserved; a short tag that starts with `sha256:` or `@sha256:` is pushed by digest and gets no tag;
- every push is tried up to 4 times, 3 s apart, on top of the transport retries go-containerregistry makes for transient errors, and a final failure stops the whole push.

`ref.name` plays no part in pushing.

### 8.8. Discovery tags

After all layouts, push writes one tag per item, so that listing the tags of the parent repository enumerates the items (`createModulesIndex`, `createPackagesIndex`, `createPluginsIndex` in [push.go](../internal/mirror/push.go)):

| For every directory directly under | Tag |
|---|---|
| `U/modules/` | `T/MP:<directory name>`, or `T:<directory name>` when MP is empty |
| `U/packages/` | `T/packages:<directory name>` |
| `U/deckhouse-cli/plugins/` | `R/deckhouse-cli/plugins:<directory name>` |

The tagged image is a fresh random image whose single layer holds one random 32-byte file, so its digest changes on every push and only the tag name carries meaning. A directory gets its tag whether or not it holds a layout with images. Only items under these three paths become discoverable.

### 8.9. Order, atomicity, cleanup

Push is sequential: layouts in path order, descriptors in index order, then the module, package and plugin discovery tags. It is not transactional, so a failure leaves everything written before it in the registry. Once the pre-check has passed, SIGINT and SIGTERM cancel gracefully and the command exits 0 with a partial result; before that, and for any other signal, the signal's default action terminates the process. U is removed when `PushService.Push` returns, and whenever the command exits 0, cancellation included, it also removes `<tmp>` recursively (§11.2); a killed push leaves both behind (§11.12).

### 8.10. Routing table

Destinations of the layouts that pull produces. For a target split by §8.2, R is the parent of T; for any other target, R is T.

| Layout path | Destination |
|---|---|
| *(root)* | `T` |
| `install/`, `install-standalone/`, `release-channel/` | `T/install`, `T/install-standalone`, `T/release-channel` |
| `security/<db>/` | `T/security/<db>` |
| `modules/<name>/`, `modules/<name>/release/`, `modules/<name>/extra/<extra>/` | `T/MP/<name>`, `T/MP/<name>/release`, `T/MP/<name>/extra/<extra>` |
| `packages/<name>/`, `packages/<name>/version/`, `packages/<name>/extra/<extra>/` | `T/packages/<name>`, `T/packages/<name>/version`, `T/packages/<name>/extra/<extra>` |
| `installer/` | `R/installer` |
| `deckhouse-cli/` | `R/deckhouse-cli` |
| `deckhouse-cli/plugins/<name>/` | `R/deckhouse-cli/plugins/<name>` |
| discovery tags | `T/MP:<module>`, `T/packages:<package>`, `R/deckhouse-cli/plugins:<plugin>` |
| pre-check | `T:d8WriteCheck` |

## 9. Writing a push-compatible archive by hand

An archive is pushed as intended when:

1. it is an uncompressed tar of regular files with relative paths and no `..` segments, or a complete set of chunks without gaps;
2. every image sits in a self-contained OCI layout (§5.1) whose path inside the archive is the intended repository path relative to T, or relative to R for `installer/` and `deckhouse-cli/`;
3. every descriptor to push carries `io.deckhouse.image.short_tag` with a valid tag (`[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}`);
4. modules, packages and plugins sit under `modules/<name>/`, `packages/<name>/` and `deckhouse-cli/plugins/<name>/`, since only there do they get a discovery tag and does `--modules-path-suffix` apply;
5. nothing is under a top-level `tmp/` (§4.2);
6. if it is named `module-<x>.tar`, some regular-file entry name starts with `modules/<x>`, since otherwise the legacy rule relocates its content (§8.5);
7. no other archive of the bundle gives one of its tags a different digest in the same layout (§8.5).

The file name is otherwise free, and archives can be renamed and pushed with `--file`, as long as it ends in `.tar`, or forms a chunk set `<name>.tar.NNNN.chunk` numbered from `0000` without gaps, and does not start with `module-` unless item 6 holds. Its position in the sort of §8.3 decides the merge order of §8.5.

## 10. Example

A pull from `registry.deckhouse.ru/deckhouse/ee` with three platform versions, a module `foo` with one extra image, packages `bar` and `baz` (`baz` publishes a version tag and no channels), the d8 CLI and three plugins produces:

```
bundle/
├── platform.tar            # (root), install/, install-standalone/, release-channel/
├── deckhousereleases.yaml
├── installer.tar           # installer/
├── security.tar            # security/{trivy-db,trivy-bdu,trivy-java-db,trivy-checks}/
├── module-foo.tar          # modules/foo/, modules/foo/release/, modules/foo/extra/scanner/
├── package-bar.tar         # packages/bar/, packages/bar/version/
├── package-versions.tar    # packages/bar/version/, packages/baz/version/
├── deckhouse-cli.tar       # deckhouse-cli/
├── plugin-foo-tool.tar     # deckhouse-cli/plugins/foo-tool/
├── plugin-package.tar      # deckhouse-cli/plugins/package/
└── plugin-system.tar       # deckhouse-cli/plugins/system/
```

`d8 mirror push bundle/ registry.example.com/deckhouse/ee`, with T `registry.example.com/deckhouse/ee` and R `registry.example.com/deckhouse`, writes the following, listed by repository (the push order is that of §8.9: the pre-check, the layouts sorted by path, then the discovery tags):

```
registry.example.com/deckhouse/ee:d8WriteCheck                            pre-check
registry.example.com/deckhouse/ee:v1.70.3 :v1.71.2 :v1.72.1               platform versions
registry.example.com/deckhouse/ee:<hex> :sha256-<hex>.att                 images from images_digests.json, VEX
registry.example.com/deckhouse/ee/install:v1.70.3 … :alpha … :stable      versions and channel aliases
registry.example.com/deckhouse/ee/install-standalone:v1.70.3 …
registry.example.com/deckhouse/ee/release-channel:alpha … :v1.72.1
registry.example.com/deckhouse/ee/security/trivy-db:2                     and trivy-bdu:1, trivy-java-db:1, trivy-checks:0
registry.example.com/deckhouse/ee/modules/foo:v1.2.0 :v1.2.0.att :<hex>
registry.example.com/deckhouse/ee/modules/foo/release:alpha … :v1.2.0
registry.example.com/deckhouse/ee/modules/foo/extra/scanner:v3
registry.example.com/deckhouse/ee/packages/bar:v0.3.0
registry.example.com/deckhouse/ee/packages/bar/version:stable :v0.3.0
registry.example.com/deckhouse/ee/packages/baz/version:v0.0.1
registry.example.com/deckhouse/installer:latest
registry.example.com/deckhouse/deckhouse-cli:v0.13.1                      whole multi-platform index
registry.example.com/deckhouse/deckhouse-cli/plugins/system:v1.0.0        and package:v1.1.0, foo-tool:v2.0.0
registry.example.com/deckhouse/ee/modules:foo                             discovery tags
registry.example.com/deckhouse/ee/packages:bar :baz
registry.example.com/deckhouse/deckhouse-cli/plugins:foo-tool :package :system
```

`baz` is in the bundle only through `package-versions.tar`, yet it gets a discovery tag like `bar`.

## 11. Known deviations

Verified on 2026-10-01 by running `PullService` and `PushService`, and `d8 mirror push` itself, against test registries. Each item breaks a rule above or a promise of README.

1. `security.tar` takes the whole `<tmp>`. The security layouts are rooted at `<tmp>` itself and the archive is packed from there (`createOCIImageLayoutsForSecurity(workingDir)` and `bundle.Pack(ctx, svc.layout.workingDir, …)` in [security/security.go](../internal/mirror/security/security.go)). Every file under `<tmp>` at that moment goes into `security.tar` and is deleted from disk: foreign files when `--tmp-dir` points at a shared directory, the empty `platform/…` and `installer/installer/` layouts when those phases are skipped, and leftovers of an earlier interrupted pull, whose layouts push then publishes. Because the raw `--tmp-dir` value is used as the pack root, a value that is not a clean path also breaks the entry names: with `--tmp-dir /mnt/tmp/` every entry is the absolute path `/mnt/tmp/security/trivy-db/…` and push publishes the databases at `T/mnt/tmp/security/…`; with `./scratch` the entries start with `scratch/`, and with `tmp/` they fall under the reserved `tmp/` and are lost (§4.2).
2. Whenever push exits 0, the command deletes `<tmp>` itself with everything in it (`PostRunE` calls `os.RemoveAll(TempDir)` in [cmd/push/push.go](../internal/mirror/cmd/push/push.go)), files d8 did not create included, for example all of `--tmp-dir /mnt/large-disk/tmp`.
3. A damaged archive does not fail the push. A truncation on an entry boundary, which a missing chunk can produce because parts end on whole writes, reads as a clean end of the tar: the archive unpacks without any message and its remaining entries are lost. Any other damage (truncated inside an entry, corrupt data, an escaping entry) yields only a `Failed to unpack` warning, and push exits 0. What was extracted before the error stays in `U/tmp`: the move of the next archive (§8.5, step 3) carries it into U, or into `U/modules/<x>/` when the next archive falls under the legacy rule, and when no archive follows, push publishes it under `T/tmp/…`. A truncated `platform.tar` pushed alone produced `T/tmp`, `T/tmp/install` and `T/tmp/install-standalone`.
4. `release-channel/` carries channels that point at versions the bundle does not hold: every published channel is downloaded, and only the `install/` aliases are filtered (§6.1). With `--include-platform ">=1.71"` and `stable` at v1.70.3, the target gets `release-channel:stable` announcing v1.70.3 without `T:v1.70.3`, and a custom `--deckhouse-tag` that has no release-channel image ships every upstream channel unchanged. README's statement that channels whose snapshot falls outside an `--include-platform` constraint are excluded from the bundle holds only for `install/`.
5. The channels of an exact package pin are not reliably re-pointed. `package-<name>.tar` re-points the channels of `packages/<name>/version/` to the pinned version, `package-versions.tar` carries the upstream channels of the same layout, and the merge (§8.5) keeps both under the same `ref.name`. Which one push publishes is decided per channel by the archive order and, for merged layouts of more than 12 descriptors, by the unstable sort, so a pin can end up with a mix of pinned and upstream channels. With small layouts, `--include-package aaa@=v0.2.0` published `packages/aaa/version:stable` as the upstream v0.3.0 and `--include-package zzz@=v0.2.0` as v0.2.0, because `package-zzz.tar` sorts after `package-versions.tar`. With a source that has a port, the alias `ref.name` of §11.9 sorts after every upstream one, and the pin wins.
6. `--since-version` does not narrow the bundle. It is parsed into `params.PullParams.SinceVersion`, but `PullServiceOptions` has no field for it, so `platform.Options.SinceVersion` is never set (`NewPullService` in [pull.go](../internal/mirror/pull.go)).
7. Pull and push disagree on a single-segment edition path. A pull from `host/ee` reads the installer and the d8 CLI from `host/installer` and `host/deckhouse-cli` (§7.1), while a push to `host/ee` writes them to `host/ee/installer` and `host/ee/deckhouse-cli` (§8.2).
8. The pre-check (§8.4) covers T only, so with an edition target push writes to R without having checked access to it.
9. Alias descriptors rebuild `ref.name` by cutting the original at its first `:` (`TagImage` in [pkg/registry/image/layout.go](../pkg/registry/image/layout.go) and [pkg/libmirror/layouts/indexes.go](../pkg/libmirror/layouts/indexes.go)), so for a source with a port (`host:5000/…`) an alias gets the `ref.name` `host:<channel>`. Routing is not affected; the merge key and the sort order of §8.5 are.
10. A selected platform version without `install:<version>` in the source crashes the pull with `runtime error: invalid memory address or nil pointer dereference`. The install download tolerates the missing tag and leaves an empty entry in its download list, which the digest extraction then dereferences (`pullDeckhousePlatform` in [platform/platform.go](../internal/mirror/platform/platform.go)). A custom `--deckhouse-tag` without an install image is enough to trigger it.
11. A relative `<tmp>` whose path starts with `module-` sends every layout to `T/MP`. `pushSingleLayout` in [push.go](../internal/mirror/push.go) still carries a check for the old `module-<name>.tar` layout that tests the layout's filesystem path, `strings.HasPrefix(layoutDir, "module-")`, instead of the archive name; with a relative path such as `module-foo/.tmp/mirror/push/unified/install` it matches every layout. `d8 mirror push module-foo/ <registry>` with the default `--tmp-dir`, or any relative `--tmp-dir module-…`, is enough: a bundle with `install:v1.0.0` and `modules/foo:v0.1.0` produced `T/modules:v1.0.0` and `T/modules:v0.1.0`, and the tags of different layouts overwrite each other there.
12. Push does not empty U before unpacking (`Push` in [push.go](../internal/mirror/push.go) only creates it). U is removed when `Push` returns, but a push killed by SIGKILL or SIGHUP, or one that crashes, leaves U and `U/tmp` behind, and the next push with the same `<tmp>` merges their content into its own and publishes it.
13. `d8 tools gostsum` with several files prints a wrong hash for every file after the first: it reuses one hasher without resetting it, so the second hash covers both files ([internal/tools/gostsum/gostsum.go](../internal/tools/gostsum/gostsum.go)). Check `.gostsum` files one file per invocation (§4.4).
