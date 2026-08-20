# `d8 mirror` Bundle Layout

How `d8 mirror pull` lays out the archives it writes, why it lays them out that way, and what `d8 mirror push` requires in order to route every artifact back into the correct registry repository.

## The one rule that ties `pull` and `push` together

The bundle is a **path-preserving snapshot of the source registry tree**. Every archive contains one or more [OCI image layouts](https://github.com/opencontainers/image-spec/blob/main/image-layout.md), and the path of a layout *inside the tar* is exactly the registry segment that layout must be pushed to. `push` performs **no path translation**: whatever relative path a layout ends up at after unpacking becomes its path in the destination registry (see the package doc comment on `PushService` in [push.go](../internal/mirror/push.go)).

Everything below is a consequence of this single rule. `pull` encodes the destination into the archive contents; `push` reads it back out positionally.

---

## 1. How `d8 mirror pull` builds the bundle

`pull` downloads the platform, installer, security databases, modules and packages, and writes them into the bundle directory (the first positional argument) as a set of tar archives. The orchestration lives in [`PullService.Pull`](../internal/mirror/pull.go); each component is pulled by its own service under [`internal/mirror/`](../internal/mirror/) (`platform`, `installer`, `security`, `modules`, `packages`).

### 1.1. Archives written to the bundle directory

| Archive | Produced by | One per… | Contains (registry segments) |
|---|---|---|---|
| `platform.tar` | `platform` service | bundle | repo root, `install/`, `install-standalone/`, `release-channel/` |
| `installer.tar` | `installer` service | bundle | `installer/` |
| `security.tar` | `security` service | bundle | `security/trivy-db/`, `security/trivy-bdu/`, `security/trivy-java-db/`, `security/trivy-checks/` |
| `module-<name>.tar` | `modules` service | module | `modules/<name>/` (main + `release/` + `extra/<extra-name>/`) |
| `package-<name>.tar` | `packages` service | package | `packages/<name>/` (main + `version/` + `extra/<extra-name>/`) |
| `package-versions.tar` | `packages` service | bundle | `packages/<name>/version/` for every package |

The segment constants are defined once in [internal/layout.go](../internal/layout.go) and reused by both `pull` and `push`, so the two sides can never drift.

> Note the difference between `install` and `installer`. `platform.tar` carries `install/` and `install-standalone/` (the in-cluster installer images that ship with the platform), while `installer.tar` carries `installer/` (the standalone `d8` installer image family). They are distinct repositories.

### 1.2. What each archive contains

**`platform.tar`** — packed from the platform working directory with no prefix ([platform.go](../internal/mirror/platform/platform.go), `bundle.Pack`). The sub-layouts are already arranged on disk in their final registry shape:

```
platform.tar
├── index.json                 # Deckhouse main images        -> <repo>:<version>
├── blobs/
├── install/                   # in-cluster installer          -> <repo>/install:<version>
├── install-standalone/        # standalone in-cluster installer-> <repo>/install-standalone:<version>
└── release-channel/           # channel + version metadata    -> <repo>/release-channel:<channel|version>
```

**`installer.tar`** — the standalone installer layout, packed from a working dir whose only entry is `installer/`:

```
installer.tar
└── installer/                 # -> <repo>/installer:<version>
    ├── index.json
    └── blobs/
```

**`security.tar`** — the four Trivy databases, each its own layout under `security/`:

```
security.tar
└── security/
    ├── trivy-db/              # -> <repo>/security/trivy-db:<version>
    ├── trivy-bdu/
    ├── trivy-java-db/
    └── trivy-checks/
```

**`module-<name>.tar`** — one archive per module, packed with the prefix `modules/<name>` ([modules.go](../internal/mirror/modules/modules.go), `bundle.PackWithPrefix`):

```
module-<name>.tar
└── modules/
    └── <name>/
        ├── index.json         # module main image  -> <repo>/modules/<name>:<version>
        ├── blobs/
        ├── release/           # module channels    -> <repo>/modules/<name>/release:<channel>
        └── extra/
            └── <extra-name>/   # extra images       -> <repo>/modules/<name>/extra/<extra-name>:<version>
```

**`package-<name>.tar`** — the package analogue of `module-<name>.tar`, prefixed with `packages/<name>`. The release segment is named `version/` instead of `release/`:

```
package-<name>.tar
└── packages/
    └── <name>/
        ├── index.json         # -> <repo>/packages/<name>:<version>
        ├── version/           # -> <repo>/packages/<name>/version:<channel>
        └── extra/<extra-name>/ # -> <repo>/packages/<name>/extra/<extra-name>:<version>
```

**`package-versions.tar`** — an aggregate of the `version/` (release-channel) layout of *every* package, built in one pass with `bundle.PackSourcesWithPrefix`, each source prefixed with `packages/<name>/version` ([packages.go](../internal/mirror/packages/packages.go)):

```
package-versions.tar
└── packages/
    ├── <name-a>/version/
    └── <name-b>/version/
```

### 1.3. Naming rules and why

- **Monolithic components get one fixed-name archive.** `platform`, `installer` and `security` are always pulled as a whole, so each is a single archive with a stable name. This keeps the bundle predictable and lets a user pass a single archive to `push --file platform.tar`.
- **Modules and packages get one archive per item** (`module-<name>.tar`, `package-<name>.tar`). Independent archives are what make `--include-module`/`--exclude-module` (and the package equivalents) cheap: adding or removing an item adds or removes exactly one file, without repacking anything else. It also makes a bundle incrementally extensible — you can pull one more module later and drop its archive next to the others.
- **`package-versions.tar` is always written**, regardless of `--no-packages` or any filter (`PullService.Pull` calls `PullPackageVersions` unconditionally). The package release-channel catalog is bundle-level metadata that must stay in sync on every pull, so it is never skipped.

### 1.4. Where the registry segment comes from (prefixes)

There are two ways the path inside the tar acquires its registry segment, and both produce the same result:

- **On-disk placement** — `platform`, `installer` and `security` build their sub-layouts directly under the final segment names on disk (`install/`, `installer/`, `security/trivy-db/`, …) and pack the working directory with **no prefix**. The path is baked into the directory tree.
- **Pack-time prefix** — `modules` and `packages` stage each item in a bare per-item working directory and inject the segment with `PackWithPrefix("modules/<name>")` / `PackWithPrefix("packages/<name>")` at pack time. This is necessary precisely because each item is packed separately into its own archive.

Either way, the invariant from the top of this document holds: **tar path == registry segment.**

### 1.5. Atomic writes — no stub archives

Every archive is written through [`pack.Bundle`](../internal/mirror/pack/pack.go), which stages the payload to a temporary name (`<name>.tmp`, or `<name>.NNNN.chunk.tmp` when chunking) and renames it to the final name **only after a successful, non-cancelled pack**. On any error — including `Ctrl+C` / timeout — the staged files are deleted. This is what guarantees the bundle directory never contains half-written or empty (`~5 KiB`) archives left over from an interrupted pull. Cancellation is also propagated cleanly through the puller so an aborted download can never be silently misread as "tag not found" and turned into a stub (see [puller.go](../internal/mirror/puller/puller.go)).

### 1.6. Chunking

When `--images-bundle-chunk-size` is set, `pack.Bundle` splits each archive into fixed-size parts named `<archive>.tar.NNNN.chunk` (zero-padded, 4+ digits — e.g. `platform.tar.0000.chunk`, `platform.tar.0001.chunk`), via [chunk_writer.go](../internal/mirror/chunked/chunk_writer.go). Chunking is a transport concern only: the parts concatenate back into the exact same tar stream, so the logical archive and its contents are unchanged.

### 1.7. GOST checksums

With `--gost-digest`, `pull` writes a `<archive>.gostsum` file next to every `.tar` and `.chunk` after the bundle is complete. These are integrity checksums for the transfer; they are not part of the archive and are ignored by `push`.

### 1.8. Shared blobs and `index.json` merging

Several archives can legitimately target the **same** OCI layout path. They share blobs (named by content hash, so there is never a collision) but each carries its own `index.json` that lists only the tags it contributed. When such archives are unpacked into a shared directory, a plain overwrite of `index.json` would drop every tag the last archive did not include. To prevent that, [bundle.go](../pkg/libmirror/bundle/bundle.go) **merges** `index.json` manifest lists (deduplicating by digest + `ref.name`) instead of overwriting. This merge behavior is what makes it safe to split a registry tree across many independent archives in the first place.

### 1.9. Completeness check

After pulling a layout, the puller cross-checks the download plan against the resulting `index.json`: every planned image must be present under its short tag, or the pull fails (`verifyPlannedImagesLanded` in [puller.go](../internal/mirror/puller/puller.go)). An incomplete bundle fails loudly at pull time instead of surfacing later as `ImagePullBackOff` in an air-gapped cluster.

---

## 2. What `d8 mirror push` expects and how it routes artifacts

`push` takes the bundle (a directory, a single archive, or files passed via `--file`), unpacks everything into one unified tree, and pushes each layout to the segment its path dictates. The orchestration is [`PushService.Push`](../internal/mirror/push.go).

### 2.1. How push discovers archives

Archive discovery is purely by **file extension**, not by name ([validation.go](../internal/mirror/cmd/push/validation.go), `isPackageFile`):

- any regular file ending in `.tar`, **or**
- any chunk part matching `<name>.tar.NNNN.chunk` (which is collapsed to its canonical `<name>.tar` so all parts are reassembled together).

Everything matching is treated as a package to unpack. **The file name does not determine where the contents go** — routing is decided later, from the paths *inside* the archive. The only place a file name matters is the legacy `module-<name>.tar` special case (§2.7). You can therefore rename archives freely, or push a hand-built archive with any name, as long as its internal paths are correct.

### 2.2. Unpack into a unified tree

All discovered archives are unpacked into a single `unified/` working directory, preserving their internal paths. Because multiple archives can share a layout path, `index.json` files are merged on collision (§1.8), never overwritten. After this step the unified tree looks exactly like the registry tree the bundle represents.

### 2.3. The routing principle: the path *is* the destination

`push` then walks the unified tree, treats **every directory that contains an `index.json` as an OCI layout**, and pushes it to `registry/<relative-path>` — where `<relative-path>` is that directory's path relative to the unified root (`findLayouts` + `pushSingleLayout` in [push.go](../internal/mirror/push.go)). No mapping table, no per-component logic: **the relative path of the layout is the registry segment, verbatim.** A layout at the root pushes to the repo root; a layout at `security/trivy-db` pushes to `<repo>/security/trivy-db`.

### 2.4. Routing table

| Layout path in the unified tree | Pushed to | Comes from |
|---|---|---|
| `` (root) | `<repo>:<tag>` | `platform.tar` (Deckhouse main) |
| `install/` | `<repo>/install` | `platform.tar` |
| `install-standalone/` | `<repo>/install-standalone` | `platform.tar` |
| `release-channel/` | `<repo>/release-channel` | `platform.tar` |
| `installer/` | `<repo>/installer` | `installer.tar` |
| `security/<db>/` | `<repo>/security/<db>` | `security.tar` |
| `modules/<name>/` (+ `release/`, `extra/<extra>/`) | `<repo>/modules/<name>[/…]` | `module-<name>.tar` |
| `packages/<name>/` (+ `version/`, `extra/<extra>/`) | `<repo>/packages/<name>[/…]` | `package-<name>.tar`, `package-versions.tar` |

### 2.5. The `short_tag` annotation decides the image tag

Within a layout, `push` does not invent tags. It reads the destination tag from each manifest's `io.deckhouse.image.short_tag` annotation (`AnnotationImageShortTag` in [layout.go](../pkg/registry/image/layout.go)). Descriptors without that annotation are **skipped with a warning**; when two descriptors carry the same short tag, the last one wins (`dedupManifestsByShortTag` in [pusher.go](../internal/mirror/pusher/pusher.go)). So the section is chosen by layout path, and the tag within that section is chosen by the annotation — both are read from the archive, never derived from the file name.

### 2.6. Discovery index tags

After pushing the layouts, `push` creates lightweight discovery tags so the platform can enumerate what is available:

- for every directory directly under `modules/`, it pushes a tiny placeholder image to `<repo>/modules:<name>`;
- for every directory directly under `packages/`, it pushes one to `<repo>/packages:<name>`.

These tags are what `ListTags` on `<repo>/modules` and `<repo>/packages` returns, so a module/package is only discoverable if its layout sits at `modules/<name>/` / `packages/<name>/`. This is the practical reason the `modules/` and `packages/` prefixes are mandatory (`createModulesIndex` / `createPackagesIndex` in [push.go](../internal/mirror/push.go)).

### 2.7. Special case: legacy `module-<name>.tar`

Older bundles packed a module's contents at the **root** of `module-<name>.tar`, without an inner `modules/<name>/` prefix. `push` keeps working with them: `bundle.Unpack` notices a package name starting with `module-` and, if the archive carries no `modules/<name>` entries, relocates the unpacked contents under `modules/<name>/` so routing still lands correctly ([bundle.go](../pkg/libmirror/bundle/bundle.go)). This is the one and only case where the **file name** influences routing. New bundles produced by current `pull` always carry the prefix internally and do not rely on it.

### 2.8. Special case: `--modules-path-suffix`

Modules are **always** stored under `modules/` inside the bundle. If you push them to a non-default location, `--modules-path-suffix` rewrites the leading `modules` segment at push time (`remapModulesSegment` in [push.go](../internal/mirror/push.go); `NormalizeModulesPath` in [service.go](../pkg/registry/service/service.go)). The remap happens only on push — the bundle contents are unchanged — and the discovery index tags (§2.6) follow the same remapped path.

---

## 3. Checklist: a push-compatible archive

An archive (whatever its name) is processed correctly by `push` if:

1. It is a valid tar, or a complete set of `<name>.tar.NNNN.chunk` parts with none missing.
2. Every image lives in a valid OCI layout — a directory containing `index.json` plus a `blobs/` tree.
3. The layout's path inside the tar equals the target registry segment (`install/`, `installer/`, `security/trivy-db/`, `modules/<name>/`, `packages/<name>/version/`, …). Root-level `index.json` targets the repo root.
4. Every manifest to be pushed carries the `io.deckhouse.image.short_tag` annotation; that value becomes its tag.
5. Modules are under `modules/<name>/` and packages under `packages/<name>/`, otherwise the discovery index tags and `--modules-path-suffix` remap will not apply to them.

Archives that share a layout path may each carry a partial `index.json`; the tags are unioned on unpack, so nothing is lost.

---

## 4. End-to-end example

A `pull` of the platform, one module and one package produces:

```
bundle/
├── platform.tar
├── installer.tar
├── security.tar
├── module-stronghold.tar
├── package-deckhouse-cli.tar
└── package-versions.tar
```

`push bundle/ registry.example.com/deckhouse/fe` unpacks all six into one tree:

```
unified/
├── index.json                         # platform main
├── install/ install-standalone/ release-channel/
├── installer/
├── security/{trivy-db,trivy-bdu,trivy-java-db,trivy-checks}/
├── modules/stronghold/{,release/,extra/<extra>/}
└── packages/deckhouse-cli/{,version/,extra/<extra>/}
```

and pushes each layout to the segment its path names:

```
registry.example.com/deckhouse/fe                        <- unified/index.json
registry.example.com/deckhouse/fe/install                <- unified/install
registry.example.com/deckhouse/fe/installer              <- unified/installer
registry.example.com/deckhouse/fe/security/trivy-db      <- unified/security/trivy-db
registry.example.com/deckhouse/fe/modules/stronghold     <- unified/modules/stronghold
registry.example.com/deckhouse/fe/packages/deckhouse-cli <- unified/packages/deckhouse-cli
…
registry.example.com/deckhouse/fe/modules:stronghold     <- discovery index tag
registry.example.com/deckhouse/fe/packages:deckhouse-cli <- discovery index tag
```

The bundle carried no routing table and `push` consulted none: every destination above was read straight out of the paths the archives were built with.
