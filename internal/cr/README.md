# `d8 cr` - Container Registry Tool

`d8 cr` is the container-registry subtree of the Deckhouse CLI. It is a
self-contained re-implementation of [crane](https://github.com/google/go-containerregistry/tree/main/cmd/crane)-style
workflows on top of [`go-containerregistry`](https://github.com/google/go-containerregistry),
extended with first-class commands for **inspecting the filesystem of an image
without running it**.

It is intended for engineers, SREs and CI pipelines that need to interact with
OCI/Docker registries from the command line: pulling and pushing images,
listing tags, inspecting manifests and configs, and exploring image contents
file-by-file.

Authentication piggybacks on the standard Docker config
(`~/.docker/config.json`), so `docker login` / `d8 login` work transparently.

---

## Table of contents

- [Features](#features)
- [Command map](#command-map)
- [Global flags](#global-flags)
- [Use cases](#use-cases)
- [Examples](#examples)
- [Architecture](#architecture)

---

## Features

### Image transfer (pull / push)

- **Pull** one or many images into a single artifact:
  - `tarball` (default) - docker-compatible multi-image tar that `docker load`
    and `podman load` read natively
  - `oci` - OCI image-layout directory that preserves the full multi-arch
    index and supports **resumable** pulls (already-downloaded layers are
    reused on rerun)
  - `legacy` - last-resort single-image format for very old `docker load`
    consumers
- **Push** a local tarball or OCI layout to any OCI/Docker registry.
  Supports pushing multi-manifest OCI layouts as a single index (`--index`).
- **Layer cache** (`--cache-path`) that is reused across pulls and self-heals
  corrupt entries on next access.

### Inspection

- **`manifest`** - print the raw manifest bytes exactly as the registry
  returned them. Pipe to `jq`, feed to a signature verifier, archive for
  audit.
- **`config`** - print the raw image config JSON (entrypoint, env, labels,
  history, ...).
- **`digest`** - resolve the `sha256:...` digest of an image, either from a
  registry or from a local tarball (`--tarball`). With `--full-ref` produces
  the pinnable reference `registry/repo@sha256:...`.
- **`ls REPO`** - list all tags in a repository.
- **`catalog REGISTRY`** - list all repositories in a registry.
- **`export`** - stream the merged filesystem of an image as a tar (crane-
  compatible: verbatim linknames, whiteouts filtered).

### Filesystem inspection (`d8 cr fs`)

A purpose-built subtree that reads the **merged filesystem** an image would
expose at runtime (with deleted-by-upper-layer files hidden), without
unpacking anything to disk:

- **`fs ls IMAGE [PATH]`** - list files at PATH (or the whole image), with
  optional long form (mode, size).
- **`fs cat IMAGE PATH`** - print a single regular file to stdout (no need
  for `docker run --rm IMAGE cat ...`).
- **`fs tree IMAGE [PATH]`** - render the filesystem as a tree, optionally
  capped by depth and annotated with human-readable sizes.
- **`fs extract IMAGE -o DIR`** - extract the full merged filesystem to a
  directory, with **path-traversal and absolute-symlink protections** that
  the verbatim `export` does not perform.

### Multi-arch handling

The global `--platform os/arch[/variant][:osversion]` flag resolves a
multi-arch index to a single image for image-level commands (pull tarball,
manifest, config, digest, all `fs *`, export). With `--format oci`, `pull`
keeps the entire index by default - omit `--platform` and you get every
platform on disk; pass it and you narrow down to one.

### Security and connectivity

- **TLS by default**, with `--insecure` to opt into plain HTTP and skip
  TLS verification. Localhost and RFC1918 hosts already auto-allow HTTP
  without `--insecure`.
- **Non-distributable (foreign) layers** are skipped on push by default;
  enable them with `--allow-nondistributable-artifacts` for registries that
  proxy Windows base images and similar.
- **Verbose mode** (`-v`) routes `go-containerregistry` debug output to
  stderr for troubleshooting auth, redirects, retries, etc.

### Quality-of-life

- **Shell completion** for image references, tags, paths inside images,
  enum flags, etc., for bash/zsh/fish/powershell. Completion-time network
  calls are bounded by a short timeout and degrade silently on failure.
- **Crashes-safe sinks**: `export` removes a half-written file on error so
  downstream `tar tf` cannot mistakenly consume a truncated archive.
- **Resumable OCI pulls**: re-running `pull --format oci` skips intact
  blobs and cleans up in-flight temp files left over from a `Ctrl+C`.

---

## Command map

```
d8 cr
├── pull IMAGE... PATH         Pull image(s) to a tarball or OCI layout
├── push PATH IMAGE            Push a tarball or OCI layout to a registry
├── export IMAGE [TARBALL]     Stream the merged filesystem as a tar
├── ls REPO                    List tags in a repository
├── catalog REGISTRY           List repositories in a registry
├── manifest IMAGE             Print the raw manifest
├── config IMAGE               Print the raw config JSON
├── digest [IMAGE]             Print the digest (registry or --tarball)
└── fs                         Inspect / extract files inside an image
    ├── ls IMAGE [PATH]        List files
    ├── cat IMAGE PATH         Print one regular file
    ├── tree IMAGE [PATH]      Render the filesystem as a tree
    └── extract IMAGE -o DIR   Extract the merged filesystem to disk
```

## Global flags

These persistent flags apply to every `d8 cr` subcommand:

| Flag | Purpose |
|---|---|
| `-v`, `--verbose` | Enable debug logs on stderr |
| `--insecure` | Allow plain HTTP and skip TLS verification |
| `--allow-nondistributable-artifacts` | Include non-distributable (foreign) layers when pushing |
| `--platform os/arch[/variant][:osversion]` | Resolve multi-arch indices to a specific platform (image-level commands only) |

---

## Use cases

### 1. Air-gapped delivery

Pull a fully reproducible set of images on a connected host as an OCI
layout, ship the directory across the air gap, and push it back into a
local registry:

```bash
d8 cr pull --format oci \
  registry.deckhouse.io/deckhouse/ce/install:stable \
  registry.deckhouse.io/deckhouse/ce/release:stable \
  ./deckhouse-bundle

# carry ./deckhouse-bundle across the air gap, then:

d8 cr push --index ./deckhouse-bundle registry.internal/deckhouse/bundle:v1
```

OCI layout preserves every architecture in the manifest list, blob digests
are stable, and rerunning the same pull is resumable.

### 2. Docker-compatible distribution

Build a single tarball that `docker load` / `podman load` can ingest on a
target machine without touching any registry:

```bash
d8 cr pull --platform linux/amd64 nginx:1.27 my-tools:1.0 ./images.tar
scp images.tar prod-host:/tmp/
ssh prod-host 'docker load -i /tmp/images.tar'
```

### 3. Reproducing a known image by digest

Pin a tag to its digest, then verify a deployment is using exactly that
image:

```bash
DIGEST=$(d8 cr digest --full-ref registry.internal/app:v1.4.2)
echo "$DIGEST"
# registry.internal/app@sha256:dead...beef

kubectl get pod my-app -o jsonpath='{.spec.containers[0].image}'
# must match $DIGEST exactly
```

### 4. Inspecting a release without `docker run`

CI pipelines, security audits and bug-bounty workflows often need to look
inside an image but cannot (or must not) execute it. `d8 cr fs` reads the
merged filesystem directly from the registry:

```bash
# Is /etc/passwd what we expect?
d8 cr fs cat registry.internal/app:v1.4.2 etc/passwd

# Where is the binary?
d8 cr fs tree registry.internal/app:v1.4.2 usr/local/bin --size

# Full audit: extract to disk with path-traversal protection,
# then run an offline scanner.
d8 cr fs extract registry.internal/app:v1.4.2 -o ./rootfs
grep -r 'BEGIN PRIVATE KEY' ./rootfs || echo 'clean'
```

### 5. Discovering what is in a registry

Walk an unfamiliar registry without scripting against its HTTP API:

```bash
d8 cr catalog registry.internal | head
d8 cr ls registry.internal/team-x/payments
d8 cr ls --omit-digest-tags registry.internal/team-x/payments
```

The `--omit-digest-tags` filter strips the `sha256-*` tags that
[cosign](https://github.com/sigstore/cosign) and similar tools leave behind
so you see only "real" human-readable tags.

### 6. Signature and audit pipelines

Feed a raw, byte-stable manifest into a signature verifier or store it for
audit:

```bash
d8 cr manifest registry.internal/app:v1.4.2 > app-v1.4.2.manifest.json
sha256sum app-v1.4.2.manifest.json
cosign verify --key cosign.pub --signature app-v1.4.2.sig app-v1.4.2.manifest.json
```

### 7. Pushing build output to a registry

A common CI step at the tail of a build: load a tarball produced by the
builder, then publish it with a digest reference written back to a file
that downstream steps can read:

```bash
d8 cr push ./build/out.tar \
  registry.internal/app:${CI_COMMIT_TAG} \
  --image-refs ./build/image.ref

cat ./build/image.ref
# registry.internal/app@sha256:...
```

### 8. Local registry / `kind` workflows

Push to plain-HTTP local registries without fighting TLS:

```bash
# kind ships a local registry on 5000/HTTP
d8 cr push --insecure ./out.oci localhost:5000/dev/app:latest
```

Localhost is auto-allowed for HTTP, so `--insecure` is only needed for
non-loopback plain-HTTP registries.

### 9. Diffing two images

Compare the file listing or a specific file across two tags:

```bash
diff \
  <(d8 cr fs ls -l registry.internal/app:v1.4.1 | sort) \
  <(d8 cr fs ls -l registry.internal/app:v1.4.2 | sort)

diff \
  <(d8 cr fs cat registry.internal/app:v1.4.1 etc/app/config.yaml) \
  <(d8 cr fs cat registry.internal/app:v1.4.2 etc/app/config.yaml)
```

---

## Examples

A copy-pasteable cheat-sheet of the most common invocations.

### Pull

```bash
# Default tarball (single image, docker load compatible)
d8 cr pull --platform linux/amd64 alpine:3.19 ./alpine.tar

# Multi-image tarball
d8 cr pull --platform linux/amd64 alpine:3.19 busybox:1.36 ./images.tar

# OCI layout, all platforms preserved
d8 cr pull --format oci alpine:3.19 ./alpine-oci

# OCI layout with a shared layer cache (resumable across runs)
d8 cr pull --format oci --cache-path ~/.cache/d8-cr alpine:3.19 ./alpine-oci

# Legacy docker tarball (single image, lossy on digests)
d8 cr pull --format legacy --platform linux/amd64 alpine:3.19 ./alpine-legacy.tar
```

### Push

```bash
# Push a docker tarball
d8 cr push ./image.tar registry.internal/myapp:v1

# Push an OCI layout (single manifest)
d8 cr push ./image-oci registry.internal/myapp:v1

# Push an OCI layout as a multi-manifest index
d8 cr push --index ./bundle-oci registry.internal/bundle:v1

# Push and record the digest reference
d8 cr push ./image.tar registry.internal/myapp:v1 --image-refs ./image.ref

# Push to a plain-HTTP local registry
d8 cr push --insecure ./image.tar localhost:5000/myapp:v1
```

### Inspect

```bash
# Manifest (raw bytes, pipe-friendly)
d8 cr manifest alpine:3.19 | jq .

# Multi-arch: pick a platform
d8 cr manifest --platform linux/arm64 alpine:3.19 | jq .

# Config (entrypoint, env, history, ...)
d8 cr config alpine:3.19 | jq '.config.Env'

# Digest of a remote image
d8 cr digest alpine:3.19
# sha256:abcdef...

# Digest with full pinnable reference
d8 cr digest --full-ref alpine:3.19
# index.docker.io/library/alpine@sha256:abcdef...

# Digest of a local tarball, selecting a specific tag
d8 cr digest --tarball ./images.tar alpine:3.19
```

### List

```bash
# Tags
d8 cr ls registry.internal/team/app
d8 cr ls --full-ref registry.internal/team/app
d8 cr ls --omit-digest-tags registry.internal/team/app

# Repositories
d8 cr catalog registry.internal
d8 cr catalog --full-ref registry.internal
```

### Export (crane-compatible filesystem tar)

```bash
# Stream to stdout, pipe to tar
d8 cr export alpine:3.19 - | tar tf - | head

# Write to file
d8 cr export alpine:3.19 alpine-fs.tar
```

### Filesystem inspection (`d8 cr fs`)

```bash
# List the root of the merged filesystem
d8 cr fs ls alpine:3.19

# Long format (mode + size)
d8 cr fs ls -l alpine:3.19 etc

# Tree, with sizes, capped at depth 2
d8 cr fs tree alpine:3.19 --size -L 2

# Tree, directories first
d8 cr fs tree alpine:3.19 etc --dirsfirst

# Read a single file
d8 cr fs cat alpine:3.19 etc/os-release

# Extract the whole filesystem (with safety checks)
d8 cr fs extract alpine:3.19 -o ./alpine-rootfs
```

### Global flags in action

```bash
# Verbose: show go-containerregistry debug logs
d8 cr -v ls registry.internal/team/app

# Insecure: plain HTTP + skip TLS verify
d8 cr --insecure ls localhost:5000/myapp

# Platform: pick a specific arch from a multi-arch index
d8 cr --platform linux/arm64/v8 manifest registry.internal/app:v1

# Foreign layers: include them on push
d8 cr --allow-nondistributable-artifacts push ./win.tar registry.internal/win:base
```

---

## Architecture

`d8 cr` is wired in `internal/cr/cmd/cr.go` and is composed of small,
single-responsibility packages:

| Package | Responsibility |
|---|---|
| `internal/cr/cmd` | Root cobra command, persistent flags, integration tests |
| `internal/cr/cmd/basic` | Top-level subcommands: `pull`, `push`, `export`, `ls`, `catalog`, `manifest`, `config`, `digest` |
| `internal/cr/cmd/fs` | `fs` subtree: `ls`, `cat`, `tree`, `extract` |
| `internal/cr/cmd/completion` | Shell-completion helpers (bounded, fail-silent) |
| `internal/cr/cmd/rootflagnames` | Single source of truth for persistent flag names |
| `internal/cr/internal/registry` | Thin domain layer over `go-containerregistry` (fetch / push / list / options) |
| `internal/cr/internal/image` | Pure operations over `v1.Image` / `v1.ImageIndex` (multi-arch resolve, ...) |
| `internal/cr/internal/imageio` | Disk I/O: tarball + OCI image-layout load/save |
| `internal/cr/internal/imagefs` | Merged-filesystem reader with whiteout handling and safe extraction |
| `internal/cr/internal/output` | Text rendering for `fs` subcommands |

Subcommands share a single `*registry.Options` populated by the root
command's `PersistentPreRunE`. This means flag values set on the root are
always observed by leaf commands before their `RunE` runs - the wiring is
fully exercised by hermetic integration tests in
`internal/cr/cmd/integration_test.go` against an in-memory registry.
