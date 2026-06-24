# `d8 mirror pull --dry-run` Guide

## What dry-run does

`--dry-run` runs the full planning pipeline of `d8 mirror pull` — version resolution,
release-channel discovery, module filtering, installer tag lookup — then **prints the
complete list of images that would be downloaded** and exits without writing any bundle
output to the bundle directory.

The key distinction from a no-op:

| Step | Normal pull | Dry-run |
|------|-------------|---------|
| Validate registry access | yes | yes |
| Resolve versions / channels | yes | yes |
| Read installer `images_digests.json` (platform) | via OCI layout in tmpDir | **streamed from the registry** (no layout) |
| Stage installer/security/module OCI layout dirs in tmpDir | yes (with blobs) | **scaffolding only** (no image blobs) |
| Pull release-channel metadata | yes | yes |
| Download platform/module/security blobs | yes | **no** |
| Write `platform.tar`, `security.tar`, module tarballs | yes | **no** |
| Write `deckhousereleases.yaml` | yes | **no** |
| Compute GOST digests | yes | **no** |

In dry-run the **platform** service streams the built-in image digest list
(`images_tags.json` / `images_digests.json`) straight from the remote installer image,
layer by layer, without writing an OCI layout. The `installer`, `security` and `modules`
services still create their OCI layout directories under `--tmp-dir` (or
`<bundle-path>/.tmp`), but in dry-run they pull no image blobs (only layout scaffolding),
so `--tmp-dir` ends up non-empty while the **bundle directory** (first positional
argument) remains empty.

---

## CLI usage

```bash
d8 mirror pull --dry-run <bundle-path> [flags]
```

All normal `pull` flags are accepted. Dry-run respects every filter and skip flag
(`--deckhouse-tag`, `--since-version`, `--no-platform`, `--no-modules`, etc.).

### Minimal example — exact tag, platform only

```bash
d8 mirror pull --dry-run /tmp/bundle \
  --source registry.deckhouse.io/deckhouse/fe \
  --license <your-token> \
  --deckhouse-tag v1.69.0 \
  --no-modules \
  --no-security-db
```

Expected output (abbreviated):

```
INFO  Skipped releases lookup as tag "v1.69.0" is specifically requested with --deckhouse-tag
INFO  Deckhouse releases to pull: [1.69.0]
INFO  Searching for Deckhouse built-in modules digests
INFO  [dry-run] Streaming installer metadata for v1.69.0 from registry
INFO  Deckhouse digests found: 319
INFO  [dry-run] Platform images that would be pulled:
INFO    Deckhouse components: 319 images
INFO      registry.deckhouse.io/deckhouse/fe@sha256:…
INFO    Release channels: 1
INFO      registry.deckhouse.io/deckhouse/fe/release-channel:v1.69.0
INFO    Installer: 1
INFO      registry.deckhouse.io/deckhouse/fe/install:v1.69.0
INFO    Standalone installer: 1
INFO    Total: 322 platform images
INFO  [dry-run] Installer images that would be pulled:
INFO    registry.deckhouse.io/deckhouse/fe/install:v1.69.0
  …
  No images were downloaded (dry-run).
```

### All components

```bash
d8 mirror pull --dry-run /tmp/bundle \
  --source registry.deckhouse.io/deckhouse/fe \
  --license <your-token> \
  --deckhouse-tag v1.69.0
```

This resolves platform images, installer, security databases, and all modules.
No blobs are downloaded; the bundle directory stays empty.

### Since a minimum version (channel-based pull)

```bash
d8 mirror pull --dry-run /tmp/bundle \
  --source registry.deckhouse.io/deckhouse/fe \
  --license <your-token> \
  --since-version 1.68.0
```

### Module whitelist

```bash
d8 mirror pull --dry-run /tmp/bundle \
  --source registry.deckhouse.io/deckhouse/fe \
  --license <your-token> \
  --deckhouse-tag v1.69.0 \
  --include-module stronghold \
  --include-module commander-agent
```

---

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Planning succeeded (or was cancelled by the user) |
| non-0 | Registry unreachable, invalid flags, or licence denied |

---

## Testing dry-run

### Unit tests (offline, stub registry)

Unit tests live alongside the packages they cover and run with `go test` — no network
access, no credentials needed. The stub registry is activated via:

```
STUB_REGISTRY_CLIENT=true
```

The stub seeds versions `v1.68.0`–`v1.72.10`, channels `alpha`/`beta`/`early-access`/
`stable`/`rock-solid`, and several module tags.

#### Run all dry-run unit tests

```bash
go test ./internal/mirror/... -run 'TestDryRun' -v -timeout 120s
```

#### Specific packages

```bash
# Pull-command level (flag registration, no bundle output, exit 0)
go test ./internal/mirror/cmd/pull/ -run 'TestDryRun' -v

# Platform service level (digests streamed, no platform OCI layout, bundle stays empty)
go test ./internal/mirror/platform/ -run 'TestDryRun' -v
```

#### What the tests assert

| Test | What it checks |
|------|----------------|
| `TestDryRunFlagRegistered` | `--dry-run` cobra flag exists and defaults to `false` |
| `TestDryRunNoBundleOutput` | bundleDir has no `.tar`/`.chunk`/`.gostsum` after full run |
| `TestDryRunNoBundleWithNoPlatform` | same, with `--no-platform --no-security-db` |
| `TestDryRunWithDeckhouseTag` | specific `--deckhouse-tag` works in dry-run |
| `TestDryRunExitsZeroOnSuccess` | `Execute()` returns `nil` |
| `TestDryRun_NoBundleFilesWritten` | platform service: bundleDir empty |
| `TestDryRun_NoOCILayoutCreated` | platform service: `<tmpDir>/platform/install/` is **not** created (digests are streamed) |
| `TestDryRun_WorkingDirHasLayouts` | installer / security service: OCI layout dir staged in workingDir, bundleDir empty |

### Integration smoke test (real registry)

`TestDryRunRealRegistry` in
`internal/mirror/cmd/pull/pull_realregistry_test.go`
is skipped automatically unless both environment variables are set:

| Variable | Value |
|----------|-------|
| `D8_TEST_REGISTRY` | `registry.deckhouse.io/deckhouse/fe` |
| `D8_TEST_LICENSE_TOKEN` | your Deckhouse license key |

```bash
D8_TEST_REGISTRY=registry.deckhouse.io/deckhouse/fe \
D8_TEST_LICENSE_TOKEN=<token> \
  go test ./internal/mirror/cmd/pull/ \
    -run TestDryRunRealRegistry \
    -v -timeout 300s
```

The test asserts:
1. `Execute()` returns `nil`
2. The bundle directory has no `.tar` / `.chunk` output
3. The tmp directory is **non-empty** - the `installer`, `security` and `modules` services
   stage OCI layout scaffolding there (no image blobs); the platform digest list itself is
   streamed from the registry, not staged

Sample passing output:

```
=== RUN   TestDryRunRealRegistry
…
INFO  Deckhouse digests found: 319
INFO  [dry-run] Platform images that would be pulled:
INFO    Deckhouse components: 319 images
INFO      registry.deckhouse.io/deckhouse/fe@sha256:e927fc9…
INFO    Total: 322 platform images
  … more dry-run plans for installer / security / modules …
    pull_realregistry_test.go:94: tmpDir files written during dry-run: 51
    pull_realregistry_test.go:95: bundleDir entries (must be 0): 0
--- PASS: TestDryRunRealRegistry (79.99s)
```

---

## How dry-run works internally

```
Puller.Execute()
  └─ mirror.NewPullService(… DryRun: pullflags.DryRun …)
       └─ PullService.Pull()
            ├─ platform.Service.PullPlatform()          [DryRun=true]
            │    ├─ validatePlatformAccess()              ← real network call
            │    ├─ findTagsToMirror()                    ← real network call
            │    ├─ downloadList.FillDeckhouseImages()    ← in-memory
            │    └─ pullDeckhousePlatform()
            │         └─ [DryRun] pullDeckhousePlatformDryRun()
            │              ├─ extractImageDigestsFromRemote()  ← streams images_tags.json /
            │              │      images_digests.json from the remote image (no OCI layout)
            │              └─ print plan → return nil
            │            (pullDeckhouseReleaseChannels / pullInstallers /
            │             pullStandaloneInstallers / pullDeckhouseImages all SKIPPED)
            ├─ installer.Service.PullInstaller()         [DryRun=true]
            │    ├─ validateInstallerAccess()
            │    ├─ findTagsToMirror()
            │    ├─ downloadList.FillInstallerImages()
            │    └─ [dry-run guard] print plan → return nil
            ├─ security.Service.PullSecurity()           [DryRun=true]
            │    ├─ validateSecurityAccess()
            │    ├─ downloadList.FillSecurityImages()
            │    └─ [dry-run guard] print plan → return nil
            └─ modules.Service.PullModules()             [DryRun=true]
                 ├─ discover modules (ListRepositories)
                 ├─ per module: extractVersionsFromReleaseChannels()
                 └─ [dry-run guard] print plan → return nil

  After Pull() returns:
    if DryRun → print summary "No images were downloaded (dry-run)." → return nil
    else      → computeGOSTDigests, finalCleanup
```

The platform service reads `images_digests.json` (or `images_tags.json`) **without**
pulling the installer to `tmpDir`: `extractImageDigestsFromRemote` streams just the layer
containing that file straight from the registry. It is the only source for the ~300
component image digest references that the platform bundle would contain - without it,
dry-run could only report the 5-10 top-level image tags, missing the vast majority of
what a real pull actually downloads.

---

## Temporary files

| Location | Created in dry-run? | Description |
|----------|---------------------|-------------|
| `<tmpDir>/platform/...` | **no** | Platform digests are streamed; no platform OCI layout is written |
| `<tmpDir>/installer/` | **yes** | Installer OCI layout dir (scaffolding only, no blobs) |
| `<tmpDir>/security*/`, `<tmpDir>/modules*/` | **yes** | Security / module OCI layout dirs (scaffolding only, no blobs) |
| `<bundleDir>/platform.tar` | no | Not created |
| `<bundleDir>/security.tar` | no | Not created |
| `<bundleDir>/modules-*.tar` | no | Not created |
| `<bundleDir>/deckhousereleases.yaml` | no | Not created |

`tmpDir` is cleaned up by a subsequent normal pull or can be removed manually.
