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
| Pull installer to tmpDir | yes | **yes** (needed to read `images_digests.json`) |
| Pull release-channel metadata | yes | yes |
| Download platform/module/security blobs | yes | **no** |
| Write `platform.tar`, `security.tar`, module tarballs | yes | **no** |
| Write `deckhousereleases.yaml` | yes | **no** |
| Compute GOST digests | yes | **no** |

Installer OCI layouts land in `--tmp-dir` (or `<bundle-path>/.tmp`) so the tool can
extract the built-in image digest list from `deckhouse/candi/images_digests.json`.  
The **bundle directory** (first positional argument) remains empty.

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
INFO  ╔ Pull release channels and installers
INFO  ║ [1 / 1] Pulling registry.deckhouse.io/deckhouse/install:v1.69.0
INFO  ╚ Pull release channels and installers succeeded in …
INFO  Extracting images digests from Deckhouse installer v1.69.0
INFO  Deckhouse digests found: 319
INFO  Found 320 images
INFO  [dry-run] Platform images that would be pulled:
INFO    registry.deckhouse.io/deckhouse/fe@sha256:…
INFO    registry.deckhouse.io/deckhouse/fe/release-channel:v1.69.0
INFO    registry.deckhouse.io/deckhouse/fe/install:v1.69.0
  …
INFO  [dry-run] Done. No images were downloaded.
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

# Platform service level (installer pulled to tmpDir, bundle stays empty)
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
| `TestDryRun_InstallerPulledToTmpDir` | platform service: `<tmpDir>/platform/install/` exists |

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
2. The bundle directory is **empty** — no `.tar` output
3. The tmp directory is **non-empty** — OCI layouts were written (installer pull), proving
   `images_digests.json` extraction was attempted

Sample passing output:

```
=== RUN   TestDryRunRealRegistry
…
INFO  Deckhouse digests found: 319
INFO  Found 320 images
INFO  [dry-run] Platform images that would be pulled:
INFO    registry.deckhouse.io/deckhouse/fe@sha256:e927fc9…
  … 320 lines …
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
            │         ├─ pullDeckhouseReleaseChannels()   ← writes to tmpDir
            │         ├─ pullInstallers()                 ← writes to tmpDir ← KEY STEP
            │         ├─ pullStandaloneInstallers()       ← writes to tmpDir
            │         │   (pullDeckhouseImages SKIPPED in dry-run)
            │         ├─ ExtractImageDigestsFromInstaller ← reads images_digests.json
            │         └─ [dry-run guard] print plan → return nil
            │              (no platform.tar written)
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
    if DryRun → print "[dry-run] Done." → return nil
    else      → computeGOSTDigests, finalCleanup
```

The installer image **is** pulled to `tmpDir` in dry-run mode because
`images_digests.json` inside it is the only source for the ~300 component image
digest references that the platform bundle would contain. Without this step,
dry-run could only report the 5–10 top-level image tags, missing the vast majority
of what a real pull actually downloads.

---

## Temporary files

| Location | Created in dry-run? | Description |
|----------|---------------------|-------------|
| `<tmpDir>/platform/install/` | **yes** | Installer OCI layout |
| `<tmpDir>/platform/install-standalone/` | **yes** | Standalone installer OCI layout |
| `<tmpDir>/platform/release/` | **yes** | Release-channel metadata OCI layout |
| `<bundleDir>/platform.tar` | no | Not created |
| `<bundleDir>/security.tar` | no | Not created |
| `<bundleDir>/modules-*.tar` | no | Not created |
| `<bundleDir>/deckhousereleases.yaml` | no | Not created |

`tmpDir` is cleaned up by a subsequent normal pull or can be removed manually.
