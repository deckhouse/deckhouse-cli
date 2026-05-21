# `d8 mirror pull --proxy-registry`

This document describes `--proxy-registry`, a `d8 mirror pull` mode that lets you pull a Deckhouse Kubernetes Platform bundle from a source registry that does **not** implement the registry catalog API (`/v2/_catalog`, `/v2/<name>/tags/list`) — typically a caching/transparent proxy in front of an upstream Deckhouse registry.

For everything else about `d8 mirror pull` (authentication, bundle layout, push command, troubleshooting, etc.) see [README.MD](./README.MD).

---

## Why this mode exists

The default `d8 mirror pull` discovers tags by calling the registry's `ListTags` endpoint once per repository: once for the platform release-channel repo, once for the modules root, and once per module. That works against `registry.deckhouse.ru` and any registry that implements the catalog API.

Caching/proxy registries usually refuse the catalog API outright — they only serve the per-tag manifest and blob endpoints. Against such a registry, the default discovery path returns an empty tag list and the resulting bundle is empty too, even when the cache already holds the exact tags you asked for.

`--proxy-registry` swaps the catalog calls for a deterministic forward-probe walk over semver tags, seeded from the version anchors you supply via `--include-platform` and `--include-module`. Every tag the registry actually serves ends up in the bundle; every tag that returns `404 Not Found` is treated as "not in the cache" and skipped.

---

## When to use

| Goal | Recommended flag |
|------|-----------------|
| Pull from `registry.deckhouse.ru` directly | omit `--proxy-registry` |
| Pull from a caching/proxy registry that has already cached the desired versions | `--proxy-registry` + `--include-platform` + `--include-module` |
| Pull from a registry that supports the catalog API but you still want range-based filtering | omit `--proxy-registry`, use `--include-platform` alone |

---

## End-to-end flow

When you run `d8 mirror pull --proxy-registry` against a caching registry, the pipeline takes the following branches relative to a regular pull. Steps marked **(probe)** are the only ones that differ from the default flow; everything else (image pulling, layout writing, bundle packing, GOST digests) is identical.

```
                       ┌──────────────────────────────────────┐
1.  CLI parses flags   │ --proxy-registry on?                 │
                       │  yes → require --include-platform    │
                       │        and/or --include-module       │
                       │        each with @<constraint>       │
                       │        reject --deckhouse-tag and    │
                       │        --since-version               │
                       └──────────────────────────────────────┘
                                       │
                                       ▼
2.  Connect to source registry — same authn as normal pull.

3.  Platform component (skipped if --no-platform):
    a. Fetch release-channel snapshots (alpha/beta/early-access/stable/
       rock-solid, plus LTS if it exists). These are direct per-tag
       GET requests, so they work against a proxy registry.
    b. (probe) Instead of ListTags on the release-channel repo, run the
       forward-probe walk seeded from --include-platform's lower bound.
       Each candidate version becomes a HEAD against
       <source>/release-channel:vX.Y.Z.
    c. Apply the normal latest-patch-per-minor and inclusive-anchor
       rules to the probe result; merge with channel snapshots; drop
       channels that no longer fall inside the constraint window.
    d. Pull installer, standalone installer, and Deckhouse platform
       images for the resolved version set (per-tag GETs, no listing).
    e. Pack into platform.tar.

4.  Installer component (skipped if --no-installer):
    Pull <source>/install:<--installer-tag> directly (single tag, no
    listing needed; works on any registry).

5.  Security databases component (skipped if --no-security-db):
    Pull security DB images by their well-known tags (per-tag GETs).

6.  Modules component (skipped if --no-modules and not --only-extra-images):
    For each module name pulled from the --include-module whitelist
    (proxy mode never asks the registry "what modules exist?"):

    a. Walk release channels per module — same per-tag GET pattern as
       step 3a. Missing channels are tolerated.
    b. (probe) Instead of Module(name).ListTags, run the forward-probe
       walk seeded from the module's --include-module@<constraint>.
       Each candidate becomes a HEAD against
       <source>/modules/<name>:vX.Y.Z.
    c. Apply the filter's latest-patch-per-minor / anchor rules; merge
       with channel-snapshot versions.
    d. Pull module images, release version images, and any internal
       digest images referenced by images_digests.json (per-digest GETs).
    e. Pull extra images discovered via extra_images.json.
    f. Pull VEX attestations (unless --skip-vex-images) by deriving the
       .att tag from each image digest and checking if it exists.
    g. Pack into module-<name>.tar.

7.  GOST digests (if --gost-digest):
    Compute STREEBOG checksums next to each .tar / .chunk.

8.  Cleanup tmp directory.
```

The "(probe)" steps are the only network behaviour that changes — they're the ones a proxy registry rejects by returning an empty tag list. Every other step uses per-tag GET / HEAD requests, which is exactly what a proxy registry is good at.

---

## Walk algorithm

1. Start from the lowest semver literal named in the constraint (e.g. `^1.64.0` starts at `1.64.0`, `>=1.64 <=1.68` starts at `1.64.0`).
2. Increment the patch component by 1 and re-probe. Keep going as long as the registry returns the manifest **and** the version still satisfies the constraint.
3. When a patch step fails, advance to `(major, minor+1, 0)` and probe once. If it exists, resume step 2 from there.
4. When the new-minor probe also fails, advance to `(major+1, 0, 0)` and probe once. If it exists, resume step 2.
5. When the new-major probe also fails (or falls outside the constraint), stop. The bundle contains every tag confirmed during the walk.

The walk never invents tags: only versions that the registry confirmed are written to the bundle, and the latest-patch-per-minor / inclusive-anchor rules described in [README.MD: Platform Version Filtering](./README.MD#platform-version-filtering) and [README.MD: Module Filtering](./README.MD#module-filtering) are applied to the result the same way they would be after a normal `ListTags`.

---

## Worked example

Assume the source proxy registry has already cached the following platform release tags (every other tag returns `404 Not Found`):

```
v1.64.0, v1.64.1, v1.64.2
v1.65.0
v1.66.0, v1.66.1
```

You run:

```bash
d8 mirror pull /tmp/d8-bundle \
  --source proxy.internal.company.com/deckhouse/ee \
  --license $LICENSE_TOKEN \
  --proxy-registry \
  --include-platform ">=1.64.0 <=1.68.0" \
  --no-modules
```

The platform probe issues the following HEAD requests in order. The right column shows the state transition the algorithm makes after each result.

| # | HEAD request | Result | Probe state after |
|---|------------------------------------------------|----------|------------------------------------------------------|
| 1 | `release-channel:v1.64.0` | 200 OK | append v1.64.0, continue patch loop at 1.64.1 |
| 2 | `release-channel:v1.64.1` | 200 OK | append v1.64.1, continue patch loop at 1.64.2 |
| 3 | `release-channel:v1.64.2` | 200 OK | append v1.64.2, continue patch loop at 1.64.3 |
| 4 | `release-channel:v1.64.3` | 404 | patch loop ends; jump to next minor (1.65.0) |
| 5 | `release-channel:v1.65.0` | 200 OK | append v1.65.0, resume patch loop at 1.65.1 |
| 6 | `release-channel:v1.65.1` | 404 | patch loop ends; jump to next minor (1.66.0) |
| 7 | `release-channel:v1.66.0` | 200 OK | append v1.66.0, resume patch loop at 1.66.1 |
| 8 | `release-channel:v1.66.1` | 200 OK | append v1.66.1, continue patch loop at 1.66.2 |
| 9 | `release-channel:v1.66.2` | 404 | patch loop ends; jump to next minor (1.67.0) |
| 10 | `release-channel:v1.67.0` | 404 | new-minor probe failed; jump to next major (2.0.0) |
| 11 | (skipped) `v2.0.0` is outside `<=1.68.0` | n/a | constraint excludes 2.0.0; **probe terminates** |

After the probe finishes, the downstream pipeline keeps only the highest patch per `(major, minor)` (so `v1.64.0` and `v1.64.1` are dropped because `v1.64.2` is newer in the same minor). The final platform set written to `platform.tar` is therefore:

```
v1.64.2, v1.65.0, v1.66.1
```

…plus any version pinned by an existing release channel snapshot (alpha/beta/etc.) that also satisfies the constraint.

Note that `v1.67.x` and `v1.68.x` would have been pulled too if the proxy registry had cached them — the probe asks about `v1.67.0` in step 10 and stops only because the answer is `404`. This is intentional: the proxy registry is the source of truth about which tags it can actually serve, and the probe's job is to faithfully reproduce that subset.

---

## What "exists" and "not found" mean on the wire

The probe relies on the standard registry-v2 manifest endpoint:

```
HEAD /v2/<repo>/manifests/<tag>
```

The mapping from HTTP response to probe action is:

| HTTP response | Treated as | Probe action |
|----------------|------------|---------------------------------------------------|
| `200 OK` | Tag exists | Append to bundle, advance patch by 1 |
| `404 Not Found` (incl. `MANIFEST_UNKNOWN` body) | Tag absent | End patch loop, fall through to next minor / major |
| `401 Unauthorized`, `403 Forbidden` | Auth failure | Abort the entire pull with the error |
| `5xx`, network error, timeout | Real failure | Abort the entire pull with the error |

In other words: only an unambiguous "the registry does not have this tag" stops the probe — everything else is propagated so a transient network blip never gets silently mistaken for "release series ended". This is the same error policy used by `CheckImageExists` in the rest of the pull pipeline.

If a proxy registry returns `200 OK` for tags it later refuses to serve the manifest of, the per-tag GET in the normal pull step (step 3d / 6d of the flow) will surface a clear error against that exact tag.

---

## Per-component behaviour in proxy mode

| Component | Default mode | `--proxy-registry` mode |
|-----------|--------------|--------------------------|
| Source `_catalog` | Not used (CLI pulls per-repo) | Not used |
| Modules root `ListTags` (used to enumerate the module catalog) | One catalog call lists every module | **Skipped.** Module names come from `--include-module` directly |
| Platform release-channel `ListTags` | One catalog call lists every release tag | **Skipped.** Tags come from the forward-probe walk seeded by `--include-platform` |
| Per-module `ListTags` | One catalog call per module lists tag history | **Skipped.** Tags come from the forward-probe walk seeded by `--include-module@<constraint>` |
| Per-channel `GetImage` / `GetMetadata` | Per-tag GET, works on proxies | **Unchanged** — still used |
| Per-tag manifest `GET` (during actual pull) | Per-tag GET, works on proxies | **Unchanged** — still used |
| Per-tag manifest `HEAD` (CheckImageExists) | Used opportunistically (e.g. LTS-channel existence, VEX detection) | **Used as the probe primitive** |
| Internal digests (`images_digests.json`) | Pulled by digest reference | **Unchanged** — proxy registries serve digest pulls fine |
| Extra images (`extra_images.json`) | Per-tag GETs after parsing the JSON | **Unchanged** |
| VEX attestations (`.att` tags) | Existence check + per-tag GET | **Unchanged** |

The reason this works: every operation other than the three `ListTags` calls is already a per-resource HTTP request, which a proxy registry handles by either serving from its cache or forwarding to the upstream registry on demand.

---

## Required flag combinations

| Other flags | Required with `--proxy-registry` |
|------------|----------------------------------|
| platform is being pulled (default) | `--include-platform <constraint>` |
| `--no-platform` is set | `--include-platform` is **not** required |
| modules are being pulled (default) or `--only-extra-images` | At least one `--include-module <name>@<constraint>`. Every entry **must** include `@<constraint>` — `--include-module foo` alone is rejected because the probe would otherwise start at `v0.0.0` and silently miss every real tag |
| `--no-modules` is set | `--include-module` is **not** required |
| `--exclude-module` | Honoured (subtracts from the include list) |
| `--deckhouse-tag` | **Conflict**: a single pinned tag is already a direct check; do not combine with `--proxy-registry` |
| `--since-version` | **Conflict**: `--since-version` has no upper bound and the probe cannot terminate. Use `--include-platform` with an explicit range instead |
| `--dry-run` | Honoured — runs the probe and prints the plan without downloading any blobs |

---

## Performance characteristics

Each probe step is one `HEAD` to the source registry, costing on the order of one round-trip. The total number of probe requests for a single component is therefore roughly:

```
probe_requests ≈ (# of patches actually present) + (# of "patch series ended" gaps) + (# of "minor series ended" gaps) + 1
```

In practice, for a constraint like `>=1.64.0 <=1.68.0` against a registry that holds all five minors with a handful of patches each, the platform probe issues somewhere between 20 and 40 HEAD requests before terminating. Per-module probes are similar in shape but smaller — most modules have far fewer historical versions than the platform itself.

If you have lots of modules listed in `--include-module`, the per-module probes run sequentially in the existing loop (one module at a time, same as the regular pull). The dominant cost of the pull is still the actual image data transfer, not the probe.

---

## Limitations and known caveats

1. **Sparse patch ranges:** the probe stops at the first missing patch in a series. If a registry's cache has `v1.64.0`, `v1.64.2`, `v1.64.5` but not `v1.64.1`, the probe will capture `v1.64.0` and then jump to `v1.65.0` because `v1.64.1` is missing. Patches `v1.64.2` and `v1.64.5` are not retried in proxy mode. Pre-warm the cache before pulling, or use `--include-platform "=v1.64.5"` (exact-tag form) to mirror a specific patch unconditionally.
2. **Sparse minor ranges:** the same logic applies to minors. If the cache has `v1.64.x` and `v1.66.x` but no `v1.65.x` at all, the new-minor probe at `v1.65.0` fails, the new-major probe at `v2.0.0` fails, and the walk terminates — `v1.66.x` is missed. Use a tighter constraint range per minor, or warm the missing minor in the cache first.
3. **Pre-release versions (`v1.65.0-rc.1`) are never probed:** the walk increments only the `(major, minor, patch)` triple. If your proxy holds only `-rc.x` tags for a particular series, pull them directly via `--include-platform "=v1.65.0-rc.1"`.
4. **Custom non-semver tags are not probed:** the probe is semver-only by construction. Use the exact-tag form (`--include-platform "=customtag"` or `--include-module name@=customtag`) to pull non-semver tags from a proxy.
5. **The probe cost grows with constraint width:** a very wide constraint like `>=0.0.0 <100.0.0` will probe a lot of empty (major, minor) combinations before terminating. Always supply a realistic lower bound in `--include-platform` / `--include-module`.
6. **No fallback to `ListTags`:** if `--proxy-registry` is set and a particular registry path actually supports the catalog API, the catalog is still ignored. Drop the flag for that pull if you want to use catalog-based discovery.

---

## Examples

```bash
# Pull a platform window and one module from a proxy registry
d8 mirror pull /tmp/d8-bundle \
  --source proxy.internal.company.com/deckhouse/ee \
  --license $LICENSE_TOKEN \
  --proxy-registry \
  --include-platform ">=1.64.0 <=1.68.0" \
  --include-module prometheus@^1.0.0

# Modules only — skip the platform probe entirely
d8 mirror pull /tmp/d8-bundle \
  --source proxy.internal.company.com/deckhouse/ee \
  --license $LICENSE_TOKEN \
  --proxy-registry \
  --no-platform \
  --include-module prometheus@^1.0.0 \
  --include-module ingress-nginx@^1.5.0

# Platform only — skip modules entirely
d8 mirror pull /tmp/d8-bundle \
  --source proxy.internal.company.com/deckhouse/ee \
  --license $LICENSE_TOKEN \
  --proxy-registry \
  --no-modules \
  --include-platform "^1.64.0"

# Validate the proxy-mode plan before doing any actual pulling
d8 mirror pull /tmp/d8-bundle \
  --source proxy.internal.company.com/deckhouse/ee \
  --license $LICENSE_TOKEN \
  --proxy-registry \
  --include-platform ">=1.64.0 <=1.68.0" \
  --include-module prometheus@^1.0.0 \
  --dry-run
```
