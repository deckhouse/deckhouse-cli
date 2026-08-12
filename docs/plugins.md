# d8 Plugins (`d8 plugins`)

Plugins are versioned binaries distributed through the cluster registry.
`d8` installs, updates, and removes them for you.

**Contents:** [Source](#plugin-source) · [Commands](#commands) ·
[Versions & majors](#versions-majors-and-switching) ·
[Requirements](#requirements) ·
[Flags & env](#flags-and-environment-variables) ·
[Troubleshooting](#troubleshooting) · [Advanced](#advanced-hidden-flags)

> [!NOTE]
> The `d8 plugins` command group is hidden from the root `--help` while the
> plugin ecosystem rolls out. The commands below are fully functional.

## Plugin source

Plugins are pulled from the in-cluster **registry-packages-proxy**, the same
channel as d8 self-update. This is the only supported path: every `d8 plugins`
command reaches the registry through the proxy, so a reachable cluster is
required. (A hidden, temporary `--source` flag pulls straight from a registry
repo instead - see [Advanced](#advanced-hidden-flags) - but it bypasses the
cluster and is not the intended flow.) The access model:

- Authentication: the **Bearer token** from your kubeconfig (client
  certificates do not work).
- Authorization: the ClusterRole
  `d8:registry-packages-proxy:cli-download`, bound by the cluster
  administrator. A denial is cached for 30 seconds, so a `403` caught before
  the binding existed clears in half a minute.
- Endpoint: discovered automatically through your kubeconfig's API server,
  which also needs `get` on the `registry-packages-proxy` Ingress; override
  with `--rpp-endpoint` / `D8_RPP_ENDPOINT`, pass a private CA with
  `--rpp-ca-file`.

Starting from scratch? Follow
[self-update.md - Getting started](self-update.md#getting-started): the same
kubeconfig and the same grant work for plugins.

The access model is the same as for d8 self-update (see
[self-update.md - How access works](self-update.md#how-access-works) for the
OIDC-kubeconfig and endpoint-discovery details), down to the ClusterRole:
plugins are published under `deckhouse-cli/plugins/<name>` and travel the same
`/v1/images/` route, so `cli-download` covers both. The administrator grants it
as described in
[Granting access to CLI downloads](/products/kubernetes-platform/documentation/v1/modules/registry-packages-proxy/#granting-access-to-cli-downloads).

## Commands

| Command | What it does |
|---|---|
| `d8 plugins versions <name>` | lists all published versions of one plugin |
| `d8 plugins install <name>` | installs the newest version compatible with your cluster |
| `d8 plugins install <name> --version X` | installs an exact version |
| `d8 plugins install <name> --use-major N` | switches majors explicitly |
| `d8 plugins update <name>` / `update all` | updates within the current major |
| `d8 plugins list` | shows installed plugins (the proxy serves no catalog, so available plugins are not listed) |
| `d8 plugins contract <name>` | shows a plugin's contract: version, description, requirements |
| `d8 plugins remove <name>` / `remove all` | removes plugins |

```console
$ d8 plugins versions package
  v0.1.2   newer
* v0.0.21  current
  v0.0.20

$ d8 plugins install package
Installing plugin: package
Tag: v0.0.21
...
✓ Plugin 'package' successfully installed!
```

## Versions, majors and switching

Plugins are stored per major version, with a `current` symlink selecting the
active one:

```
/opt/deckhouse/lib/deckhouse-cli/plugins/<name>/v<major>/
```

Rules that follow from this layout:

- `d8 plugins update` stays **within the installed major**. Crossing majors is
  always an explicit decision: `--use-major N` or `--version X`.
- Installing a version that is already on disk just repoints the symlink - no
  download.
- Installing the active version says so and does nothing; `--force`
  re-downloads.
- No root access to `/opt/deckhouse/lib`? Plugins go to `~/.deckhouse-cli`
  automatically.

## Requirements

A plugin's contract may declare requirements, all validated **before** anything
is downloaded or switched:

- **Kubernetes / Deckhouse version** constraints (semver). A cluster whose
  Deckhouse version is not a release semver - e.g. a `dev` build - skips the
  Deckhouse check with a warning instead of failing.
- **Modules**: required-enabled (optionally with a version constraint),
  conditional (checked only if the module is enabled), any-of (at least one of
  a group must be enabled), and forbidden (a module that must *not* be enabled).
- **Other plugins**: mandatory dependencies, and conditional ones that are
  enforced only if that plugin is already installed.

```console
$ d8 plugins install package
...
Error: plugin requirements not satisfied      # e.g. requires plugin delivery-kit
```

Mandatory plugin dependencies are installed and upgraded automatically during
`install` / `update`. Cluster-side requirements (Kubernetes / Deckhouse /
modules) are only *verified* - d8 never changes the cluster for you.

- `--skip-cluster-checks` (or `D8_PLUGINS_SKIP_CLUSTER_CHECKS=1`) downgrades the
  cluster-side checks to a warning - useful when the cluster is unreachable or
  air-gapped. Plugin-to-plugin requirements are still enforced.

## Flags and environment variables

| Flag | Env | Purpose |
|---|---|---|
| `--kubeconfig`, `-k` / `--context` | `KUBECONFIG` | cluster identity (the Bearer token source) |
| `--plugins-dir` | `DECKHOUSE_CLI_PATH` | plugins directory |
| `--skip-cluster-checks` | `D8_PLUGINS_SKIP_CLUSTER_CHECKS=1` | skip cluster-side requirement checks |
| `--rpp-endpoint` | `D8_RPP_ENDPOINT` | proxy base URL; discovered from the cluster when empty |
| `--rpp-ca-file` | `D8_RPP_CA_FILE` | PEM CA bundle to verify the proxy TLS certificate |
| `--insecure-skip-tls-verify` | - | skip TLS verification of both the API server and the proxy (debugging only) |
| `--version X` *(install only)* | - | install an exact version; may be a pre-release |
| `--use-major N` *(install, update)* | - | cross to major `N`; by default operations stay within the installed major |
| `--force` *(install only)* | - | reinstall even if already current (re-pull and re-verify) |

The persistent flags above are shared by every `d8 plugins` subcommand; the
`--source*` family is hidden - see [Advanced](#advanced-hidden-flags).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `image or tag not found` (404) | that plugin - or that specific version - is not published in this cluster's registry | check with `d8 plugins versions <name>`; publishing is the plugin CI's job |
| `... unauthorized (401)` | no accepted Bearer token (a client-certificate kubeconfig is not enough) | use an OIDC-token kubeconfig (Kubeconfig Generator or `d8 login`) |
| `... forbidden (403)` | your identity may not download plugins | ask an admin to bind the ClusterRole `d8:registry-packages-proxy:cli-download`; a denial is cached for 30 seconds, so retry in half a minute |
| `... requirements not satisfied` | mandatory **plugin** dependencies are missing or version-incompatible | run `d8 plugins contract <name>`; on `install` deps auto-install, but at plugin *run* time install them manually as the hint says (`d8 plugins install <dep>`) |
| `... requires Kubernetes/Deckhouse/module ...` | a **cluster-side** requirement is unmet (a different message from the row above) | upgrade the cluster/module, or pass `--skip-cluster-checks` to bypass verification |
| `... upstream error (5xx)` | the proxy could not reach the backing registry | retry shortly, or check the `registry-packages-proxy` pods in `d8-cloud-instance-manager` |
| `endpoint discovery ... failed`, `x509:` to the API server | endpoint discovery goes through your kubeconfig's **API server** (not the proxy), which was unreachable or had an invalid certificate | confirm the API server is reachable with a valid cert. To get through meanwhile: `--insecure-skip-tls-verify` for a bad certificate, or `--rpp-endpoint https://registry-packages-proxy.<domain>` (`D8_RPP_ENDPOINT`) to skip discovery altogether |
| `cannot reach the cluster to ...` | the cluster is needed to verify requirements or select a version, but is unreachable | pass `--skip-cluster-checks` (`D8_PLUGINS_SKIP_CLUSTER_CHECKS=1`) |

The access model is shared with d8 self-update; see
[self-update.md - Troubleshooting](self-update.md#troubleshooting) for the
registry-packages-proxy side.

## Advanced (hidden flags)

These flags are hidden from `--help` and exist as a temporary escape hatch;
prefer the proxy flow above.

- `--source <registry-repo>` pulls plugins **directly from a registry
  repository, bypassing the cluster and the proxy**. It automatically enables
  `--skip-cluster-checks`, so cluster-side requirements are not verified.
  Credentials come from `--source-login` / `--source-password`, or `--license`
  (a shortcut for `--source-login=license-token`), or your
  `~/.docker/config.json` - in that order. `--tls-skip-verify` and `--insecure`
  relax TLS / allow HTTP for that registry.
