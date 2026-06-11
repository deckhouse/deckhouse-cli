# d8 Plugins (`d8 plugins`)

Plugins are versioned binaries distributed through the cluster registry.
`d8` installs, updates, and removes them for you.

**Contents:** [Source](#plugin-source) · [Commands](#commands) ·
[Versions & majors](#versions-majors-and-switching) ·
[Requirements](#requirements) · [Auto-update](#automatic-plugin-updates) ·
[Flags & env](#flags-and-environment-variables) ·
[Troubleshooting](#troubleshooting)

> [!NOTE]
> The `d8 plugins` command group is hidden from the root `--help` while the
> plugin ecosystem rolls out. The commands below are fully functional.

## Plugin source

Plugins are pulled from the in-cluster **registry-packages-proxy**, the same
channel as d8 self-update. There is no direct-registry path: every `d8 plugins`
command reaches the registry through the proxy, so a reachable cluster is
required. The access model:

- Authentication: the **Bearer token** from your kubeconfig (client
  certificates do not work).
- Authorization: the ClusterRole `d8:registry-packages-proxy:cli-download`,
  bound by the cluster administrator.
- Endpoint: discovered automatically; override with `--rpp-endpoint` /
  `D8_RPP_ENDPOINT`, pass a private CA with `--rpp-ca-file`.

See [self-update.md - How access works](self-update.md#how-access-works) for
the full picture (RBAC binding example, OIDC kubeconfig, endpoint discovery).

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

A plugin's contract may declare requirements:

- other plugins;
- Kubernetes / Deckhouse versions;
- enabled modules.

They are validated **before** anything is downloaded or switched:

```console
$ d8 plugins install package
...
Error: plugin requirements not satisfied      # e.g. requires plugin delivery-kit
```

- `--resolve-plugins-conflicts` - install missing required plugins
  automatically.
- `--skip-cluster-checks` (or `D8_PLUGINS_SKIP_CLUSTER_CHECKS=1`) - skip
  cluster-side checks, e.g. in air-gapped scenarios.

## Automatic plugin updates

Installed plugins are kept fresh automatically:

- d8 updates them in the background to the newest cluster-compatible version
  **within their major**.
- At most once per 6 hours; never blocks your commands.

Opting out:

- `D8_DISABLE_PLUGIN_AUTO_UPDATE=1` - disable the background plugin auto-update;
- `D8_DISABLE_UPDATE_NOTIFY=1` - independent switch for the d8 self-update notice
  (does not affect plugin auto-update).

## Flags and environment variables

| Flag | Env | Purpose |
|---|---|---|
| `--kubeconfig`, `-k` / `--context` | `KUBECONFIG` | cluster identity (the Bearer token source) |
| `--plugins-dir` | `DECKHOUSE_CLI_PATH` | plugins directory |
| `--skip-cluster-checks` | `D8_PLUGINS_SKIP_CLUSTER_CHECKS=1` | skip cluster-side requirement checks |
| `--rpp-endpoint` | `D8_RPP_ENDPOINT` | proxy base URL; discovered from the cluster when empty |
| `--rpp-ca-file` | `D8_RPP_CA_FILE` | PEM CA bundle to verify the proxy TLS certificate |
| `--rpp-insecure-skip-tls-verify` | - | skip proxy TLS verification (debugging only) |
| - | `D8_DISABLE_PLUGIN_AUTO_UPDATE=1` | disable background plugin auto-update only |
| - | `D8_DISABLE_UPDATE_NOTIFY=1` | disable the d8 self-update notice and its refresh (not plugin updates) |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `image or tag not found` (404) on a plugin | the plugin is not published in this cluster's registry | check with `d8 plugins versions <name>`; publishing is the plugin CI's job |
| `plugin requirements not satisfied` | the contract requires other plugins or cluster versions/modules | see `d8 plugins contract <name>`; `--resolve-plugins-conflicts` for plugin deps |
| 401 / 403 / `x509: ...` reaching the proxy | access or TLS issue with the registry-packages-proxy | see [self-update.md - Troubleshooting](self-update.md#troubleshooting) - the access model is shared |
