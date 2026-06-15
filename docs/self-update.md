# d8 Self-Update (`d8 cli`)

`d8` updates itself **through the cluster**. No registry credentials needed:

- Artifacts are served by the in-cluster **registry-packages-proxy**.
- You authenticate with your **ordinary kubeconfig** - the same identity you
  use for `kubectl`.
- The cluster administrator grants (and revokes) download permission with a
  regular RBAC binding.

**Contents:** [Access](#how-access-works) · [Commands](#commands) ·
[Version store](#how-versions-are-stored) ·
[Switching & rollback](#switching-and-rollback) ·
[Flags & env](#flags-and-environment-variables) ·
[Troubleshooting](#troubleshooting)

> Plugin management (`d8 plugins`) uses the same access model and is covered
> in [plugins.md](plugins.md).

## How access works

```
d8 cli update
        │  Bearer token from your kubeconfig
        ▼
registry-packages-proxy.<publicDomain>     (found automatically via Ingress)
        │  TokenReview + SubjectAccessReview (kube-rbac-proxy)
        ▼
cluster registry (credentials live only inside the cluster)
```

### Authentication

- The proxy accepts the **Bearer token** from your kubeconfig.
- Client certificates do **not** work (for example, the root
  `kubernetes-admin` config on master nodes).

> [!TIP]
> Get a personal OIDC kubeconfig from your cluster's Kubeconfig Generator:
> `https://kubeconfig.<publicDomain>`.

### Authorization

Download permission is the ClusterRole
`d8:registry-packages-proxy:cli-download`. By default it is bound to
**nobody** - the administrator decides who may download:

```bash
kubectl create clusterrolebinding d8-cli-download \
  --clusterrole=d8:registry-packages-proxy:cli-download \
  --group=<your-operators-group>   # or --user=... / --serviceaccount=...
```

### Endpoint

- Discovered automatically from the cluster (the `registry-packages-proxy`
  Ingress).
- In closed environments, set it explicitly: `--rpp-endpoint` /
  `D8_RPP_ENDPOINT`.
- Private CA? Pass the bundle with `--rpp-ca-file`.

## Commands

| Command | What it does |
|---|---|
| `d8 cli check` | reports whether a newer version is available |
| `d8 cli versions` (alias: `list`) | lists published versions, newest first |
| `d8 cli update [--version X]` | installs a version and switches to it |
| `d8 cli use <version>` | switches to a version; instant if it is already installed |

```console
$ d8 cli check
A newer deckhouse-cli is available: v0.14.0 (current: v0.13.1). Run 'd8 cli update' to upgrade.

$ d8 cli versions
  v0.14.0  newer
* v0.13.1  current  installed
  v0.13.0  installed

$ d8 cli update
Updating deckhouse-cli to v0.14.0...
deckhouse-cli updated to v0.14.0.
Previous version v0.13.1 remains installed - switch back with 'd8 cli use v0.13.1'.
```

## How versions are stored

Installed versions live in a per-user store. A symlink selects the active one:

```
/opt/deckhouse/bin/d8 -> ~/.deckhouse-cli/cli/current -> versions/v0.14.0/d8
```

What this gives you:

- **Switching is instant** - just a symlink repoint. No download, no network,
  no `sudo`.
- **Old versions stay installed** - rollback is one command.
- **Migration is automatic** - the first `update` or `use` converts a
  plain-file installation to this layout; the original binary is kept with a
  `.old` suffix.

> [!NOTE]
> Every downloaded binary is verified (a smoke run of `--version`) **before**
> it becomes active. A corrupt or wrong-platform artifact never replaces a
> working d8.

## Switching and rollback

```console
$ d8 cli use v0.13.1            # already installed: instant, no cluster access
Switched deckhouse-cli to v0.13.1 (installed locally).
Previous version v0.14.0 remains installed - switch back with 'd8 cli use v0.14.0'.

$ d8 cli use 0.13.0             # the "v" prefix is optional
$ d8 cli use v0.13.0            # repeated: "deckhouse-cli is already at v0.13.0."
```

- Rollback after an update: `d8 cli use <previous>` - the previous version
  stays installed.
- `d8 cli use <TAB>` completes the locally installed versions (enable shell
  completion with `d8 completion`).

## Flags and environment variables

| Flag | Env | Purpose |
|---|---|---|
| `--kubeconfig`, `-k` / `--context` | `KUBECONFIG` | cluster identity (the Bearer token source) |
| `--rpp-endpoint` | `D8_RPP_ENDPOINT` | proxy base URL; discovered from the cluster when empty |
| `--rpp-ca-file` | `D8_RPP_CA_FILE` | PEM CA bundle to verify the proxy TLS certificate |
| `--rpp-insecure-skip-tls-verify` | - | skip proxy TLS verification (debugging only) |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `... unauthorized` (401) | no token in kubeconfig, or a client-certificate identity | use an OIDC kubeconfig from the Kubeconfig Generator |
| `... forbidden` (403) | the `cli-download` role is not bound to you | ask the administrator for the ClusterRoleBinding |
| 403 right after the role was bound | the proxy caches authorization for ~5 min per token | retry with a fresh token or wait 5 minutes |
| `x509: certificate signed by unknown authority` | the proxy endpoint uses a CA your system does not trust | pass `--rpp-ca-file <ca.pem>` |
| `x509: ... doesn't contain any IP SANs` | you are connecting to a pod IP instead of the Ingress host | set `--rpp-endpoint https://registry-packages-proxy.<publicDomain>` |
| `deckhouse-cli is already up to date` | you run the latest version | use `--version X` to install an exact (older) one |
| `d8 cli use X` downloads although X was installed before | the local store was cleaned, or X was installed on another machine/user | it will download once and stay installed |
