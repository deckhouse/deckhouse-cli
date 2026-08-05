# d8 Self-Update (`d8 dist`)

`d8` updates itself **through the cluster**. No registry credentials needed:

- Artifacts are served by the in-cluster **registry-packages-proxy**.
- You authenticate with your **ordinary kubeconfig** - the same identity you
  use for `kubectl`.
- The cluster administrator grants (and revokes) download permission with a
  regular RBAC binding.

**Contents:** [Getting started](#getting-started) ·
[Access](#how-access-works) · [Commands](#commands) ·
[Version store](#how-versions-are-stored) ·
[Switching & rollback](#switching-and-rollback) ·
[Flags & env](#flags-and-environment-variables) ·
[Troubleshooting](#troubleshooting)

> Plugin management (`d8 dist plugins`) uses the same access model and is
> covered in [plugins.md](plugins.md).

## Getting started

You need a kubeconfig that authenticates with a **Bearer token**, and an identity
that holds two permissions on the cluster. Access to master nodes is not needed.

1. Ask an administrator to grant your user or group access. They bind one
   ClusterRole and one read permission, both described in
   [Granting access to CLI downloads](/products/kubernetes-platform/documentation/v1/modules/registry-packages-proxy/#granting-access-to-cli-downloads).
   The same grant also covers `d8 plugins`.

1. Use the kubeconfig you already use for `kubectl`, as long as it carries a
   token. A client-certificate config - the `kubernetes-admin` one on a master
   node, for example - is rejected by the proxy. Without a token, get a personal
   kubeconfig from the Deckhouse console at `https://console.<publicDomain>`.

1. Check that it works:

   ```bash
   d8 cli check
   ```

   `up to date`, or a newer version being offered, means access is fine. On
   `403` the role is not bound yet - retry in half a minute, then ask the
   administrator.

`d8` picks up `KUBECONFIG` (or the default `~/.kube/config`) the same way
`kubectl` does. Point it at another file with `--kubeconfig`, or select a
context with `--context`.

## How access works

```
d8 dist update
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
> Get a personal OIDC kubeconfig from the Deckhouse console:
> `https://console.<publicDomain>`. Clusters without the console module serve
> the standalone generator at `https://kubeconfig.<publicDomain>` instead.

### Authorization

You need two permissions, and by default neither is bound to anyone - the
administrator grants them:

- Downloading: the ClusterRole `d8:registry-packages-proxy:cli-download`. The
  same role covers `d8 plugins`.
- Endpoint discovery: `get` on the `registry-packages-proxy` Ingress in
  `d8-cloud-instance-manager`. Without it, pass `--rpp-endpoint` by hand.

The proxy caches a denial for 30 seconds, so a `403` caught before the binding
existed clears in half a minute. See
[Granting access to CLI downloads](/products/kubernetes-platform/documentation/v1/modules/registry-packages-proxy/#granting-access-to-cli-downloads)
for the commands.

### Endpoint

- Discovered automatically from the cluster (the `registry-packages-proxy`
  Ingress).
- In closed environments, set it explicitly: `--rpp-endpoint` /
  `D8_RPP_ENDPOINT`.
- Private CA? Pass the bundle with `--rpp-ca-file`.

## Commands

| Command | What it does |
|---|---|
| `d8 dist status` | prints a distribution summary: the d8 version, installed plugins, what is outdated (local data only when the cluster is unreachable) |
| `d8 dist check` | reports whether a newer version is available |
| `d8 dist versions` (alias: `list`) | lists published versions, newest first |
| `d8 dist update [--version X]` | installs a version and switches to it |
| `d8 dist use <version>` | switches to a version; instant if it is already installed |

```console
$ d8 dist status
deckhouse-cli (d8)
  Version:  v0.13.1
  Latest:   v0.14.0  update available - run 'd8 dist update'

Plugins (1 installed):
  NAME    VERSION  LATEST  STATUS
  system  1.2.0    1.2.0   up to date

$ d8 dist check
A newer deckhouse-cli is available: v0.14.0 (current: v0.13.1). Run 'd8 dist update' to upgrade.

$ d8 dist versions
  v0.14.0  newer
* v0.13.1  current  installed
  v0.13.0  installed

$ d8 dist update
Updating deckhouse-cli to v0.14.0...
deckhouse-cli updated to v0.14.0.
Previous version v0.13.1 remains installed - switch back with 'd8 dist use v0.13.1'.
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
$ d8 dist use v0.13.1            # already installed: instant, no cluster access
Switched deckhouse-cli to v0.13.1 (installed locally).
Previous version v0.14.0 remains installed - switch back with 'd8 dist use v0.14.0'.

$ d8 dist use 0.13.0             # the "v" prefix is optional
$ d8 dist use v0.13.0            # repeated: "deckhouse-cli is already at v0.13.0."
```

- Rollback after an update: `d8 dist use <previous>` - the previous version
  stays installed.
- `d8 dist use <TAB>` completes the locally installed versions (enable shell
  completion with `d8 completion`).

## Flags and environment variables

| Flag | Env | Purpose |
|---|---|---|
| `--kubeconfig`, `-k` / `--context` | `KUBECONFIG` | cluster identity (the Bearer token source) |
| `--rpp-endpoint` | `D8_RPP_ENDPOINT` | proxy base URL; discovered from the cluster when empty |
| `--rpp-ca-file` | `D8_RPP_CA_FILE` | PEM CA bundle to verify the proxy TLS certificate |
| `--insecure-skip-tls-verify` | - | skip TLS verification of both the API server and the proxy (debugging only) |

`d8 cli` opens two TLS connections, each verified on its own: one to the
Kubernetes API server to find the proxy, one to the proxy to download.
`--insecure-skip-tls-verify` covers both.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `... unauthorized` (401) | no token in kubeconfig, or a client-certificate identity | use an OIDC kubeconfig from the Deckhouse console |
| `... forbidden` (403) | the `cli-download` role is not bound to you | ask the administrator for the ClusterRoleBinding |
| 403 right after the role was bound | the proxy caches a denial for 30 seconds | retry in half a minute |
| `x509: certificate signed by unknown authority` | the proxy endpoint uses a CA your system does not trust | pass `--rpp-ca-file <ca.pem>` |
| `endpoint discovery ... x509:` naming the API server host | the API server certificate is untrusted, expired or replaced by the ingress fallback | fix the cluster certificate, or pass `--insecure-skip-tls-verify` to get through meanwhile |
| `x509: ... doesn't contain any IP SANs` | you are connecting to a pod IP instead of the Ingress host | set `--rpp-endpoint https://registry-packages-proxy.<publicDomain>` |
| `deckhouse-cli is already up to date` | you run the latest version | use `--version X` to install an exact (older) one |
| `d8 dist use X` downloads although X was installed before | the local store was cleaned, or X was installed on another machine/user | it will download once and stay installed |
