# `d8 dist` self-update

This document specifies how `d8 dist` updates the d8 binary through the cluster: what a working setup requires, how the client reaches the registry-packages-proxy, what the proxy serves, how versions are selected, how the local version store is laid out, and what every command does. It describes the implementation as of 2026-10-01, deckhouse-cli at this revision and the registry-packages-proxy of deckhouse `main`, and every rule names the code that implements it. Where the implementation breaks the contract, §14 lists the deviation.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119. Rules for the publisher apply to anything that puts `deckhouse-cli` images into a registry, the release pipeline or `d8 mirror push`; rules for the client describe what d8 does, rules for the proxy what the registry-packages-proxy does.

Plugins (`d8 dist plugins`) reach the proxy the same way and are described in [plugins.md](plugins.md). How `d8 mirror` carries the binary into an air-gapped registry is described in [mirror-bundle-layout.md](mirror-bundle-layout.md) §6.7 and §8.10.

- [1. Terms](#1-terms)
- [2. Invariants](#2-invariants)
- [3. Requirements](#3-requirements)
- [4. Reaching the proxy](#4-reaching-the-proxy)
- [5. The registry-packages-proxy](#5-the-registry-packages-proxy)
- [6. Publishing contract](#6-publishing-contract)
- [7. Version selection](#7-version-selection)
- [8. Version store](#8-version-store)
- [9. Switching](#9-switching)
- [10. Commands](#10-commands)
- [11. Flags and environment variables](#11-flags-and-environment-variables)
- [12. Errors and troubleshooting](#12-errors-and-troubleshooting)
- [13. Example](#13-example)
- [14. Known deviations](#14-known-deviations)

## 1. Terms

| Term | Meaning |
|---|---|
| Proxy | The registry-packages-proxy: the Deckhouse module `registry-packages-proxy` in the namespace `d8-cloud-instance-manager`, which serves artifacts of the cluster registry to clients authorized by Kubernetes RBAC (§5). |
| Endpoint | The base URL that d8 sends proxy requests to (§4.2). |
| Tag | A tag of the `deckhouse-cli` repository as the proxy lists it. |
| Version | A tag or an argument that parses as a semantic version (§7.1). A stable version has no pre-release part. |
| Platform | `<GOOS>-<GOARCH>` of the running binary, for example `linux-amd64` (`currentPlatform` in [rpp/platform.go](../internal/rpp/platform.go)). |
| Running version | The version compiled into the running binary (`internal/version.Version`, `dev` when the build sets none), as `d8 --version` prints it. |
| S | The version store, `$HOME/.deckhouse-cli/cli` (§8.1). |
| Store entry | `S/versions/<tag>/d8`, one stored version. |
| X | The running executable with all symlinks resolved (`CurrentExecutable` in [selfupdate/update.go](../internal/selfupdate/update.go)). |
| Store-managed, plain-file | An install is store-managed when X lies inside S (`Store.Contains`), and plain-file otherwise. |
| Active version | For a store-managed install the tag `S/current` points at, otherwise the running version (`activeVersionTag` in [dist/cmd/updater.go](../internal/dist/cmd/updater.go)). |
| PATH entry | The file a user runs as `d8`, for example `/opt/deckhouse/bin/d8`. In a plain-file install it is X or a symlink to X; migration (§9.1) replaces X with a symlink to `S/current`. |

## 2. Invariants

- Source. d8 gets the list of versions and the binaries only from the proxy, over HTTPS, authenticated with the identity of the current kubeconfig. It holds no registry credentials and never contacts a registry itself (§4).
- Selection. The active binary is reached through the chain PATH entry → `S/current` → `S/versions/<tag>/d8`. d8 switches versions only by atomically replacing `S/current`, writes every store entry once and deletes none, and rewrites the PATH entry only when it migrates a plain-file install (§8, §9).
- Verification. A binary becomes active only after `<binary> --version`, run from its final store path, exited 0 within 30 s (§9.1).
- Locality. A `use` that needs no download and shell completion work without a kubeconfig and without network (§10.5, §10.6). Every other operation needs both.

## 3. Requirements

Self-update works when all of the following hold:

1. The kubeconfig (§4.1) authenticates with a bearer token that the cluster accepts: a static token, a token file, an exec plugin such as `d8 login get-token`, or an OIDC auth provider. A personal OIDC kubeconfig is issued by the Deckhouse console at `https://console.<publicDomain>`, or by the kubeconfig generator at `https://kubeconfig.<publicDomain>` in clusters without the console. The proxy rejects kubeconfig client certificates (§5.2), so the `kubernetes-admin` kubeconfig of a master node does not work.
2. The identity may `get` the subresource `deployments/cli-binary` of `registry-packages-proxy` in `d8-cloud-instance-manager`. The ClusterRole `d8:registry-packages-proxy:cli-download` grants exactly that, and it is bound to nobody by default. The same permission covers `d8 dist plugins`.
3. Either the identity may `get` the Ingress `registry-packages-proxy` in `d8-cloud-instance-manager`, which `cli-download` does not include, or the endpoint is given explicitly (§4.2).
4. The endpoint is reachable and presents a certificate that the client trusts (§4.3).
5. The cluster registry holds `deckhouse-cli` images that satisfy §6.
6. The client runs on Linux or macOS, since `update` and `use` refuse to switch on Windows (§9.1). The user can write to S and, for the first switch of a plain-file install, to the directory of X.

The grants of items 2 and 3, as the module documentation gives them in [Granting access to Deckhouse CLI downloads](/products/kubernetes-platform/documentation/v1/modules/registry-packages-proxy/#granting-access-to-deckhouse-cli-downloads); `--user=<name>` or `--serviceaccount=<namespace>:<name>` can replace `--group`:

```shell
d8 k create clusterrolebinding d8-cli-download --clusterrole=d8:registry-packages-proxy:cli-download --group=<group>
d8 k -n d8-cloud-instance-manager create role d8-cli-ingress --verb=get --resource=ingresses --resource-name=registry-packages-proxy
d8 k -n d8-cloud-instance-manager create rolebinding d8-cli-ingress --role=d8-cli-ingress --group=<group>
```

A grant takes effect within 30 s and a revocation within 5 min (§5.2). To check a grant without the user's credentials:

```shell
d8 k auth can-i get deployments/registry-packages-proxy -n d8-cloud-instance-manager --subresource=cli-binary --as=<user>
d8 k auth can-i get ingresses/registry-packages-proxy -n d8-cloud-instance-manager --as=<user>
```

`d8 dist check` exits 0 once items 1–4 hold and the registry lists a stable version (§10.2). It downloads nothing, so item 5 is first exercised by `d8 dist update`.

## 4. Reaching the proxy

### 4.1. Identity

- The kubeconfig is `--kubeconfig` (`-k`), by default `$KUBECONFIG`, or `~/.kube/config` when that is unset (`DefaultKubeconfigPath` in [utilk8s/clientset.go](../internal/utilk8s/clientset.go)). A value with several paths separated by `:` is merged as kubectl does, and `--context` selects a context (`SetupK8sClientSet`).
- d8 builds the Kubernetes client and the proxy client from the same configuration (`newUpdater` in [dist/cmd/updater.go](../internal/dist/cmd/updater.go)). The proxy client keeps its credentials, whether a bearer token, a token file, an exec plugin, an auth provider or a client certificate, and sends them with every request (`buildHTTPClient` in [rpp/transport.go](../internal/rpp/transport.go)). The proxy accepts only tokens (§5.2).
- The kubeconfig is needed even with an explicit endpoint, because the proxy authenticates every request with it. Only a `use` that needs no download (§10.5) and shell completion (§10.6) run without one.

### 4.2. Endpoint

The endpoint is chosen in this order (`NewClusterClient` in [rpp/connect.go](../internal/rpp/connect.go)):

1. `--rpp-endpoint`, by default `$D8_RPP_ENDPOINT`. The value MUST parse as a URL with the scheme `https` and a host (`validateBaseURL` in [rpp/client.go](../internal/rpp/client.go)). A trailing `/` is removed, and a path is kept as a prefix of the routes.
2. Otherwise d8 discovers it through the API server of the kubeconfig (`chooseDiscoveredEndpoint` in [rpp/endpoint.go](../internal/rpp/endpoint.go)):
   - Ingress lookup. d8 reads the Ingress `registry-packages-proxy` in `d8-cloud-instance-manager`. The host of the first rule that has one gives `https://<host>`.
   - Pod lookup. Only when the API answers that the Ingress does not exist, or the Ingress has no rule with a host, d8 lists the pods labelled `app=registry-packages-proxy` in that namespace, which needs `list` on pods there, and takes the first one that is `Running`, not being deleted, has a pod IP and has the condition `Ready=True`: `https://<pod IP>:4219`. There is no failover between pods.
   - Failure. Any other failure of the Ingress lookup, a `403` included, and any failure of the pod lookup stop the command with an endpoint discovery error (§4.5). The pod lookup is not tried after a failed Ingress lookup.

The endpoint and how it was found are logged at debug level only (`LOG_LEVEL=debug`). A pod endpoint is the address of a master node, since the proxy runs in the host network; it is reachable only from the cluster network, and its certificate does not cover the IP (§5.1), so it verifies only with `--insecure-skip-tls-verify`. d8 reads no Gateway API objects: a cluster that publishes the proxy only through an HTTPRoute (§5.1) ends up in the pod lookup, so the endpoint MUST be given explicitly there.

### 4.3. TLS

d8 opens two TLS connections and verifies each on its own ([rpp/flags/flags.go](../internal/rpp/flags/flags.go)):

| Connection | Used for | Verified against | With `--insecure-skip-tls-verify` |
|---|---|---|---|
| API server | discovery (§4.2) | the CA of the kubeconfig | not verified; the kubeconfig CA is dropped (`WithInsecureSkipTLSVerify` in [utilk8s/clientset.go](../internal/utilk8s/clientset.go)) |
| Proxy | every proxy request | the system roots, plus every certificate of `--rpp-ca-file` (by default `$D8_RPP_CA_FILE`) | not verified |

- The CA of the kubeconfig never applies to the proxy, whose certificate comes from another authority (`buildHTTPClient`).
- A CA file MUST contain at least one PEM certificate, otherwise the command fails with `invalid CA bundle: no certificates parsed from CA data`. When the system roots cannot be loaded, the client is not built.
- `--insecure-skip-tls-verify` together with a CA file, from the flag or from the environment, is rejected with `unsupported client configuration: insecure TLS verification and a CA bundle are mutually exclusive`.
- HTTP proxies apply to proxy traffic as to API traffic: the `proxy-url` of the kubeconfig, otherwise `HTTPS_PROXY` and `NO_PROXY`.

### 4.4. Requests

- Every request is made once, without retries (`Client.do` in [rpp/client.go](../internal/rpp/client.go)).
- Redirects are not followed, so that the credentials never reach another host, and a `3xx` is an error (`newClient`).
- The TLS handshake MUST complete within 10 s and the response headers MUST arrive within 30 s. The body has no time limit ([rpp/transport.go](../internal/rpp/transport.go)).
- Listing: `GET <endpoint>/v1/images/deckhouse-cli/tags` with `Accept: application/json`. The body, read up to 4 MiB, MUST be a JSON object whose member `tags` is an array of strings; other members are ignored (`ListTags`).
- Download: `GET <endpoint>/v1/images/deckhouse-cli/images/<tag>?platform=<platform>`, where `<tag>` MUST match `^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$` (`validateTag` in [rpp/image.go](../internal/rpp/image.go)). The body MUST be a gzip-compressed tar. d8 takes the first regular-file entry whose name, without a leading `./`, has the base name `d8`, and writes it with mode `0755` (`ExtractFileToPath` in [rpp/extract.go](../internal/rpp/extract.go)). d8 rejects a file longer than 512 MiB and reads at most 1 GiB of decompressed tar in all; a tar without such an entry fails with `file not found in image: "d8"`.
- d8 verifies no digest and no signature of the download, because the proxy reports only the manifest digest, not a hash of the body (`PullImage`). Integrity rests on the TLS connection to the proxy and on the smoke test (§9.1).
- The client also implements `GET /v1/images/<image>/manifests/<ref>`, for plugins; self-update does not use it.

### 4.5. Status mapping

`Client.do` maps the response status to an error, and the `d8 dist` commands turn the recognised errors into a diagnostic (`Diagnose` in [dist/cmd/errdetect/diagnose.go](../internal/dist/cmd/errdetect/diagnose.go)), printed to stderr as `error: <category>`, the error chain and the suggested fixes.

| Response or failure | Error | Diagnostic category |
|---|---|---|
| `2xx` | none | |
| `401` | `unauthorized` | `registry-packages-proxy: unauthorized (401)` |
| `403` | `forbidden` | `registry-packages-proxy: forbidden (403)` |
| `404` | `image or tag not found` | `registry-packages-proxy: version not found (404)` |
| `500` and above | `registry proxy upstream error (status <code>): <body>` | `registry-packages-proxy: upstream error (5xx)` |
| any other status, `3xx` included | `unexpected status <code>: <body>` | none |
| discovery failed (§4.2) | `registry-packages-proxy endpoint discovery failed: <cause>` | `registry-packages-proxy: endpoint discovery via the Kubernetes API failed` |

- `<body>` is at most 256 bytes of the response body with control characters replaced by spaces. The bodies of `401`, `403` and `404` are dropped, so the user name that the proxy puts into a `403` is not shown.
- Any error without a diagnostic is printed as `Error executing command: <error>`. Every failing command exits 1.

## 5. The registry-packages-proxy

This section describes the server side that d8 relies on, as implemented on deckhouse `main`.

### 5.1. Exposure

The module runs the proxy on every master node in the host network ([deployment.yaml](https://github.com/deckhouse/deckhouse/blob/main/modules/039-registry-packages-proxy/templates/deployment.yaml)):

| Access | Address | Certificate | Routes |
|---|---|---|---|
| Master node | `https://<node IP>:4219` | generated by kube-rbac-proxy at start, self-signed for the node host name, no IP SANs | all |
| Ingress `registry-packages-proxy` | `https://registry-packages-proxy.<publicDomain>` | per the module's HTTPS settings | `/v1/images/` only |
| HTTPRoute `registry-packages-proxy` | the same host | per the module's HTTPS settings | `/v1/images/` only |

- The Ingress exists only when Ingress is enabled for the module, which is the default, `publicDomainTemplate` is set and the cluster is bootstrapped ([ingress.yaml](https://github.com/deckhouse/deckhouse/blob/main/modules/039-registry-packages-proxy/templates/ingress.yaml)). The HTTPRoute exists when a Gateway is available to the module ([httproute.yaml](https://github.com/deckhouse/deckhouse/blob/main/modules/039-registry-packages-proxy/templates/httproute.yaml)). The two are independent.
- kube-rbac-proxy generates its certificate for `os.Hostname()` when it is given none, which is the case here.

### 5.2. Authentication and authorization

Requests pass kube-rbac-proxy v0.11.0 with the Deckhouse patches ([kube-rbac-proxy](https://github.com/deckhouse/deckhouse/tree/main/modules/000-common/images/kube-rbac-proxy)) before they reach the proxy:

- Authentication. A bearer token is checked with a TokenReview. A client certificate is accepted only when the CA of the ConfigMap `kube-rbac-proxy-ca.crt` signed it; that is the internal kube-rbac-proxy CA of Deckhouse, not the cluster CA that signs kubeconfig certificates such as `kubernetes-admin`. A request without an accepted credential gets `401` with the body `Unauthorized`.
- Authorization. A SubjectAccessReview for `get` on `apps/deployments/cli-binary` named `registry-packages-proxy` in `d8-cloud-instance-manager`. A denial is `403` with the body `Forbidden (user=<name>, verb=get, resource=deployments, subresource=cli-binary)`.
- Caching, as set in kube-rbac-proxy v0.11.0 (`pkg/authn/delegating.go`, `pkg/authz/auth.go`): token reviews are cached for 2 min, allowed decisions for 5 min and denied decisions for 30 s. A new grant therefore takes effect within 30 s, and a revoked one keeps working for up to 5 min. With `--stale-cache-interval=5m`, an identity that was allowed during the last 5 min stays allowed while the API server is unreachable.

### 5.3. Routes

The routes are under `/v1/images/<image>/`, where `<image>` is `deckhouse-cli` or `deckhouse-cli/plugins/<name>` with a single-segment `<name>`; any other path is `404` (`CLIHandler` in [proxy.go](https://github.com/deckhouse/deckhouse/blob/main/go_lib/registry-packages-proxy/proxy/proxy.go)):

| Route | Response |
|---|---|
| `GET tags` | `200` with `{"name": "<image>", "tags": [...]}` holding every tag of the repository, unfiltered; `404` when no candidate repository exists (§5.4) |
| `GET` or `HEAD images/<version>[?platform=<os>-<arch>]` | `200` with `Content-Type: application/x-gzip`, `Content-Disposition: attachment; filename="<last image segment>-<version>.tar.gz"`, and `ETag` and `Docker-Content-Digest` set to the resolved manifest digest; `404` when the tag or the platform does not exist; `400` when the parameter is not `<os>-<arch>` |
| `GET manifests/<ref>` | the raw manifest with its media type |

An upstream failure is `502`, and `500` means that the proxy has no registry configuration.

### 5.4. Repository lookup

The proxy reads the cluster repository C from the Secret `d8-system/deckhouse-registry` and tries, in this order (`cliRepoCandidates` in [cli_repo.go](https://github.com/deckhouse/deckhouse/blob/main/go_lib/registry-packages-proxy/proxy/cli_repo.go)):

1. `C/deckhouse-cli`, where `d8 mirror push` to C puts the CLI when C does not end with an edition;
2. `<C without its last segment>/deckhouse-cli`, only when the last segment of C is `ce`, `be`, `se`, `se-plus`, `ee` or `fe` and the host and at least one path segment remain. The official registry publishes the CLI there, once for all editions, and `d8 mirror push` puts it there for a target that ends with an edition ([mirror-bundle-layout.md](mirror-bundle-layout.md#82-edition-split) §8.2).

`cse` is not in the list (§14.12). A candidate that answers `404`, `401` or `403`, or fails, is skipped. When all fail, the first error that is not a `404` is reported, as `502`, and otherwise `404`. The candidate that answered last is tried first on later requests. Both candidates are read with the credentials of the same Secret.

### 5.5. Platform and body

- With `platform`, a tag that is an image index resolves to the first child whose `os` and `architecture` equal the parameter; variants are not compared, and a missing child is `404`. A tag that is a single image is served whatever its platform. Without `platform`, an index resolves to `linux/amd64`. d8 always sends `platform` (§4.4).
- The body is the last layer of the resolved image, compressed as stored in the registry (`GetPackage`, `selectImageLayer` in [default_client.go](https://github.com/deckhouse/deckhouse/blob/main/go_lib/registry-packages-proxy/registry/default_client.go)). The module documentation describes the body as the image with flattened layers, but layers are flattened only for package icons, not on these routes.

## 6. Publishing contract

A registry serves self-update to a client on platform P when:

1. the repository `deckhouse-cli` exists at a root that the proxy tries (§5.4);
2. every release is tagged with the version its binary prints in `d8 --version`, spelled `vX.Y.Z`, or `vX.Y.Z-<pre-release>` for a pre-release. Other tags MAY exist and are ignored when they are not versions (§7.1);
3. every version tag is an image index with a child whose `os` and `architecture` match P, or a single image built for P;
4. the last layer of that image is a gzip-compressed tar holding a regular file with the base name `d8`, at most 512 MiB long, and the tar decompresses to at most 1 GiB up to the end of that file (§4.4);
5. that file runs on P, and `d8 --version` exits 0 within 30 s (§9.1);
6. a published tag is never moved to another binary, since a client keeps the binary it stored under that tag (§8.3).

The release images are multi-platform indexes under plain version tags (`rppSource` in [selfupdate/rpp_source.go](../internal/selfupdate/rpp_source.go)). `d8 mirror pull` copies one such index, with all its children, into `deckhouse-cli.tar`, and `d8 mirror push` places it at a root of §5.4 ([mirror-bundle-layout.md](mirror-bundle-layout.md#67-deckhouse-clitar) §6.7, §8.10), except for `cse` targets (§14.12).

## 7. Version selection

### 7.1. Parsing and order

- Tags and arguments are parsed with `semver.NewVersion` of `github.com/Masterminds/semver/v3`. The `v` prefix is optional and missing minor or patch numbers are 0, so `0.14`, `v0.14` and `v0.14.0` are one version. Strings that do not parse, such as `latest` or `sha256-<hex>.att`, are not versions and are skipped everywhere (`Updater.Versions`, `maxSemver` in [selfupdate/update.go](../internal/selfupdate/update.go)).
- Versions compare by SemVer precedence, so two spellings of one version are equal.
- Latest is the highest stable version, reported in its published spelling (`LatestVersion`). Pre-releases are installed only when named.
- A version is newer when it is greater than the reference version (§7.2). A reference that is not a version, such as `dev`, is older than every release.

### 7.2. Reference version

| Command | Compares against |
|---|---|
| `check`, `update` | the running version |
| `status`, `versions` | the active version |
| `use` | the active version for "already at", then the stored versions and the running version for an offline switch |

The running and the active version differ only when a store entry does not hold the version of its tag (§14.6, §14.11).

### 7.3. Requested versions

`update --version X`, and `use X` when X is not stored, download the tag X exactly as typed (§10.4, §10.5), so X MUST be spelled as the published tag: `0.14.0` does not find `v0.14.0` (§14.1). X MUST parse as a version, otherwise the switch fails with `tag "<X>" is not a semver version`. X need not appear in the listing, since d8 asks the proxy for it directly, and downgrades and pre-releases are allowed. A downloaded version is stored under the spelling it was fetched with.

## 8. Version store

### 8.1. Location

S is `.deckhouse-cli/cli` in the home directory of the process, which is `$HOME` on Linux and macOS (`NewStore` in [selfupdate/store.go](../internal/selfupdate/store.go)). The store belongs to a `$HOME`, not to a machine: `sudo`, whose default `env_reset` sets `HOME` to the target user's home, works on root's store. When the home directory cannot be determined, `update` and `use` fail with `version store is unavailable`, and the other commands run without store data.

### 8.2. Entries

| Path | Kind | Written by | Content |
|---|---|---|---|
| `S/versions/<tag>/d8` | regular file, mode `0755` | a download, or the seeding of a plain-file install (§9.1, steps 4 and 5) | the binary of `<tag>` |
| `S/versions/<tag>/d8.staged` | regular file | that write, while it runs | removed when the write ends |
| `S/current` | symlink | every switch, through `S/current.staged` and a rename | `versions/<tag>/d8`, relative |
| `S/install.lock` | empty file | `update` and `use`, while they switch | §8.4 |
| `X.old` | regular file | migration | the plain file that X was |
| X | symlink | migration | `S/current`, absolute |

Directories are created with mode `0755`.

### 8.3. Rules

- A version is stored when `S/versions/<tag>/d8` is a regular file, symlinks followed, and `<tag>` is a version (`Store.List`). Other names, other files and empty directories under `S/versions` are ignored.
- d8 writes a store entry once: a stored version is never downloaded or copied again (`install`). d8 deletes no entry either, so the store grows by one full binary per stored version, and an entry that fails its smoke test stays until it is deleted by hand (§14.7).
- A new entry is staged as `d8.staged`, made executable, smoke-tested and renamed into place, so a partial or failing binary never appears under its final name. A failure leaves the empty `<tag>` directory behind.
- `use` resolves its argument against the store by version, not by spelling (`Store.Resolve`).
- The active tag is the name of the parent directory of the `current` target, provided that name is a version (`CurrentTag`). Neither the existence nor the content of the target is checked.
- An install is store-managed when X lies under S with the symlinks of S resolved (`Store.Contains`). Only then does `current` define the active version.

### 8.4. Lock

`update` and `use` hold `S/install.lock` for the whole switch (`acquireLock` in [selfupdate/update.go](../internal/selfupdate/update.go), `Acquire` in [lockfile/lockfile.go](../internal/lockfile/lockfile.go)):

- The lock is an empty file created with `O_EXCL`. While it exists, another switch fails at once with `an update is already in progress (lock file <path> exists)`; there is no waiting.
- A lock file older than 1 h counts as left behind by a killed process: it is reclaimed with the warning `reclaiming a stale self-update lock`, and the switch proceeds.
- `status`, `check`, `versions` and completion take no lock. The lock covers one store: it serializes neither the stores of different homes nor writers of the PATH entry other than d8 (§14.11).

## 9. Switching

### 9.1. Procedure

`SwitchTo` in [selfupdate/update.go](../internal/selfupdate/update.go) makes `<tag>` the active version. `update`, and `use` with a download, pass it a fetch function; `use` of a stored or of the running version passes none.

1. On Windows, fail with `self-update is not supported on Windows; download the new d8 binary manually`.
2. Fail when S is unavailable. Create S and acquire the lock (§8.4).
3. Decide whether the install is store-managed (§8.3).
4. For a plain-file install, seed the store with X under the running version, when that is a version and is not stored yet, through the same staged write and smoke test as a download (`retain`). A failure is logged at debug level and ignored, so a `dev` build is never stored.
5. When `<tag>` is not stored, fail with `version <tag> is not in the local store` if there is no fetch function, and otherwise download it into a new entry (§4.4, §8.3).
6. Run `S/versions/<tag>/d8 --version`. It MUST exit 0 within 30 s, otherwise the switch fails with `new binary failed its --version smoke test: <error> (output: <first 200 characters>)`. The test runs for stored entries too, so a new entry is tested twice. Only the exit status counts (§14.6).
7. Record the tag that `current` points at as the previous tag.
8. Replace `current`: create `current.staged` pointing at `versions/<tag>/d8` and rename it over `current`.
9. For a plain-file install, migrate X: rename X to `X.old`, replacing an existing `X.old`, then create X as a symlink to `S/current`. If the rename fails with a permission error, the switch fails with the diagnostic `updating d8 needs write access to <directory of X>`; if the symlink cannot be created, `X.old` is renamed back. After a failed migration `current` is restored to the previous tag, or removed when there was none.
10. Release the lock.

Migration runs whenever X lies outside S: after another tool replaced the symlink with a plain file, the next switch migrates again (§14.9), and X inside the store of another home is migrated as well (§14.10).

### 9.2. Failures

| Failing step | Store | `current` | PATH entry |
|---|---|---|---|
| 1, 2 | S may have been created | unchanged | unchanged |
| 5, 6 | the entry seeded in step 4 stays; an empty `versions/<tag>/` may stay | unchanged | unchanged |
| 8 | all entries stay | unchanged | unchanged |
| 9 | all entries stay | restored | unchanged, or restored from `X.old` |

A failed `update` of a plain-file install thus leaves the running version stored without migrating it. With a read-only PATH directory, `v0.13.1` and `v0.14.0` stayed in the store and `current` was removed.

### 9.3. Messages

After a successful switch the command prints (`printSwitchNotes` in [dist/cmd/use.go](../internal/dist/cmd/use.go)):

- when step 9 ran: `The d8 binary in PATH is now a symlink into the version store; the previous binary is kept with a ".old" suffix.`
- when step 7 found a previous tag: `Previous version <previous> remains installed - switch back with 'd8 dist use <previous>'.`

A first migration finds no previous tag and prints no such line, although step 4 stored the replaced version. A switch to the active version names that version as its own predecessor (§14.8).

## 10. Commands

The commands live under `d8 dist` ([dist/cmd](../internal/dist/cmd/)). `d8 dist` alone prints its help, and an extra argument is an error. Regular output goes to stdout and diagnostics to stderr. Output colours are dropped when stdout is not a terminal or `NO_COLOR` is set.

### 10.1. `d8 dist status`

`collectSummary` in [dist/cmd/status.go](../internal/dist/cmd/status.go) prints the local state first and then asks the cluster:

1. `Version:` is the active version. Plugins are read from the plugins root that holds an install, the one of `DECKHOUSE_CLI_PATH` (by default `/opt/deckhouse/lib/deckhouse-cli`) or `~/.deckhouse-cli` ([plugins.md](plugins.md)); none gives `Plugins: none installed`.
2. It builds the client and lists the tags. Success adds `Latest: <latest>  update available - run 'd8 dist update'`, or `Latest: <latest>  up to date`.
3. When plugins are installed, each row gets the highest stable published version of the plugin and the verdict `update available`, `up to date`, or `unknown` when that lookup failed.

Any failure of step 2, or of setting up the plugin services in step 3, ends the cluster part: the summary then ends with `Warning: could not check for updates - cluster unreachable.` and the error in parentheses (§14.4), and the plugin table has no `LATEST` and `STATUS` columns. The command always exits 0.

### 10.2. `d8 dist check`

`check` lists the tags and compares latest with the running version (§7):

- when latest is newer: `A newer deckhouse-cli is available: <latest> (current: <running>). Run 'd8 dist update' to upgrade.`
- otherwise: `deckhouse-cli is up to date (<running>).`

Both cases exit 0, so the exit status does not tell whether an update exists. Without a stable version the command fails with `no released deckhouse-cli versions found`.

### 10.3. `d8 dist versions`

`versions`, alias `list`, prints every version newest first, pre-releases included (`formatVersionList` in [dist/cmd/versions.go](../internal/dist/cmd/versions.go)). A line holds the version, padded to the widest one, and its relation to the active version: `* <v>  current` for the active version, `  <v>  newer` above it and `  <v>` below it; `  installed` is appended when the version is stored. When the active version is not a version, such as `dev`, the lines carry no relation. Then follow:

- `Installed locally (switch with 'd8 dist use'), not published in the registry:` and the stored versions missing from the list, when there are any;
- `Current version <active> is not published in the registry.`, when the active version is not in the list.

When no tag is a version, the command fails with `no deckhouse-cli versions found in the registry`.

### 10.4. `d8 dist update`

`update [--version X]` ([dist/cmd/update.go](../internal/dist/cmd/update.go)):

1. Build the client (§4). This needs a usable kubeconfig, and the API server unless the endpoint is explicit, even when the version to install is stored.
2. Without `--version`, list the tags. When latest is not newer than the running version, print `deckhouse-cli is already up to date (<running>).` and exit 0; otherwise X is latest.
3. Print `Updating deckhouse-cli to X...` and switch to X with downloads allowed (§9.1). A stored X is not downloaded again.
4. Print `✓ deckhouse-cli updated to X.` and the notes of §9.3.

With `--version` there is no listing and no "already up to date" check, and X is fetched as typed (§7.3).

### 10.5. `d8 dist use`

`use <version>` takes an argument that MUST parse as a version, otherwise it fails with `invalid version "<arg>": ...`. The first case that matches applies (`newUseCommand` in [dist/cmd/use.go](../internal/dist/cmd/use.go)):

| # | Case | Action | Output | Cluster |
|---|---|---|---|---|
| 1 | the install is store-managed and `current` equals the argument | nothing, not even a smoke test | `deckhouse-cli is already at <tag>.` | not needed |
| 2 | a stored version equals the argument | switch to it (§9.1) | `✓ Switched deckhouse-cli to <tag> (installed locally).` | not needed |
| 3 | the running version equals the argument | switch to it, seeding it first | `✓ Switched deckhouse-cli to <running> (taken from the running binary).` | not needed |
| 4 | otherwise | download the argument as typed and switch | `Version <arg> is not installed locally, downloading...`, then `✓ Switched deckhouse-cli to <arg>.` | needed |

Equality is version equality (§7.1), so `use 0.14` selects a stored `v0.14.0`, while case 4 sends the argument unchanged (§7.3). Cases 2 to 4 also print the notes of §9.3.

### 10.6. Completion

`d8 dist use <TAB>` offers the stored versions, newest first, whose stored spelling starts with the typed text, each described as `installed locally, switches offline` (`completeStoredVersions` in [dist/cmd/use.go](../internal/dist/cmd/use.go)). It reads only S. Because it matches the spelling, `0.1<TAB>` offers nothing for entries named `v0.1…`, although `use 0.13.1` resolves them.

## 11. Flags and environment variables

The flags are persistent on `d8 dist` and are inherited by every subcommand, `d8 dist plugins` included (`AddFlags` in [rpp/flags/flags.go](../internal/rpp/flags/flags.go), `AddKubeFlags` in [plugins/flags/flags.go](../internal/plugins/flags/flags.go)). An environment default is read when d8 starts, and the flag overrides it.

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--kubeconfig`, `-k` | `KUBECONFIG` | `~/.kube/config` | kubeconfig file, or a `:`-separated list of files (§4.1) |
| `--context` | | the current context | kubeconfig context (§4.1) |
| `--rpp-endpoint` | `D8_RPP_ENDPOINT` | discovery | proxy base URL (§4.2) |
| `--rpp-ca-file` | `D8_RPP_CA_FILE` | the system roots only | PEM bundle added to the system roots for the proxy (§4.3) |
| `--insecure-skip-tls-verify` | | off | no TLS verification on either connection (§4.3) |
| `--version` (`update` only) | | latest | exact version to install (§7.3) |
| | `HOME` | | location of S (§8.1) |
| | `LOG_LEVEL` | `info` | `debug` logs the endpoint and how it was found, the requests, and why the running version was not stored |
| | `NO_COLOR`, `FORCE_COLOR` | | `NO_COLOR` disables colours; `FORCE_COLOR` forces them in diagnostics |

## 12. Errors and troubleshooting

| Message | Cause | Remedy |
|---|---|---|
| `registry-packages-proxy: unauthorized (401)` | the kubeconfig carries no token or one the cluster rejects, for example a client-certificate kubeconfig (§5.2) | use a token kubeconfig (§3, item 1) |
| `registry-packages-proxy: forbidden (403)` | `cli-download` is not bound to the identity, or a denial from before the binding is cached | bind it (§3); a cached denial clears within 30 s (§14.2) |
| `registry-packages-proxy: version not found (404)` after `GET /v1/images/deckhouse-cli/tags` | no candidate root holds `deckhouse-cli` (§5.4) | publish or mirror the CLI (§6); for a `cse` registry see §14.12 |
| `registry-packages-proxy: version not found (404)` after `GET /v1/images/deckhouse-cli/images/<tag>` | the tag is not published, is spelled differently, or its index has no image for this platform | use the spelling that `d8 dist versions` prints (§14.1); check the platforms of the index (§14.3) |
| `registry-packages-proxy: upstream error (5xx)` | the proxy failed to reach the registry, or has no registry configuration | retry; check the `registry-packages-proxy` pods in `d8-cloud-instance-manager` |
| `registry-packages-proxy: endpoint discovery via the Kubernetes API failed` | the API server is unreachable or its certificate is not trusted, the API rejects the identity, the identity may not `get` the Ingress (the chain ends in `is forbidden`, §14.5), or no proxy pod is ready | fix the cause (§3, item 3), or skip discovery with `--rpp-endpoint https://registry-packages-proxy.<publicDomain>` |
| `x509: certificate signed by unknown authority` from the endpoint | the proxy certificate does not chain to a system root | `--rpp-ca-file <ca.pem>` |
| `x509: cannot validate certificate for <IP> because it doesn't contain any IP SANs` | the endpoint is a node or pod IP (§4.2, §5.1) | `--rpp-endpoint` with the Ingress host |
| `dial tcp <IP>:4219: ...` | a pod endpoint, unreachable from outside the cluster network | `--rpp-endpoint` with the Ingress host |
| `unexpected status 3xx: ...` | the endpoint redirects, for example from `http` or to a login page | give the final `https` URL |
| `invalid proxy endpoint: ...` | `--rpp-endpoint` is not an `https` URL with a host | fix the value |
| `unsupported client configuration: insecure TLS verification and a CA bundle are mutually exclusive` | `--insecure-skip-tls-verify` together with `--rpp-ca-file` or `D8_RPP_CA_FILE` | drop one of them |
| `invalid CA bundle: no certificates parsed from CA data` | the CA file holds no PEM certificate | fix the file |
| `set up kubernetes client: reading kubeconfig file: ...` | no usable kubeconfig | §4.1 |
| `file not found in image: "d8"` | the last layer of the image holds no `d8` (§5.5) | fix the image (§6) |
| `new binary failed its --version smoke test: ...` | the binary does not run here (another platform, corrupt, missing libraries), or a stored entry is broken | for a stored entry, delete `S/versions/<tag>` and retry (§14.7) |
| `an update is already in progress (lock file <path> exists)` | another `update` or `use` is running, or one was killed less than 1 h ago | wait, or delete the lock file when no d8 is running |
| `updating d8 needs write access to <dir>` | migration cannot rename X (§9.1, step 9) | keep d8 in a directory the user can write; read §14.10 and §14.11 before re-running with `sudo` |
| `tag "<X>" is not a semver version` | `--version` is not a version | §7.3 |
| `version store is unavailable ...` | the home directory cannot be determined | set `HOME` |
| `self-update is not supported on Windows; ...` | the client runs on Windows | replace the binary by hand |
| `no released deckhouse-cli versions found` | the registry has pre-releases only | `update --version <pre-release>` |
| `no deckhouse-cli versions found in the registry` | no tag is a version | §6 |
| `Version <v> is not installed locally, downloading...` for a version stored before | the command uses another store, of another user or `$HOME` (§8.1), or the entry was deleted | run as the user who stored it |

`deckhouse-cli is already up to date (<v>).` and `deckhouse-cli is already at <v>.` are not errors; `update --version X` installs another version.

## 13. Example

A workstation runs d8 v0.13.1 as a plain file at `/home/alice/.local/bin/d8`; the registry publishes `v0.13.0`, `v0.13.1`, `v0.14.0` and `v0.15.0-rc.1`:

```console
$ d8 dist check
A newer deckhouse-cli is available: v0.14.0 (current: v0.13.1). Run 'd8 dist update' to upgrade.

$ d8 dist update
Updating deckhouse-cli to v0.14.0...
✓ deckhouse-cli updated to v0.14.0.
The d8 binary in PATH is now a symlink into the version store; the previous binary is kept with a ".old" suffix.

$ d8 dist versions
  v0.15.0-rc.1  newer
* v0.14.0       current  installed
  v0.13.1       installed
  v0.13.0

$ d8 dist use v0.13.1
✓ Switched deckhouse-cli to v0.13.1 (installed locally).
Previous version v0.14.0 remains installed - switch back with 'd8 dist use v0.14.0'.
```

The proxy received `GET /v1/images/deckhouse-cli/tags` from `check`, `update` and `versions`, and `GET /v1/images/deckhouse-cli/images/v0.14.0?platform=linux-amd64` from `update`; `use` sent nothing. Afterwards:

```
/home/alice/.local/bin/
├── d8 -> /home/alice/.deckhouse-cli/cli/current
└── d8.old                  the v0.13.1 file that update replaced
/home/alice/.deckhouse-cli/cli/
├── current -> versions/v0.13.1/d8
└── versions/
    ├── v0.13.1/d8          seeded from the running binary by update
    └── v0.14.0/d8          downloaded by update
```

## 14. Known deviations

Verified on 2026-10-01 by running the `d8 dist` command tree of this revision against a fake registry-packages-proxy and Kubernetes API, unless an item says otherwise. Each item breaks a rule above, a promise of the command help, or a statement of the module documentation.

1. Versions are fetched as typed. `use` resolves stored versions by value but downloads its argument unchanged (`requested.Original()` in [dist/cmd/use.go](../internal/dist/cmd/use.go)): with `v0.15.0-rc.1` published, `d8 dist use 0.15.0-rc.1` requested `/images/0.15.0-rc.1` and failed with `version not found (404)`, and `update --version 0.14.0` failed the same way. The `v` prefix is optional for stored versions only.
2. The `403` diagnostic names the wrong wait. It says `authorization is cached ~5 min - after binding, retry with a fresh token`, while kube-rbac-proxy caches a denial for 30 s; 5 min is how long an allowed decision is cached, which delays a revocation, not a grant (§5.2). [plugins.md](plugins.md) and the module documentation state 30 s.
3. Every `404` is diagnosed as an unpublished version, with the advice to run `d8 dist versions`. A `404` of the tag listing, from a registry without a `deckhouse-cli` repository, makes that command fail the same way, and a `404` caused by an index without an image for the client's platform concerns a version that `d8 dist versions` lists.
4. `status` calls every failure `cluster unreachable`. The `401`, `403`, `404`, `5xx` and `3xx` answers of a reachable proxy all produced `Warning: could not check for updates - cluster unreachable.`, with the real error only in parentheses.
5. A missing permission on the Ingress is diagnosed as an API server problem. `cli-download` does not include it (§3), so discovery by an identity with that role alone ends in `403`, and the diagnostic gives the cause `that server was unreachable or presented an invalid certificate` and no word about the permission; `is forbidden` appears only in the error chain.
6. The smoke test does not check the version. A binary that prints `d8 version v0.15.0-rc.1`, served under the tag `v0.14.0`, was stored and activated as `v0.14.0`: `check` and `d8 --version` then report v0.15.0-rc.1, while `status`, `versions` and `use` report v0.14.0.
7. A broken store entry blocks its version for good. An entry that fails the smoke test is neither replaced nor removed: `update --version v0.14.0` and `use v0.14.0` failed with `exec format error` without any download, and `versions` kept marking v0.14.0 `installed`, until `S/versions/v0.14.0` was deleted by hand.
8. `update --version <active>` switches to the active version again and prints, for the active v0.13.0, `Previous version v0.13.0 remains installed - switch back with 'd8 dist use v0.13.0'.`
9. Migration discards earlier backups. Every migration renames X over `X.old`: after another tool had replaced the symlink with a v0.13.0 file, the next `use` made that file `d8.old`, and the original v0.13.1 backup was gone; v0.13.1 survived only as the store entry seeded by the first migration.
10. A run under another `$HOME` corrupts the first store. When the PATH entry points into the store of one home and d8 runs with another `$HOME`, for example under `sudo` (§8.1), X lies outside the store in use, so the switch treats the install as plain-file and migrates X, which is an entry of the first store: `versions/v0.14.0/d8` of the first store was renamed to `d8.old` and replaced by a symlink to the `current` of the second store, and the first user's d8 silently became v0.13.0. By §8.1, a plain-file install migrated under `sudo` also points the PATH entry into root's home, which other users usually cannot traverse. The diagnostic of §9.1, step 9, nevertheless suggests `re-run the command with sudo`.
11. The platform installer writes through the migrated PATH entry. On nodes, bashible installs d8 from the registry package `d8` with `cp -f d8 /opt/deckhouse/bin` and `chmod u+s /opt/deckhouse/bin/d8`, and does it again whenever the package digest changes ([install](https://github.com/deckhouse/deckhouse/blob/main/modules/007-registrypackages/images/d8/scripts/install)). Reproduced with GNU coreutils 9.4: after migration, `cp -f` writes into the active store entry through the symlinks, and `chmod` makes that entry setuid, so the entry no longer holds the version of its tag (`status`, `versions` and `use` report the old tag, `check` and `d8 --version` the new binary). When the entry is being executed at that moment, `cp -f` gets `ETXTBSY`, removes the symlink and writes a plain file instead, which the next switch migrates again (item 9). Besides, the d8 that bashible installs is setuid root to reach the plugins directory, while a store entry has mode `0755`, so a migrated `/opt/deckhouse/bin/d8` no longer runs setuid.
12. `cse` registries are not found. `pkg.Edition` includes `cse` ([pkg/edition.go](../pkg/edition.go)), so `d8 mirror push` to `…/deckhouse/cse` puts the CLI at `…/deckhouse/deckhouse-cli` ([mirror-bundle-layout.md](mirror-bundle-layout.md#82-edition-split) §8.2), while the proxy strips only `ce`, `be`, `se`, `se-plus`, `ee` and `fe` (§5.4) and looks only at `…/deckhouse/cse/deckhouse-cli`. A CSE cluster filled that way answers `404` to every self-update request. Established from the code of both sides, not by running.
