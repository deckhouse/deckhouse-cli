# `d8 dist` self-update

This document specifies how `d8 dist` updates the d8 binary through the cluster: what a working setup requires, how the client reaches the registry-packages-proxy, what the proxy serves, how versions are selected, how the local version store is laid out, and what every command does. It describes the implementation as of 2026-10-01, deckhouse-cli at this revision and the registry-packages-proxy of deckhouse `main`, and every rule names the code that implements it. Where the implementation breaks the contract, §14 lists the deviation.

The key words MUST, MUST NOT, SHOULD NOT and MAY are used as in RFC 2119. Rules for the publisher apply to anything that puts `deckhouse-cli` images into a registry, the release pipeline or `d8 mirror push`; rules for the client describe what d8 does, rules for the proxy what the registry-packages-proxy does.

Plugins (`d8 dist plugins`) reach the proxy the same way and are described in [plugins.md](plugins.md). How `d8 mirror` carries the binary into an air-gapped registry is described in [mirror-bundle-layout.md](mirror-bundle-layout.md) [§6.7](mirror-bundle-layout.md#67-deckhouse-clitar) and [§8.10](mirror-bundle-layout.md#810-routing-table).

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
| C | The cluster repository, read by the proxy from the Secret `d8-system/deckhouse-registry` (§5.4). |
| Tag | A tag of the `deckhouse-cli` repository as the proxy lists it, or a string used as one: the argument of a download (§7.3) or the name of a store entry. |
| Version | A string that parses as a semantic version (§7.1). A stable version has no pre-release part. |
| Platform | `<GOOS>-<GOARCH>` of the running binary, for example `linux-amd64` (`currentPlatform` in [rpp/platform.go](../internal/rpp/platform.go)). |
| Running version | The version compiled into the running binary (`internal/version.Version`, `dev` when the build sets none), as `d8 --version` prints it. It need not be a version. |
| S | The version store, `$HOME/.deckhouse-cli/cli` (§8.1). |
| Store entry | `S/versions/<tag>/d8`, one stored tag. |
| X | The path of the running executable with all symlinks resolved, taken when the command starts (`CurrentExecutable` in [selfupdate/update.go](../internal/selfupdate/update.go)). |
| Store-managed, plain-file | An install is store-managed when X lies inside S (`Store.Contains`), and plain-file otherwise. |
| Active tag | The tag that `S/current` names (§8.3), or none. |
| Active version | For a store-managed install the active tag when there is one, otherwise the running version (`activeVersionTag` in [dist/cmd/updater.go](../internal/dist/cmd/updater.go)). |
| PATH entry | The file a user runs as `d8`, for example `/opt/deckhouse/bin/d8`. In a plain-file install it is X or a symlink to X; migration (§9.1) replaces X with a symlink to `S/current`. |

## 2. Invariants

- Source. d8 gets the list of versions and the binaries only from the proxy, over HTTPS, authenticated with the identity of the current kubeconfig. It holds no registry credentials and never contacts a registry itself (§4).
- Selection. The active binary is reached through the chain PATH entry → `S/current` → `S/versions/<tag>/d8`. d8 switches versions only by atomically replacing `S/current`, writes every store entry once and deletes none, and rewrites X only when it migrates a plain-file install, in two steps that are not atomic together (§8, §9).
- Verification. A binary becomes active only after `<binary> --version`, run from its final store path, exited 0 within 30 s (§9.1).
- Locality. A `use` that needs no download and shell completion work without a kubeconfig and without network (§10.5, §10.6). `status` without them prints only the local part (§10.1), and `update` of a stored tag with an explicit endpoint needs a kubeconfig that parses but no network (§10.4). Every other operation needs both.

## 3. Requirements

In short: a token kubeconfig, the grants below, then `d8 dist check` and `d8 dist update`. Self-update works when all of the following hold:

1. The kubeconfig (§4.1) authenticates with a bearer token that the cluster accepts: a static token, a token file, an exec plugin such as `d8 login get-token`, or an OIDC auth provider. A personal OIDC kubeconfig is issued by the kubeconfig generator of the Deckhouse web UI at `https://console.<publicDomain>`; older Deckhouse releases also serve a standalone generator at `https://kubeconfig.<publicDomain>`. The proxy rejects the client certificates of kubeconfigs (§5.2), so the `kubernetes-admin` kubeconfig of a master node does not work.
2. The identity may `get` the subresource `deployments/cli-binary` of `registry-packages-proxy` in `d8-cloud-instance-manager`. The ClusterRole `d8:registry-packages-proxy:cli-download` grants exactly that, and it is bound to nobody by default. The same permission covers plugin downloads; plugins with cluster requirements need more ([plugins.md](plugins.md#15-known-deviations) §15.11).
3. Either the identity may `get` the Ingress `registry-packages-proxy` in `d8-cloud-instance-manager`, which `cli-download` does not include, or the endpoint is given explicitly (§4.2).
4. The endpoint is reachable and presents a certificate that the client trusts (§4.3).
5. The cluster registry holds `deckhouse-cli` images that satisfy §6.
6. The client runs on Linux or macOS, since `update` and `use` refuse to switch on Windows (§9.1). The user can write to S and, for the first switch of a plain-file install, to the directory of X.

An administrator grants items 2 and 3 as [Granting access to Deckhouse CLI downloads](https://github.com/deckhouse/deckhouse/blob/main/modules/039-registry-packages-proxy/docs/README.md#granting-access-to-deckhouse-cli-downloads) describes; `--user=<name>` or `--serviceaccount=<namespace>:<name>` can replace `--group`:

```shell
d8 k create clusterrolebinding d8-cli-download --clusterrole=d8:registry-packages-proxy:cli-download --group=<group>
d8 k -n d8-cloud-instance-manager create role d8-cli-ingress --verb=get --resource=ingresses --resource-name=registry-packages-proxy
d8 k -n d8-cloud-instance-manager create rolebinding d8-cli-ingress --role=d8-cli-ingress --group=<group>
```

A grant takes effect within 30 s and a revocation within 5 min (§5.2). To check a grant without the user's credentials, impersonate the user together with the group of the binding, since `--as` alone carries no groups:

```shell
d8 k auth can-i get deployments/registry-packages-proxy -n d8-cloud-instance-manager --subresource=cli-binary --as=<user> --as-group=<group>
d8 k auth can-i get ingresses/registry-packages-proxy -n d8-cloud-instance-manager --as=<user> --as-group=<group>
```

`d8 dist check` exits 0 once items 1–4 hold and the registry lists a stable version (§10.2). It exercises only items 1 and 2 of §6, so `d8 dist update` is the first command to exercise items 3–5 of §6 and item 6 above.

## 4. Reaching the proxy

```
d8 dist <command>
  │ (1) discovery, unless the endpoint is explicit (§4.2):
  │     GET ingresses/registry-packages-proxy, then pods
  ├───────────────────────────────▶ Kubernetes API server (kubeconfig server:)
  │ (2) GET /v1/images/deckhouse-cli/..., credentials of the kubeconfig (§4.1)
  └───────────────────────────────▶ kube-rbac-proxy :4219          TokenReview, SubjectAccessReview (§5.2)
                                      └─▶ registry-packages-proxy   credentials of d8-system/deckhouse-registry (§5.4)
                                            └─▶ cluster registry
```

### 4.1. Identity

- The kubeconfig is `--kubeconfig` (`-k`), by default `$KUBECONFIG`, or `~/.kube/config` when that is unset (`DefaultKubeconfigPath` in [utilk8s/clientset.go](../internal/utilk8s/clientset.go)). A value with several paths separated by `:` is merged as kubectl does, and `--context` selects a context (`SetupK8sClientSet`).
- d8 builds the Kubernetes client and the proxy client from the same configuration (`newUpdater` in [dist/cmd/updater.go](../internal/dist/cmd/updater.go)). The proxy client keeps its credentials, whether a bearer token, a token file, an exec plugin, an auth provider or a client certificate, and sends them with every request (`buildHTTPClient` in [rpp/transport.go](../internal/rpp/transport.go)). The proxy accepts bearer tokens, and client certificates only from its own CA (§5.2), which kubeconfigs do not carry.
- A kubeconfig that parses is needed even with an explicit endpoint, because the proxy client is built from it. Without one, a `use` that needs no download and completion still work, and `status` prints only its local part (§2).

### 4.2. Endpoint

The endpoint is chosen in this order (`NewClusterClient` in [rpp/connect.go](../internal/rpp/connect.go)):

1. `--rpp-endpoint`, by default `$D8_RPP_ENDPOINT`. The value MUST be an absolute `https` URL with a host (`validateBaseURL` in [rpp/client.go](../internal/rpp/client.go)), and it MUST NOT carry userinfo, a query or a fragment, which pass the check but break the requests (§14.16). Trailing slashes are removed, and a path is kept as a prefix of the routes.
2. Otherwise d8 discovers it through the API server of the kubeconfig (`chooseDiscoveredEndpoint` in [rpp/endpoint.go](../internal/rpp/endpoint.go)):
   - Ingress lookup. d8 reads the Ingress `registry-packages-proxy` in `d8-cloud-instance-manager`. The host of the first rule that has one gives `https://<host>`.
   - Pod lookup. Only when the API answers that the Ingress does not exist, or the Ingress has no rule with a host, d8 lists the pods labelled `app=registry-packages-proxy` in that namespace, which needs `list` on pods there, and takes the first one that is `Running`, not being deleted, has a pod IP and has the condition `Ready=True`: `https://<pod IP>:4219`. There is no failover between pods.
   - Failure. Any other failure of the Ingress lookup, a `401` or `403` included, and any failure of the pod lookup stop the command with an endpoint discovery error (§4.5). The pod lookup is not tried after a failed Ingress lookup.

The endpoint and how it was found are logged at debug level only (`LOG_LEVEL=debug`). A pod endpoint is the address of a master node, since the proxy runs in the host network; it is reachable only from the cluster network, and its certificate does not cover the IP (§5.1), so it works only with `--insecure-skip-tls-verify`. d8 reads no Gateway API objects: a cluster that publishes the proxy only through an HTTPRoute (§5.1) ends up in the pod lookup, so the endpoint MUST be given explicitly there.

### 4.3. TLS

d8 opens two TLS connections and verifies each on its own ([rpp/flags/flags.go](../internal/rpp/flags/flags.go)):

| Connection | Used for | Verified against | With `--insecure-skip-tls-verify` |
|---|---|---|---|
| API server | discovery (§4.2) | the CA of the kubeconfig, or the system roots when it has none; not verified when the kubeconfig sets `insecure-skip-tls-verify` | not verified; the kubeconfig CA is dropped (`WithInsecureSkipTLSVerify` in [utilk8s/clientset.go](../internal/utilk8s/clientset.go)) |
| Proxy | every proxy request | the system roots, plus every certificate of `--rpp-ca-file` (by default `$D8_RPP_CA_FILE`) | not verified |

- `buildHTTPClient` resets the CA and `insecure-skip-tls-verify` of the kubeconfig for the proxy connection, so neither applies there, but it keeps `tls-server-name`, which then breaks the verification of the proxy (§14.15).
- A non-empty CA file MUST contain at least one PEM certificate, otherwise the command fails with `invalid CA bundle: no certificates parsed from CA data`; an empty file is ignored. With a CA file, a failure to load the system roots fails the command too (`certPoolWith`).
- `--insecure-skip-tls-verify` together with a CA file, from the flag or from the environment, is rejected with `unsupported client configuration: insecure TLS verification and a CA bundle are mutually exclusive`. The check runs when the proxy client is built (`New` in [rpp/client.go](../internal/rpp/client.go)), after discovery, so a failing discovery reports its own error instead.
- `--insecure-skip-tls-verify` SHOULD NOT be used beyond debugging: it sends the credentials of the kubeconfig to hosts whose identity is not verified.
- Forward HTTP proxies apply to requests to the proxy as to API requests: the `proxy-url` of the kubeconfig, which ignores `NO_PROXY`, otherwise `HTTPS_PROXY` and `NO_PROXY` (`tlsTransportCache.get` in `k8s.io/client-go/transport/cache.go`).

### 4.4. Requests

- d8 makes every request once and has no retry loop of its own (`Client.do` in [rpp/client.go](../internal/rpp/client.go)). Underneath, the Go transport resends a `GET` once when a reused connection fails before any response, HTTP/2 retries refused streams, and the discovery requests of §4.2 go through the client-go REST client, which retries a `GET` up to 10 times on a broken connection or on a `429` or `5xx` with `Retry-After`.
- Redirects are not followed, so that the credentials never reach another host, and a `3xx` is an error (`newClient`).
- A connection MUST be established within 30 s (the client-go dialer) and its TLS handshake MUST complete within 10 s, and the response headers MUST arrive within 30 s, except over HTTP/2 in release builds (§14.14). The body has no time limit ([rpp/transport.go](../internal/rpp/transport.go)).
- Listing: `GET <endpoint>/v1/images/deckhouse-cli/tags` with `Accept: application/json`. The body, read up to 4 MiB, MUST be a JSON object whose member `tags` is an array of strings; other members are ignored (`ListTags`). A longer body fails with `decode tags response for "deckhouse-cli": unexpected EOF`.
- Download: `GET <endpoint>/v1/images/deckhouse-cli/images/<tag>?platform=<platform>`, where `<tag>` MUST match `^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$` (`validateTag` in [rpp/image.go](../internal/rpp/image.go)). The body MUST be a gzip-compressed tar. d8 takes the first regular-file entry whose name, without a leading `./`, has the base name `d8`, and writes it with mode `0755` (`ExtractFileToPath` in [rpp/extract.go](../internal/rpp/extract.go)). A file longer than 512 MiB fails with `"<path>/d8.staged" exceeds the 536870912-byte limit`, and more than 1 GiB of decompressed tar with `read tar: unexpected EOF`; a tar without such an entry fails with `file not found in image: "d8"`.
- d8 verifies no digest and no signature of the download, because the proxy reports only the manifest digest, not a hash of the body (`PullImage`). Integrity rests on the TLS connection to the proxy and on the smoke test (§9.1).
- The client also implements `GET /v1/images/<image>/manifests/<ref>`, for plugins; self-update does not use it.

### 4.5. Status mapping

`Client.do` maps the response status to an error that starts with the method and the path of the request, for example `GET /v1/images/deckhouse-cli/tags: unauthorized` (`statusError` in [rpp/client.go](../internal/rpp/client.go)). The `d8 dist` commands turn the recognised errors into a diagnostic (`Diagnose` in [dist/cmd/errdetect/diagnose.go](../internal/dist/cmd/errdetect/diagnose.go)), printed to stderr as `error: <category>`, the error chain and the suggested fixes.

| Response or failure | Error, after `<METHOD> <path>: ` | Diagnostic category |
|---|---|---|
| `2xx` | none | |
| `401` | `unauthorized` | `registry-packages-proxy: unauthorized (401)` |
| `403` | `forbidden` | `registry-packages-proxy: forbidden (403)` |
| `404` | `image or tag not found` | `registry-packages-proxy: version not found (404)` |
| `500` and above | `registry proxy upstream error (status <code>): <body>` | `registry-packages-proxy: upstream error (5xx)` |
| any other status, `3xx` included | `unexpected status <code>: <body>` | none |
| discovery failed (§4.2) | `registry-packages-proxy endpoint discovery failed: <cause>`, without the prefix | `registry-packages-proxy: endpoint discovery via the Kubernetes API failed` |

- `<body>` is the trimmed response body, at most 256 bytes, with control characters replaced by spaces; when it is blank, `: <body>` is left out. The bodies of `401`, `403` and `404` are dropped, so the user name that the proxy puts into a `403` is not shown.
- Any error without a diagnostic is printed as `Error executing command: <error>`. A failing command exits 1, except after a failed smoke test (§14.13) (`execute` in [cmd/d8/root.go](../cmd/d8/root.go)).

## 5. The registry-packages-proxy

This section describes the server side that d8 relies on, as implemented on deckhouse `main`. The module has no settings of its own; the global `modules` settings of Deckhouse configure it.

### 5.1. Exposure

The module runs the proxy on the master nodes in the host network, one replica per master in HA mode, which is the default with more than one master, and a single replica otherwise ([deployment.yaml](https://github.com/deckhouse/deckhouse/blob/main/modules/039-registry-packages-proxy/templates/deployment.yaml)):

| Access | Address | Certificate | Routes |
|---|---|---|---|
| Master node | `https://<node IP>:4219` | generated by kube-rbac-proxy at start, self-signed for the node host name, no IP SANs | all |
| Service `registry-packages-proxy` | `https://registry-packages-proxy.d8-cloud-instance-manager.svc` | the node certificate | all |
| Ingress `registry-packages-proxy` | `https://registry-packages-proxy.<publicDomain>` | per `modules.https` | `/v1/images/` only |
| HTTPRoute `registry-packages-proxy` | the same host | per `modules.https` | `/v1/images/` only |

- The Service and the Ingress exist only when `publicDomainTemplate` is set and the cluster is bootstrapped, the Ingress also only when `modules.ingress.enabled` is not false ([ingress.yaml](https://github.com/deckhouse/deckhouse/blob/main/modules/039-registry-packages-proxy/templates/ingress.yaml)).
- The HTTPRoute exists, independently of the Ingress, when in addition `modules.gatewayAPI.enabled` is not false, a Gateway is available to the module and the HTTPRoute and ListenerSet APIs are installed. It attaches through a ListenerSet and serves HTTPS only when `modules.https.mode` is `CertManager` or `CustomCertificate`, otherwise plain HTTP on port 80 ([httproute.yaml](https://github.com/deckhouse/deckhouse/blob/main/modules/039-registry-packages-proxy/templates/httproute.yaml)).
- kube-rbac-proxy generates its certificate for `os.Hostname()` when it is given none, which is the case here.

### 5.2. Authentication and authorization

Requests pass kube-rbac-proxy v0.11.0 with the Deckhouse patches ([kube-rbac-proxy](https://github.com/deckhouse/deckhouse/tree/main/modules/000-common/images/kube-rbac-proxy)) before they reach the proxy:

- Authentication. A bearer token is checked with a TokenReview. A client certificate is accepted only when it is presented directly on port 4219, since the Ingress and the HTTPRoute terminate TLS, and only when the CA of the ConfigMap `kube-rbac-proxy-ca.crt` signed it; that is the internal kube-rbac-proxy CA of Deckhouse, not the cluster CA that signs kubeconfig certificates such as `kubernetes-admin`. A request without an accepted credential gets `401` with the body `Unauthorized`.
- Authorization. A SubjectAccessReview for `get` on `apps/deployments/cli-binary` named `registry-packages-proxy` in `d8-cloud-instance-manager`. A denial is `403` with the body `Forbidden (user=<name>, verb=get, resource=deployments, subresource=cli-binary)`. kube-rbac-proxy derives the verb from the method and maps only `POST`, `GET`, `PUT`, `PATCH` and `DELETE`, so a `HEAD` is checked with an empty verb, which `cli-download` does not grant.
- Caching, as set in kube-rbac-proxy v0.11.0 (`pkg/authn/delegating.go`, `pkg/authz/auth.go`): token reviews are cached for 2 min, allowed decisions for 5 min and denied decisions for 30 s. A new grant therefore takes effect within 30 s, and a revoked one keeps working for up to 5 min. With `--stale-cache-interval=5m`, a token or a certificate that was allowed during the last 5 min stays allowed while the API server is unreachable; the entry is keyed by the token, so a refreshed OIDC token is not covered.
- Other failures: `401` when the TokenReview itself fails, `500` with `Authorization error (user=<name>, …)` when the SubjectAccessReview fails and the stale cache has no entry, and `502` when the proxy container does not answer.

### 5.3. Routes

The routes are under `/v1/images/<image>/`, where `<image>` is `deckhouse-cli` or `deckhouse-cli/plugins/<name>` with a single-segment `<name>`; any other path is `404` (`CLIHandler` in [proxy.go](https://github.com/deckhouse/deckhouse/blob/main/go_lib/registry-packages-proxy/proxy/proxy.go)):

| Route | Response |
|---|---|
| `GET tags` | `200` with `{"name": "<image>", "tags": [...]}` holding every tag of the repository, unfiltered |
| `GET images/<tag>[?platform=<os>-<arch>]` | `200` with the body of §5.5 as `application/x-gzip` |
| `GET manifests/<ref>` | `200` with the raw manifest and its media type |

- A download carries `Content-Disposition: attachment; filename="<last image segment>-<tag>.tar.gz"`, and `ETag` and `Docker-Content-Digest` set to the resolved manifest digest.
- `HEAD images/<tag>` is routed as well, but kube-rbac-proxy rejects it for `cli-download` (§5.2).
- `404` means no candidate repository (§5.4), no such tag, or no image for the platform (§5.5); `400` a `platform` parameter that is not `<os>-<arch>`; `502` an upstream failure; `500` no registry configuration in the proxy. kube-rbac-proxy adds the codes of §5.2.

### 5.4. Repository lookup

The proxy reads C from the Secret `d8-system/deckhouse-registry` and tries, in this order (`cliRepoCandidates` in [cli_repo.go](https://github.com/deckhouse/deckhouse/blob/main/go_lib/registry-packages-proxy/proxy/cli_repo.go)):

1. `C/deckhouse-cli`, where `d8 mirror push` to C puts the CLI when C does not end with an edition;
2. `<C without its last segment>/deckhouse-cli`, only when the last segment of C is `ce`, `be`, `se`, `se-plus`, `ee` or `fe` and the host and at least one path segment remain. The module documentation names this root as the one where the official registry publishes the CLI once for all editions, and `d8 mirror push` puts it there for a target that ends with an edition ([mirror-bundle-layout.md](mirror-bundle-layout.md#82-edition-split) §8.2).

- `cse` is not in the list (§14.12).
- A candidate that answers `404`, `401` or `403`, or fails, is skipped. When all fail, the first error that is not a `404` is reported, as `502`, and otherwise `404`; a registry that hides a missing root behind `401` or `403` therefore turns a missing CLI into `502`.
- A candidate that succeeds after only `404`, `401` or `403` answers of the candidates before it is remembered per pod, for the CLI and every plugin alike, and is tried first afterwards.
- Both candidates are read with the credentials of the same Secret. When C is the in-cluster registry of the `registry` module, `registry.d8-system.svc:5001/system/deckhouse`, it does not end with an edition, so only `C/deckhouse-cli` is tried, and the proxy reads it through the registry agent of its node at `127.0.0.1:5001` without credentials, or with the store account of the cluster while the node has no agent (`throughTheAgent` in [credentials/agent.go](https://github.com/deckhouse/deckhouse/blob/main/modules/039-registry-packages-proxy/images/registry-packages-proxy/src/internal/credentials/agent.go)).

### 5.5. Platform and body

- With `platform`, a tag that is an image index resolves to the first child whose `os` and `architecture` equal the parameter; variants are not compared, and a missing child is `404`. A tag that is a single image is served whatever its platform. Without `platform`, an index resolves to `linux/amd64`. d8 always sends `platform` (§4.4).
- The body is the last layer of the resolved image, compressed as stored in the registry (`GetPackage`, `selectImageLayer` in [default_client.go](https://github.com/deckhouse/deckhouse/blob/main/go_lib/registry-packages-proxy/registry/default_client.go)). Layers are flattened only for package icons, not on these routes (§14.18).

## 6. Publishing contract

A registry serves self-update to clients on a platform when it follows these rules:

1. The repository `deckhouse-cli` MUST exist at a root that the proxy tries (§5.4).
2. Every release MUST be tagged with the version its binary prints in `d8 --version`, spelled `vX.Y.Z`, or `vX.Y.Z-<pre-release>` for a pre-release. Other tags MAY exist, but a tag that is not a release MUST NOT parse as a version: bare numbers such as `20260101` or `v1` do (§7.1), and d8 would offer them as latest.
3. Every version tag MUST be an image index with a child whose `os` and `architecture` match the platform, or a single image built for the platform.
4. The last layer of that image MUST be a gzip-compressed tar holding a regular file with the base name `d8`, at most 512 MiB long, within the first 1 GiB of decompressed tar (§4.4).
5. That file MUST run on the platform, and `d8 --version` MUST exit 0 within 30 s (§9.1).
6. A publisher MUST NOT move a published tag to another binary: a client keeps the binary it stored under that tag (§8.3).

The release images are multi-platform indexes under plain version tags (`rppSource` in [selfupdate/rpp_source.go](../internal/selfupdate/rpp_source.go)). `d8 mirror pull` copies one such index, with all its children, into `deckhouse-cli.tar`, and `d8 mirror push` places it at a root of §5.4 ([mirror-bundle-layout.md](mirror-bundle-layout.md#67-deckhouse-clitar) §6.7, §8.10), except for `cse` targets (§14.12).

## 7. Version selection

### 7.1. Parsing and order

- Tags and arguments are parsed with `semver.NewVersion` of `github.com/Masterminds/semver/v3` v3.5.0, which coerces its input (`CoerceNewVersion`): the `v` prefix is optional, missing minor and patch numbers are 0 and leading zeros are accepted, so `0.14`, `v0.14` and `v0.14.0` are one version, and a bare number such as `20260101` is the version 20260101.0.0. `V0.14.0`, `1.2.3.4`, `v0.14.0-rc.01` and strings with surrounding spaces do not parse. Strings that do not parse, such as `latest` or `sha256-<hex>.att`, are not versions and are skipped everywhere (`Updater.Versions`, `maxSemver` in [selfupdate/update.go](../internal/selfupdate/update.go)).
- Versions compare by SemVer precedence, build metadata ignored, so two spellings of one version are equal (§14.1, §14.17).
- Latest is the highest stable version, reported in the spelling of the first listed tag that has it (`LatestVersion`). Pre-releases are installed only when named.
- A version is newer when it is greater than the reference version (§7.2). For `check`, `update` and `status`, a reference that is not a version, such as `dev`, is older than every version; `versions` then shows no relation at all (§10.3).

### 7.2. Reference version

| Command | Compares against |
|---|---|
| `check`, `update` | the running version (`LatestVersion(…, version.Version)` in [dist/cmd/check.go](../internal/dist/cmd/check.go), [dist/cmd/update.go](../internal/dist/cmd/update.go)) |
| `status`, `versions` | the active version (`activeVersionTag` in [dist/cmd/status.go](../internal/dist/cmd/status.go), [dist/cmd/versions.go](../internal/dist/cmd/versions.go)) |
| `use` | the active tag of a store-managed install for "already at", then the stored versions and the running version for an offline switch (§10.5) |

The running and the active version differ when X is a store entry other than the target of `current`, for example one run by its full path, and when an entry does not hold the version of its tag (§14.6, §14.11).

### 7.3. Requested versions

`update --version X`, and `use X` when X is not stored, download the tag X exactly as typed (§10.4, §10.5), so X MUST be spelled as the published tag: `0.14.0` does not find `v0.14.0` (§14.1).

- X MUST parse as a version. `use` rejects anything else at once with `invalid version "<X>": invalid semantic version`. `update --version` rejects it only when it switches, after printing `Updating deckhouse-cli to X...` and seeding the store, with `tag "<X>" is not a semver version: invalid semantic version` (`install` in [selfupdate/store.go](../internal/selfupdate/store.go)).
- X MUST also be a valid tag (§4.4). A version with build metadata such as `v1.0.0+build.5` parses but fails with `download new binary: invalid image reference: "v1.0.0+build.5" is not a valid image tag`, leaving an empty `S/versions/v1.0.0+build.5/` behind.
- X need not appear in the listing, since d8 asks the proxy for it directly. Downgrades and pre-releases are allowed, and an empty `--version` means latest.
- A downloaded version is stored under the spelling it was fetched with.

## 8. Version store

### 8.1. Location

S is `.deckhouse-cli/cli` in the home directory of the process, which is `$HOME` on Linux and macOS (`NewStore` in [selfupdate/store.go](../internal/selfupdate/store.go)).

- `$HOME` MUST be an absolute path: migration links X to `$HOME/.deckhouse-cli/cli/current` as given, so a relative `$HOME` leaves a dangling symlink and d8 no longer starts.
- The store belongs to a `$HOME`, not to a machine. `sudo`, whose default `env_reset` sets `HOME` to the target user's home, works on root's store (§14.10).
- Without a home directory, `use` fails with `version store unavailable: locate home directory for the version store: $HOME is not defined`, and `update` fails only when it switches, with `version store is unavailable (home directory cannot be resolved)`. Completion then offers nothing, and `status`, `check` and `versions` run without store data.

### 8.2. Files

| Path | Kind | Written by | Content |
|---|---|---|---|
| `S/versions/<tag>/d8` | regular file, mode `0755` | a download, or the seeding of a plain-file install (§9.1, steps 4 and 5) | the binary of `<tag>` |
| `S/versions/<tag>/d8.staged` | regular file | that write, while it runs | removed when the write ends |
| `S/current` | symlink | every switch, through `S/current.staged` and a rename | `versions/<tag>/d8`, relative |
| `S/install.lock` | empty file | `update` and `use`, while they switch | §8.4 |
| `X.old` | regular file | migration | the plain file that X was |
| X | symlink | migration | `$HOME/.deckhouse-cli/cli/current` |

Directories are created with mode `0755` before the umask, while a store entry is made `0755` regardless of the umask.

### 8.3. Rules

- A tag is stored when `S/versions/<tag>/d8` is a regular file, symlinks followed (`has`); a version is stored when a stored tag equals it (§7.1). Listing (`Store.List`) skips names that are not versions, files, empty directories, and `<tag>` directories that are symlinks.
- d8 writes an entry once per spelling: a stored tag is never downloaded or copied again (`install`), while another spelling of the same version gets an entry of its own (§14.17). d8 deletes no entry, so the store grows by one full binary per stored tag.
- A new entry is staged as `d8.staged`, made executable, smoke-tested and renamed into place, so a binary that fails its first smoke test never appears under its final name; the failure leaves the empty `<tag>` directory behind. An entry that fails a later smoke test (§9.1, step 6), for example one overwritten through X (§14.11), stays until it is deleted by hand (§14.7).
- `use` resolves its argument by version (`Store.Resolve`); `update --version`, seeding and downloads use the exact spelling.
- The active tag is the name of the parent directory of the target of `current`, when that name is a version, and otherwise there is none (`CurrentTag`). Neither the existence nor the content of the target is checked.
- An install is store-managed when X lies under S with the symlinks of S resolved (`Store.Contains`). Only then does the active tag define the active version.

### 8.4. Lock

`update` and `use` hold `S/install.lock` for the whole switch (`acquireLock` in [selfupdate/update.go](../internal/selfupdate/update.go), `Acquire` in [lockfile/lockfile.go](../internal/lockfile/lockfile.go)):

- The lock is an empty file created with `O_EXCL`. While it exists, another switch fails at once with `an update is already in progress (lock file <path> exists)`; there is no waiting.
- A lock file older than 1 h counts as left behind by a killed process. It is reclaimed with the log record `reclaiming a stale self-update lock`, a JSON line on stdout that gives the age in nanoseconds, and the switch proceeds.
- `status`, `check`, `versions`, completion and case 1 of `use` (§10.5) take no lock. The lock covers one store: it serializes neither the stores of different homes nor writers of the PATH entry other than d8 (§14.11).

## 9. Switching

### 9.1. Procedure

`SwitchTo` in [selfupdate/update.go](../internal/selfupdate/update.go) makes `<tag>` the active tag. `update`, and `use` with a download, pass it a fetch function; `use` of a stored or of the running version passes none.

1. On Windows, fail with `self-update is not supported on Windows; download the new d8 binary manually`. Nothing is created, but `update` and case 4 of `use` have contacted the cluster and printed their first line by then.
2. Fail when S is unavailable. Create S and acquire the lock (§8.4).
3. Decide whether the install is store-managed (§8.3).
4. For a plain-file install, seed the store: copy X to `S/versions/<running version>/d8` through the same staged write and smoke test as a download, when the running version is a version and no entry has exactly that spelling (`retain`). A failure is logged at debug level and ignored. A running version that is not a version, such as `dev`, is never stored.
5. When `<tag>` is not stored, fail with `version <tag> is not in the local store` if there is no fetch function, and otherwise download it into a new entry (§4.4, §8.3).
6. Run `S/versions/<tag>/d8 --version`. It MUST exit 0 within 30 s, otherwise the switch fails with `new binary failed its --version smoke test: <error>`, followed by ` (output: <output>)` when the binary printed anything, the output trimmed and cut to 200 bytes with `...`. The test runs for stored entries too, so a new entry is tested twice. Only the exit status counts (§14.6), and it becomes the exit status of d8 (§14.13).
7. Record the active tag as the previous tag.
8. Replace `current`: create `current.staged` pointing at `versions/<tag>/d8` and rename it over `current`.
9. For a plain-file install, migrate X: rename X to `X.old`, replacing an existing `X.old`, then create X as a symlink to `$HOME/.deckhouse-cli/cli/current`.
   - When the rename or the symlink fails with a permission error, the switch fails with the diagnostic `updating d8 needs write access to <directory of X>`, whose fixes are `re-run the command with sudo` and installing d8 in a user-writable directory.
   - When the symlink cannot be created, `X.old` is renamed back; when that fails too, X is missing and the error names `X.old` to restore by hand.
   - After a failed migration, d8 points `current` back at the previous tag, or removes it when there was none. When the previous tag is not stored any more, `current` stays on `<tag>`, and that is logged at debug level only (`restorePreviousCurrent`).
10. Release the lock.

Migration runs whenever X lies outside S. After another tool replaced the symlink with a plain file, the next switch migrates again (§14.9). A switch run from `X.old` migrates `X.old` itself, leaving `X.old.old`. X inside the store of another home is migrated as well (§14.10).

### 9.2. Failures

| Failing step | Store | `current` | X |
|---|---|---|---|
| 1 | nothing created | unchanged | unchanged |
| 2 | S may have been created | unchanged | unchanged |
| 5, 6 | the entry seeded in step 4 stays; a failed download leaves an empty `versions/<tag>/` | unchanged | unchanged |
| 8 | all entries stay | unchanged | unchanged |
| 9 | all entries stay | the previous tag, removed when there was none, or `<tag>` when the previous tag is not stored | unchanged, restored from `X.old`, or missing |

A failed `update` of a plain-file install therefore leaves the running version stored without migrating it.

### 9.3. Messages

After a successful switch the command prints (`printSwitchNotes` in [dist/cmd/use.go](../internal/dist/cmd/use.go)):

- when step 9 ran: `The d8 binary in PATH is now a symlink into the version store; the previous binary is kept with a ".old" suffix.`
- when step 7 found a previous tag: `Previous version <previous> remains installed - switch back with 'd8 dist use <previous>'.`

A first migration finds no previous tag and prints no such line, although step 4 stored the replaced version. A switch to the active tag names that tag as its own predecessor (§14.8).

## 10. Commands

| Command | Purpose | Cluster |
|---|---|---|
| `d8 dist status` | the active version, the installed plugins, and what is outdated | optional: without it only the local part |
| `d8 dist check` | whether a newer stable version is published | needed |
| `d8 dist versions` (`list`) | the published versions, newest first | needed |
| `d8 dist update [--version X]` | install the latest version, or X, and make it active | needed, except a stored X with an explicit endpoint (§10.4) |
| `d8 dist use <version>` | make a version active, downloading it only when it is not stored | only for a download |

The commands live under `d8 dist` ([dist/cmd](../internal/dist/cmd/)). `d8 dist` alone prints its help, and an extra argument is an error. Diagnostics of a failing command go to stderr; regular output and log records, which are JSON lines controlled by `LOG_LEVEL`, go to stdout. Output colours are dropped when stdout is not a terminal, `NO_COLOR` is set or `TERM` is `dumb`.

### 10.1. `d8 dist status`

`collectSummary` in [dist/cmd/status.go](../internal/dist/cmd/status.go) gathers the local state, then asks the cluster, and `renderSummary` prints the whole summary at the end:

1. The line `  Version:  <active version>`; the command help calls it the running version, which differs as §7.2 describes. Plugins are read from the plugins root that holds an install, the one of `DECKHOUSE_CLI_PATH` (by default `/opt/deckhouse/lib/deckhouse-cli`) or `~/.deckhouse-cli` ([plugins.md](plugins.md)); none gives `Plugins: none installed`.
2. It builds the client and lists the tags. Success adds `  Latest:   <latest>  update available - run 'd8 dist update'`, or `  Latest:   <latest>  up to date`.
3. When plugins are installed, each row gets the highest stable published version of the plugin, of any major ([plugins.md](plugins.md#15-known-deviations) §15.12), and the verdict `update available`, `up to date`, or `unknown` when that lookup failed. An installed version that does not parse, such as `ERROR`, counts as `up to date`.

Any failure of step 2, a registry without a stable version included, or of setting up the plugin services in step 3, ends the cluster part: the summary then ends with `Warning: could not check for updates - cluster unreachable.` and the error in parentheses (§14.4), and the plugin table has no `LATEST` and `STATUS` columns. The command always exits 0, also without a kubeconfig.

### 10.2. `d8 dist check`

`check` lists the tags and compares latest with the running version (§7, [dist/cmd/check.go](../internal/dist/cmd/check.go)):

- when latest is newer: `A newer deckhouse-cli is available: <latest> (current: <running>). Run 'd8 dist update' to upgrade.`
- otherwise: `deckhouse-cli is up to date (<running>).`

Both cases exit 0, so the exit status does not tell whether an update exists. Without a stable version the command fails with `no released deckhouse-cli versions found`.

### 10.3. `d8 dist versions`

`versions`, alias `list`, prints every version newest first, pre-releases included (`formatVersionList` in [dist/cmd/versions.go](../internal/dist/cmd/versions.go)). A line holds the version, padded to the widest one, and its relation to the active version: `* <v>  current` for the active version, `  <v>  newer` above it and `  <v>` below it; `  installed` is appended when the version is stored. Every published spelling gets a line of its own, so all spellings of the active version are marked `current` (§14.17). When the active version is not a version, such as `dev`, the lines carry no relation. Then follow:

- `Installed locally (switch with 'd8 dist use'), not published in the registry:` and the stored versions missing from the list, when there are any;
- `Current version <active> is not published in the registry.`, when the active version is not in the list.

When no tag is a version, the command fails with `no deckhouse-cli versions found in the registry`.

### 10.4. `d8 dist update`

`update [--version X]` ([dist/cmd/update.go](../internal/dist/cmd/update.go)):

1. Build the client (§4). This needs a kubeconfig that parses, and the API server unless the endpoint is explicit, even when nothing is downloaded.
2. Without `--version`, or with an empty one, list the tags. When latest is not newer than the running version, print `deckhouse-cli is already up to date (<running>).` and exit 0; otherwise X is latest.
3. Print `Updating deckhouse-cli to X...` and switch to X with downloads allowed (§9.1). A tag stored under exactly the spelling X is not downloaded again, while another spelling of the same version is (§14.17).
4. Print `✓ deckhouse-cli updated to X.` and the notes of §9.3.

With `--version` there is no listing and no "already up to date" check, and X is fetched as typed (§7.3).

### 10.5. `d8 dist use`

`use <version>` takes an argument that MUST parse as a version, otherwise it fails with `invalid version "<arg>": ...`. The first case that matches applies (`newUseCommand` in [dist/cmd/use.go](../internal/dist/cmd/use.go)):

| # | Case | Action | Output | Cluster |
|---|---|---|---|---|
| 1 | the install is store-managed and the active tag equals the argument | nothing, not even a smoke test | `deckhouse-cli is already at <tag>.` | not needed |
| 2 | a stored version equals the argument | switch to its tag (§9.1) | `✓ Switched deckhouse-cli to <tag> (installed locally).` | not needed |
| 3 | the running version equals the argument | switch to it, seeding it first in a plain-file install | `✓ Switched deckhouse-cli to <running> (taken from the running binary).` | not needed |
| 4 | otherwise | download the argument as typed and switch | `Version <arg> is not installed locally, downloading...`, then `✓ Switched deckhouse-cli to <arg>.` | needed |

- Equality is version equality (§7.1), so `use 0.14` selects a stored `v0.14.0`, while case 4 sends the argument unchanged (§7.3).
- Case 3 seeds only a plain-file install. In a store-managed install whose running version is not stored under that spelling, it fails with `version <running> is not in the local store`, without a download.
- Cases 2 to 4 also print the notes of §9.3.

### 10.6. Completion

`d8 dist use <TAB>` offers the stored versions, newest first, whose stored spelling starts with the typed text, each described as `installed locally, switches offline` (`completeStoredVersions` in [dist/cmd/use.go](../internal/dist/cmd/use.go)). It reads only S. Because it matches the spelling, `0.1<TAB>` offers nothing for entries named `v0.1…`, although `use 0.13.1` resolves them. Shell completion itself is installed with `d8 completion`.

## 11. Flags and environment variables

The flags, except `--version`, are persistent on `d8 dist` and are inherited by every subcommand, `d8 dist plugins` included (`AddFlags` in [rpp/flags/flags.go](../internal/rpp/flags/flags.go), `AddKubeFlags` in [plugins/flags/flags.go](../internal/plugins/flags/flags.go)). An environment default is read when d8 starts, and the flag overrides it.

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `--kubeconfig`, `-k` | `KUBECONFIG` | `~/.kube/config` | kubeconfig file, or a `:`-separated list of files (§4.1) |
| `--context` | | the current context | kubeconfig context (§4.1) |
| `--rpp-endpoint` | `D8_RPP_ENDPOINT` | discovery | proxy base URL (§4.2) |
| `--rpp-ca-file` | `D8_RPP_CA_FILE` | the system roots only | PEM bundle added to the system roots for the proxy (§4.3) |
| `--insecure-skip-tls-verify` | | off | no TLS verification on either connection, for debugging only (§4.3) |
| `--version`, `update` only | | latest | exact version to install (§7.3) |
| | `HOME` | | location of S, an absolute path (§8.1) |
| | `DECKHOUSE_CLI_PATH` | `/opt/deckhouse/lib/deckhouse-cli` | plugins root that `status` reads (§10.1) |
| | `HTTPS_PROXY`, `NO_PROXY` | | forward HTTP proxy, unless the kubeconfig sets `proxy-url` (§4.3) |
| | `LOG_LEVEL` | `info` | `debug` logs the endpoint and how it was found, the requests, and why the running version was not stored |
| | `NO_COLOR`, `FORCE_COLOR`, `TERM` | | `NO_COLOR` and `TERM=dumb` disable colours; `FORCE_COLOR` forces them in diagnostics |

## 12. Errors and troubleshooting

The request of a proxy error appears in the error chain, for example `╰─▶ GET /v1/images/deckhouse-cli/tags`.

| Message | Cause | Remedy |
|---|---|---|
| `registry-packages-proxy: unauthorized (401)` | the kubeconfig carries no token or one the cluster rejects, for example a client-certificate kubeconfig (§5.2) | use a token kubeconfig (§3, item 1) |
| `registry-packages-proxy: forbidden (403)` | `cli-download` is not bound to the identity, or a denial from before the binding is cached (§5.2) | bind it (§3); a cached denial clears within 30 s (§14.2) |
| `registry-packages-proxy: version not found (404)` for `GET /v1/images/deckhouse-cli/tags` | no candidate root holds `deckhouse-cli` (§5.4), or the endpoint carries a query or a fragment (§14.16) | publish or mirror the CLI (§6); for a `cse` registry see §14.12 |
| `registry-packages-proxy: version not found (404)` for `GET /v1/images/deckhouse-cli/images/<tag>` | the tag is not published, is spelled differently, or its index has no image for this platform (§5.5) | use the spelling that `d8 dist versions` prints (§14.1); check the platforms of the index (§14.3) |
| `registry-packages-proxy: upstream error (5xx)` | the proxy failed to reach the registry or was refused by it (§5.4), has no registry configuration, does not answer, or kube-rbac-proxy failed to authorize the request (§5.2) | retry; check the `registry-packages-proxy` pods in `d8-cloud-instance-manager` and the registry credentials |
| `registry-packages-proxy: endpoint discovery via the Kubernetes API failed` | the API server is unreachable or its certificate is not trusted; the API rejects the identity (§14.5); the identity may not `get` the Ingress, in which case the last line of the chain contains `is forbidden: User "<name>" cannot get resource "ingresses"` (§14.5); or the Ingress is missing and the identity may not `list` pods, or no proxy pod is ready | fix the cause (§3, item 3), or skip discovery with `--rpp-endpoint https://registry-packages-proxy.<publicDomain>` |
| `x509: certificate signed by unknown authority` from the endpoint | the proxy certificate does not chain to a system root | `--rpp-ca-file <ca.pem>` |
| `x509: certificate is valid for <names>, not <name>` from the endpoint | the kubeconfig sets `tls-server-name` (§14.15), or the endpoint host is not in the proxy certificate | remove `tls-server-name`, or use the host of the Ingress |
| `x509: cannot validate certificate for <IP> because it doesn't contain any IP SANs` | the endpoint is a node or pod IP (§4.2, §5.1) | `--rpp-endpoint` with the Ingress host |
| `dial tcp <IP>:4219: ...` | a pod endpoint, unreachable from outside the cluster network | `--rpp-endpoint` with the Ingress host |
| `unexpected status 3xx: ...` | the endpoint redirects, for example to a login page or to another host | give the final `https` URL |
| `invalid proxy endpoint: ...` | `--rpp-endpoint` is not an absolute `https` URL with a host | fix the value |
| `unsupported client configuration: insecure TLS verification and a CA bundle are mutually exclusive` | `--insecure-skip-tls-verify` together with `--rpp-ca-file` or `D8_RPP_CA_FILE` | drop one of them |
| `invalid CA bundle: no certificates parsed from CA data` | the CA file is not empty but holds no PEM certificate | fix the file |
| `set up kubernetes client: reading kubeconfig file: ...` | no usable kubeconfig | §4.1 |
| `file not found in image: "d8"` | the last layer of the image holds no `d8` (§5.5) | fix the image (§6) |
| `new binary failed its --version smoke test: ...` | the binary does not run here (another platform, corrupt, missing libraries), or a stored entry is broken; d8 exits with the status of that binary (§14.13) | for a stored entry, delete `S/versions/<tag>` and retry (§14.7) |
| `an update is already in progress (lock file <path> exists)` | another `update` or `use` is running, or one was killed less than 1 h ago | wait, or delete the lock file when no d8 is running |
| `updating d8 needs write access to <dir>` | migration cannot rename or replace X (§9.1, step 9) | keep d8 in a directory the user can write; read §14.10 and §14.11 before re-running with `sudo` |
| `invalid version "<arg>": ...` | the argument of `use` is not a version (§7.1) | §7.3 |
| `tag "<X>" is not a semver version: invalid semantic version` | `--version` is not a version | §7.3 |
| `download new binary: invalid image reference: "<X>" is not a valid image tag` | `--version` holds characters that a tag cannot, such as build metadata | §7.3 |
| `version <tag> is not in the local store` | `use` of the running version in a store-managed install that does not store it (§10.5) | `d8 dist update --version <tag>` |
| `version store unavailable: ...` from `use`, `version store is unavailable ...` from `update` | `HOME` is not set (§8.1) | set `HOME` to an absolute path |
| `self-update is not supported on Windows; ...` | the client runs on Windows | replace the binary by hand |
| `no released deckhouse-cli versions found` | the registry has pre-releases only | `update --version <pre-release>` |
| `no deckhouse-cli versions found in the registry` | no tag is a version | §6 |
| `Warning: could not check for updates - cluster unreachable.` from `status` | any failure of the cluster part, not only an unreachable cluster (§10.1, §14.4) | `d8 dist check` shows the diagnostic |
| `Version <v> is not installed locally, downloading...` for a version stored before | the command uses another store, of another user or `$HOME` (§8.1), or the entry was deleted | run as the user who stored it |

`deckhouse-cli is already up to date (<v>).` and `deckhouse-cli is already at <v>.` are not errors; `update --version X` installs another version.

## 13. Example

A workstation runs d8 v0.13.1 as a plain file at `/home/alice/.local/bin/d8`, without plugins; the registry publishes `v0.13.0`, `v0.13.1`, `v0.14.0` and `v0.15.0-rc.1`:

```console
$ d8 dist status
deckhouse-cli (d8)
  Version:  v0.13.1
  Latest:   v0.14.0  update available - run 'd8 dist update'

Plugins: none installed
Install with 'd8 dist plugins install <name>'.

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

The proxy received `GET /v1/images/deckhouse-cli/tags` from `status`, `check`, `update` and `versions`, and `GET /v1/images/deckhouse-cli/images/v0.14.0?platform=linux-amd64` from `update`; `use` sent nothing. Afterwards:

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

Verified on 2026-10-01 by running the `d8 dist` command tree of this revision against a fake registry-packages-proxy and Kubernetes API, item 13 also with a `task build` d8 and item 14 with the release toolchain (Go 1.26.5), unless an item says otherwise. Each item breaks a rule above, a promise of the command help, or a statement of the module documentation, or gives a misleading message.

1. Versions are fetched as typed, although §7.1 makes two spellings of one version equal. `use` resolves stored versions by value but downloads its argument unchanged (`requested.Original()` in [dist/cmd/use.go](../internal/dist/cmd/use.go)): with `v0.15.0-rc.1` published, `d8 dist use 0.15.0-rc.1` requested `/images/0.15.0-rc.1` and failed with `version not found (404)`, and `update --version 0.14.0` failed the same way. The `v` prefix is optional for stored versions only.
2. The `403` diagnostic names the wrong wait. It says `authorization is cached ~5 min - after binding, retry with a fresh token`, while kube-rbac-proxy caches a denial for 30 s; 5 min is how long an allowed decision is cached, which delays a revocation, not a grant (§5.2). The module documentation states 30 s. The durations come from the kube-rbac-proxy source.
3. Every `404` is diagnosed as an unpublished version, with the advice to run `d8 dist versions`. A `404` of the tag listing, from a registry without a `deckhouse-cli` repository, makes that command fail the same way, and a `404` caused by an index without an image for the client's platform concerns a version that `d8 dist versions` lists.
4. `status` calls every failure `cluster unreachable`. The `401`, `403`, `404`, `5xx` and `3xx` answers of a reachable proxy, and a registry without a stable version, all produced `Warning: could not check for updates - cluster unreachable.`, with the real error only in parentheses.
5. A missing permission on the Ingress is diagnosed as an API server problem. `cli-download` does not include it (§3), so discovery by an identity with that role alone ends in `403`, and the diagnostic gives the cause `that server was unreachable or presented an invalid certificate` and no word about the permission; `is forbidden` appears only in the error chain. A `401` of the API server during discovery gets the same cause.
6. The smoke test does not check the version. A binary that prints `d8 version v0.15.0-rc.1`, served under the tag `v0.14.0`, was stored and activated as `v0.14.0`: `check` and `d8 --version` then report v0.15.0-rc.1, while `status`, `versions` and `use` report v0.14.0.
7. A broken store entry blocks its version for good. An entry that fails the smoke test is neither replaced nor removed: `update --version v0.14.0` and `use v0.14.0` failed with `exec format error` without any download, and `versions` kept marking v0.14.0 `installed`, until `S/versions/v0.14.0` was deleted by hand.
8. `update --version <active>` switches to the active tag again and prints, for the active v0.13.0, `Previous version v0.13.0 remains installed - switch back with 'd8 dist use v0.13.0'.`
9. Migration discards earlier backups. Every migration renames X over `X.old`: after another tool had replaced the symlink with a v0.13.0 file, the next `use` made that file `d8.old`, and the original v0.13.1 backup was gone; v0.13.1 survived only as the store entry seeded by the first migration.
10. A run under another `$HOME` corrupts the first store. When the PATH entry points into the store of one home and d8 runs with another `$HOME`, for example under `sudo` (§8.1), X lies outside the store in use, so the switch treats the install as plain-file and migrates X, which is an entry of the first store. With the first store at v0.14.0, `d8 dist use v0.13.0` run with a second `$HOME` downloaded v0.13.0 into the second store, renamed `versions/v0.14.0/d8` of the first store to `d8.old` and replaced it with a symlink to the `current` of the second store, so the first user's d8, still at the active tag v0.14.0, ran v0.13.0 from then on. By §8.1, a plain-file install migrated under `sudo` also points the PATH entry into root's home, which other users usually cannot traverse. The diagnostic of §9.1, step 9, nevertheless suggests `re-run the command with sudo`.
11. The platform installer writes through a migrated `/opt/deckhouse/bin/d8`. On nodes, bashible installs d8 from the registry package `d8` with `cp -f d8 /opt/deckhouse/bin` and `chmod u+s /opt/deckhouse/bin/d8`, and does it again whenever the package digest changes ([install](https://github.com/deckhouse/deckhouse/blob/main/modules/007-registrypackages/images/d8/scripts/install)). That file is setuid root, for access to the plugins directory; migrating it replaces it with a symlink to a store entry of mode `0755`, so d8 stops running setuid. At the next reinstall, reproduced with GNU coreutils 9.4, `cp -f` writes through the symlinks into the active store entry, and `chmod` makes that entry setuid for its owner, so the entry no longer holds the version of its tag: `status`, `versions` and `use` report the old tag, `check` and `d8 --version` the new binary. When the entry is being executed at that moment, `cp -f` gets `ETXTBSY`, removes the symlink and writes a plain file instead, which the next switch migrates again (§14.9).
12. `cse` registries are not found. `pkg.Edition` includes `cse` ([pkg/edition.go](../pkg/edition.go)), so `d8 mirror push` to `…/deckhouse/cse` puts the CLI at `…/deckhouse/deckhouse-cli` ([mirror-bundle-layout.md](mirror-bundle-layout.md#82-edition-split) §8.2), while the proxy strips only `ce`, `be`, `se`, `se-plus`, `ee` and `fe` (§5.4) and looks only at `…/deckhouse/cse/deckhouse-cli`. A CSE cluster filled that way answers `404` to every self-update request. Established from the code of both sides, not by running.
13. A failed smoke test sets the exit status of d8, against §4.5. `execute` in [cmd/d8/root.go](../cmd/d8/root.go) exits with the `ExitCode()` of any error in the chain, and the smoke test wraps the `*exec.ExitError` of the tested binary (§9.1, step 6). With a stored entry that prints `boom` and exits 7, `d8 dist use` printed `new binary failed its --version smoke test: exit status 7 (output: boom)` and exited 7; with an entry that hangs, it printed `signal: killed` and exited 255.
14. The response-header timeout does not apply over HTTP/2 in release builds, against §4.4. `withTunedTransport` in [rpp/transport.go](../internal/rpp/transport.go) sets `ResponseHeaderTimeout` on a clone of the base transport, but when client-go builds that base itself, for an exec plugin such as `d8 login get-token`, a client certificate, `tls-server-name`, `proxy-url` or `--insecure-skip-tls-verify`, the HTTP/2 upgrade of `golang.org/x/net`, which builds with Go before 1.27 use, reads the timeout of the original base, which is 0. Against a server that delayed its headers for 34 s, HTTP/1.1 failed after 30 s with `net/http: timeout awaiting response headers`, while HTTP/2 with an exec plugin or `--insecure-skip-tls-verify` succeeded after 34 s. With Go 1.27 every case timed out.
15. The `tls-server-name` of the kubeconfig breaks the proxy connection. `buildHTTPClient` resets only the CA and `insecure-skip-tls-verify` of the kubeconfig (§4.3), so a server name meant for the API server is used to verify the proxy. With `tls-server-name: kubernetes` and the proxy certificate in `--rpp-ca-file`, a request failed with `x509: certificate is valid for example.com, *.example.com, not kubernetes`, and it succeeded without that field.
16. The endpoint check accepts URLs that cannot work. `validateBaseURL` checks only the scheme and the host (§4.2). `https://<host>/prefix?x=1` sent `GET /prefix?x=1/v1/images/…`, and `https://<host>/prefix#frag` sent `GET /prefix`; both were diagnosed as `version not found (404)`. `https://user:pw@<host>` sent `Authorization: Basic …` instead of the token of the kubeconfig.
17. Another spelling of a stored version is downloaded and stored again, although §7.1 makes the spellings equal. `has` and `install` compare entry names, not versions (§8.3): with `0.14.0` stored, `update --version v0.14.0` downloaded `v0.14.0` into a second entry, and a plain-file v0.14.0 was seeded as `versions/v0.14.0` next to `versions/0.14.0`. With `0.14.0`, `v0.14.0` and `v0.14` published, `versions` marked all three lines `current` and `installed`, although one entry was stored.
18. The module documentation misdescribes three points. It says that the images route returns the image "with flattened layers", while only package icons are flattened and the CLI routes serve the last layer (§5.5). It lists `HEAD` for that route, which kube-rbac-proxy checks with an empty verb that `cli-download` does not grant (§5.2). Its `can-i` checks impersonate a user without `--as-group`, so they answer `no` for the group binding it shows (§3). Established from the code, not by running.
