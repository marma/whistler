# Securing external S3 access

**Open, written 2026-10-10.** What has to be true for a managed dataset's
external listener ([storage.md](storage.md), "External access") to be safe to
expose, and what is missing today. Nothing here is built yet; it is the list to
come back to.

The listener itself is sound: a separate VersityGW process per dataset with
its own account store (`xro.*`/`xrw.*` only), reached through one Ingress per
dataset at `/<dataset>`, unrewritten. What this document is about is the path
*to* it: TLS, proxies in front of the cluster, and the per-dataset address
restriction (`Dataset.spec.externalSources`).

## TLS

**Today nothing gives the external host a real certificate.** With
`datasetServer.external.tlsSecretName` empty, the Ingress has no `tls:` block
and Traefik answers `https://` with its self-signed default certificate (issued
for a `*.traefik.default` name). The portal still tells users
`scheme: https`.

How that fails is worth knowing because it is silent: **rclone appears to
hang.** The real error is
`x509: certificate is valid for …traefik.default, not <host>`, but the AWS
SDK's backoff times rclone's default 10 low-level retries × 3 retries turns it
into minutes of nothing. `--low-level-retries 1 --retries 1 -vv` shows it at
once; `--no-check-certificate` makes it work and is not an answer.

Ways to close it, any one of which is enough:

- **A TLS Secret for the host** in the release namespace, named by
  `tlsSecretName`. The cluster has no cert-manager, so this is a certificate
  the admin brings (a public one, or an internal CA the clients trust).
- **TLS at a proxy in front of the cluster** (next section): leave
  `tlsSecretName` empty and `scheme: https`, and let the proxy present the
  certificate.
- **`scheme: http`**, for a dev LAN only. Requests stay SigV4-signed, so a key
  is not sent in the clear, but the data is, and a captured request can be
  replayed within its signing window.

To do: the portal should warn when it hands out an `https://` endpoint while
`tlsSecretName` is empty, since it cannot know whether a proxy is supplying
TLS. At least the admin should be told. Better: a setting saying "TLS is
terminated upstream" so the warning can be specific.

## Behind a TLS-terminating proxy or tunnel

This works without code changes, provided the proxy passes two things through
**unchanged**:

- **`Host`.** SigV4 signs it. A proxy that rewrites it to an internal name
  (nginx `proxy_pass http://upstream;` without `proxy_set_header Host $host;`)
  produces `SignatureDoesNotMatch` on every request.
- **The path.** No prefix stripping, re-encoding or `//` collapsing. The
  signature covers the path, and the dataset's bucket *is* the first path
  segment. This is the same reason the Ingress does not rewrite it.

Also check, for large transfers: request body limits (rclone's multipart parts
default to 5Mi, well under e.g. Cloudflare's 100MB per request), whether the
proxy buffers whole bodies, and read timeouts on long uploads and downloads.

The scheme is not signed, so `https` outside and `http` from the proxy to the
Ingress is fine. The Ingress already serves port 80.

## The address restriction does not survive a proxy

**This is the real gap.** `externalSources` ("Only these addresses") is
enforced at the ingress controller, because a NetworkPolicy on the listener
only ever sees the controller. With `sourceFilter: traefik`, Whistler creates
an `ipAllowList` Middleware **without an `ipStrategy`**, so Traefik matches the
address of the TCP connection it receives. Behind a proxy or tunnel that
address belongs to the proxy, never the client:

- an allow-list that does not include the proxy shuts out **everyone**;
- an allow-list that does include it lets **everyone** in.

"Anywhere" and "Nowhere" are unaffected; only the restricted mode depends on
seeing the client. Until this is fixed, **a dataset behind a proxy must use
Anywhere or Nowhere**, and the chart comment's requirement
(`externalTrafficPolicy: Local`, the controller must see real client
addresses) has to be read as "and no proxy in front".

### What the fix needs

Reading the client from `X-Forwarded-For` is only safe if a client cannot
supply that header itself. Both halves are required:

1. **Trust the header only from the proxy.** Traefik's entrypoint
   `forwardedHeaders.trustedIPs` set to the proxy's addresses. Without it,
   anyone can send `X-Forwarded-For: <an allowed address>` and walk through the
   filter, which is worse than the current state because it *looks* enforced.
2. **Tell the Middleware which hop is the client.** An `ipStrategy` on the
   `ipAllowList`: `depth: 1` for exactly one trusted proxy, or `excludedIPs`
   listing the proxies when the chain length varies.

Step 1 is configuration of the cluster's Traefik, not of anything Whistler
renders, so Whistler cannot do it on its own. What Whistler can do is add a
chart setting (e.g. `datasetServer.external.trustedProxies`) that

- renders the matching `ipStrategy` into each dataset's Middleware, and
- is documented as useless, and dangerous, unless the entrypoint trusts the
  same addresses. If the operator can read Traefik's static config, it could
  check this; otherwise the README has to say so loudly.

For `sourceFilter: nginx` the counterpart is `use-forwarded-headers` (or
`use-proxy-protocol`) plus `set-real-ip-from` on the controller, and
`whitelist-source-range` then sees the real client. Same rule: the controller
must trust only the proxy.

A **tunnel** (cloudflared and the like) usually adds its own header
(`CF-Connecting-IP`) and connects from inside the network. The same trust
argument applies: the header is believable only on connections from the
connector, and only if nothing else can reach the ingress.

Alternatively, do the filtering where the client's address is known: at the
proxy or in the tunnel's access policy. Then `externalSources` is
"Anywhere" from Whistler's point of view and the restriction is the edge's.
This is a legitimate deployment, but then the portal's per-dataset setting
says nothing true, and that should be visible to the admin, not just assumed.

## Checklist before exposing external S3

- [ ] The external host has a certificate clients trust (Secret, or upstream
      proxy).
- [ ] Any proxy in front preserves `Host` and the path exactly.
- [ ] If any dataset uses "Only these addresses": the controller sees real
      client addresses (no proxy, `externalTrafficPolicy: Local`), **or** the
      trusted-proxy setting above exists and the entrypoint trusts only the
      proxy.
- [ ] The external listener is reachable only through the Ingress (the
      `-external` NetworkPolicy admits ingress-controller peers only;
      `whistler.datasetServer.external` peers are right for this cluster).
- [ ] Nothing but the proxy can reach the ingress controller directly, if the
      proxy is where filtering or TLS happens.
