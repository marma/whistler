# ProxyJump gateway: real SSH addressing, TUI as a launcher

Status: implemented (2026-08-10), except that **pods have no sshd yet and are
therefore not reachable over SSH** — the exec bridge that used to serve them
is gone. Creation-from-a-connection was built and then deliberately removed;
it belongs in the launcher. See
[Implementation phases](#implementation-phases). Original design 2026-08-08.

## Problem

SSH has no SNI/Host-header equivalent: a client connecting to a wildcard DNS
name never transmits the name it dialed, so a single gateway IP cannot
distinguish "ssh to instance A" from "ssh to instance B" by the connection
alone. Whistler currently smuggles the target through the SSH **username**
(`marma-vm-test` → user `marma`, instance `vm-test`), parsed in
`SSHServer._resolve_target` ([server.py](../whistler/server.py)). It works,
but it is ugly, collides with real usernames containing `-`, and every
capability (shell, SFTP, port forwards, agent forwarding) must be re-bridged
by hand through `kubectl exec` + injected socat.

Alternatives considered and rejected:

- **One routable IP per instance** (LoadBalancer/MetalLB): user-facing IPs
  instead of names, an address-pool to manage, one TOFU host-key prompt per
  instance. No.
- **IP per instance + DNS sync** (external-dns / delegated zone): fixes the
  name, keeps the per-instance IP cost, adds out-of-cluster infrastructure
  and propagation lag between "created" and "resolvable", and forces global
  name uniqueness (or per-user subdomains).

## Chosen design

There **is** one protocol-native place where the destination name travels to
a server we control: the jump-host mechanism. `ssh -J gw instance` makes the
client open a `direct-tcpip` channel on the gateway whose open request
carries the destination hostname **as a literal string** — it never needs to
resolve in DNS. AsyncSSH surfaces exactly this as
`SSHServer.connection_requested(dest_host, dest_port, ...)`, which the server
already implements today for localhost port-forwards (server.py:410).

So the existing AsyncSSH gateway becomes a jump host:

- `ssh marma@ssh.example.com` → interactive session on the gateway → the
  (slimmed-down) TUI. The username is just the username; no encoding.
- `ssh marma@test.w` (with a one-time client config stanza) → the client
  authenticates to the gateway as `marma`, then asks it for a tunnel to
  `test.w`. The gateway resolves `test` **in marma's namespace** and splices
  the channel to the instance's sshd port. End-to-end SSH: the crypto
  terminates in the instance, and scp/rsync/`-L`/`-R`/agent forwarding /
  VS Code Remote-SSH / JetBrains Gateway all work natively with zero
  Whistler-specific bridging.

Client-side UX (printed by the TUI / portal on first use):

```
# ~/.ssh/config
Host whistler-gateway
    HostName ssh.example.com
    User marma
    AddKeysToAgent yes
    ControlMaster auto
    ControlPath ~/.ssh/cm-%C
    ControlPersist 10m

Host *.w
    ProxyJump whistler-gateway
    User marma
    AddKeysToAgent yes
    ControlMaster auto
    ControlPath ~/.ssh/cm-%C
    ControlPersist 10m
```

Then: `ssh test.w`, `scp file test.w:`, `code --remote ssh-remote+test.w`.

The agent and multiplexing lines are not decoration. **A jump is two logins**
— the gateway authenticates the user, then the instance authenticates them
again — so an encrypted key with no agent prompts for its passphrase *twice
per connection*, and VS Code Remote opens several connections. `AddKeysToAgent`
makes that one prompt ever; `ControlMaster` then reuses the open connections.
The gateway cannot collapse the two logins: authenticating the user is the
access-control decision the whole SSH plane rests on.

One consequence worth knowing: `ControlPersist` holds the connection open
after the last session exits, which delays the "last spliced channel closed"
signal that ephemeral instances are cleaned up on (below).

Key properties:

- **Name scoping falls out for free.** The gateway authenticates the user
  *before* it sees the destination name, so `test` is looked up only among
  that user's instances. Names need only be unique per user — which they
  already are (instances live in per-user namespaces). No global namespace,
  no DNS.
- **One public address**, the one that already exists. No new infrastructure.
- **On-demand creation survives.** `connection_requested` fires before any
  splice; if the name matches a template rather than an instance, the
  gateway creates an ephemeral instance (same CR-declare → operator-reconcile
  path as today: `add_instance` / `_trigger_reconcile`, then
  `_watch_pod_ready`), waits for sshd, then accepts the channel. The client
  just sees a slow connect. `ssh marma@ubuntu.w` = "give me an ubuntu, I'll
  wait" — the current `user-<template>` flow minus the username hack.
- **The suffix (`.w`) is client-side convention only.** It exists so the
  `Host *.w` stanza can match; the gateway strips any configured suffix from
  `dest_host` before lookup and otherwise treats the name verbatim. Suffix
  string is a chart value (default `.w`; admins who own a domain can use
  `.vm.example.com` for cosmetics — still no DNS records needed).

### Routing rules in `connection_requested`

Strict, fail-closed:

1. Strip the configured suffix from `dest_host` if present.
2. Look the name up among the sessions the authenticated user **may reach**
   (today: the sessions in their own namespace, ssh-mode and desktop alike);
   if not found, among the user's templates (→ ephemeral create path).
   "May reach", not "owns" — see [Membership, not
   ownership](#membership-not-ownership).
3. Only the instance's SSH port is reachable through this mechanism
   (`dest_port` must be 22 / the advertised port). Everything else →
   `OPEN_ADMINISTRATIVELY_PROHIBITED`. The gateway must never become a
   generic TCP relay.
4. The session's **zone must permit the requested SSH posture**
   ([SSH is an egress channel](#ssh-is-an-egress-channel-the-zone-fence-cannot-see)).
5. The existing `localhost`/`127.0.0.1` branch (port forwards into the
   active bridged instance) is only relevant to gateway-terminated sessions;
   see "What gets deleted" — it goes away with the exec-bridge. ProxyJump
   users' `-L`/`-R` forwards are handled by the *instance's* sshd on the
   end-to-end connection and never touch the gateway hook.

Ephemeral lifecycle: today "disconnect" is the end of the bridged session
(`_cleanup_ephemeral`). Under ProxyJump the equivalent signal is "last spliced
`direct-tcpip` channel to that instance closed" — the gateway tracks open
splices per instance and starts the (grace-period) cleanup when the count
drops to zero.

## What the security model changes

[design/security.md](security.md) landed after this design was first written.
Most of it is orthogonal — it is about storage — but four things bear on the
SSH plane, and one of them is a hole this document has to own rather than
inherit.

### The splice set is the access-control decision

security.md's organising idea for storage is that **the set of exports
published to an instance is the access-control decision**: identity is a
property of which export a write goes through, not of anything the client
asserts. The SSH plane gets the exact analogue, and it is worth stating in
the same words: **the set of instances the gateway will splice a channel to
is the access-control decision.** A user cannot reach a session pod by any
other route — the baseline NetworkPolicy admits only the gateway (and the
portal) into a user namespace — so a refusal in `connection_requested` is
server-side enforcement, not a guest-side courtesy. That makes the gateway
trusted infrastructure in the same sense the storage gateway is.

It also answers, for the SSH half, security.md's open question *"who may
connect to the session at all and how that is enforced (the SSH/console
path, not the gateway)"*. The answer is: the Whistler SSH gateway, in
`connection_requested`, before any bytes move.

### SSH is an egress channel the zone fence cannot see

This is the hole. A zone is a set of **egress** NetworkPolicies on the
session's pod. An inbound SSH connection is ingress, and the bytes flowing
back to the client ride that same established TCP connection — no egress
rule is ever consulted. So a user with an interactive login into a
restricted-zone session can `cat`, `scp`, `rsync` or `tar |` anything out of
it to their laptop, and the fence sees none of it.

security.md's driving scenario is that restricted material "must never be
reachable from a session that also reaches the internet". The egress fence
alone does not deliver that while an interactive login exists: the session
reaches the internet *through the person at the keyboard*.

ProxyJump does not introduce this — today's exec-bridge already serves a
shell and SFTP into any instance regardless of zone. What ProxyJump changes
is only the cost: native `rsync` at line rate instead of a bridged channel.
But this is the document where the SSH plane gets designed, so this is where
it gets stated.

There are two coherent readings, and a deployment has to pick one per zone
*and per group of users* rather than the code picking one globally:

- **The fence is about the machine.** The researcher is authorised to see
  the material; the zone stops the *software* on the instance from phoning
  home — a compromised dependency, telemetry, an LLM client — and stops bulk
  automated egress. Under this reading interactive SSH is fine.
- **The fence is about the data.** Then interactive access has to be
  constrained too, and the gateway is the only place that can do it.

### SSH is one of five doors

The full treatment now lives in
[design/security.md](security.md#access-channels-the-second-axis), because
this is not an SSH problem — it is the second of four axes that together make
up the border, and SSH is simply the door this document happened to be
standing in. The short version:

- The channels a person can move bytes through are end-to-end SSH, the relay,
  the portal web terminal, the **desktop clipboard**, and **screenshots**.
  Closing the first three and calling a zone contained would be wrong: the
  clipboard is bidirectional and on by default, and the portal grabs and
  serves full-resolution screenshots of every desktop session.
- The setting therefore wants to be a **channel set**, carried on the zone as
  a *ceiling* and narrowed by a per-user or per-group grant — because the
  internal helper and the external researcher meet in the same zone, on the
  same instance, and must not get the same doors.
- Whistler owns none of the fourth axis, the endpoint. A thin client is a
  real control and a real part of the answer, but it is the deployment's,
  not Whistler's, and nothing here may lean on it.

### What is actually implemented: `Zone.spec.ssh`

A first cut that names one channel, because it is the one the gateway needed:

| posture | what the gateway permits | is it a boundary? |
| --- | --- | --- |
| `direct` (default) | ProxyJump splice — full end-to-end SSH: scp, sftp, rsync, `-L`/`-R` | n/a, this is the open case |
| `relay` | gateway-mediated PTY only (the TUI handover); no SFTP subsystem, no port forwarding | **no** — friction only |
| `none` | no SSH; portal console/desktop only | for SSH only |

Two things to hold onto about this table. First, the middle row must be
described exactly that way, in the same spirit as security.md's refusal to
call subdirectory mounts isolation: `relay` blocks the *convenient* paths and
does nothing about `base64` through a terminal or a screen scrape. It is a
speed bump that makes bulk exfiltration deliberate rather than incidental,
not a control anything may depend on.

Second, `none` closes **SSH**, not the border. A session with `ssh: none`
still has a desktop, a clipboard, a web terminal and screenshots. The field
should grow into the channel set above before anything depends on its narrow
spelling — it is a cheap change now and an awkward one later.

Enforcement lives in the gateway, because it must: NetworkPolicy allows are
union'd, so the baseline ingress carve-out that lets the gateway reach :22 is
irrevocable by any zone. That is legitimate — the gateway is the only route
into a user namespace, so its refusal is server-side — but it is enforcement
by a trusted component rather than by the network. The strictly better
version, moving the carve-out out of the baseline and into the per-zone
policy so a `none` zone is unreachable at the network layer, is **not**
implemented: the zone policies are egress-only today and making them carry
ingress changes their contract. Recorded as the defence-in-depth follow-up.

### Membership, not ownership

Names resolve per user today because a session CR is `<user>-<name>` in that
user's namespace, so per-user uniqueness is free. security.md's **project
instances** — one instance, several members, each with their own login —
break that: such an instance is not owned by one user, and two users may
reasonably each have a `dev`.

The resolver therefore looks up "sessions this user may reach", which today
is exactly "sessions they own" and later is that union project membership.
The precedence rule to adopt when project instances arrive: **own sessions
win**, and a project instance is always unambiguously addressable as
`<name>.<project>` — dots rather than slashes so it still matches a
`Host *.w` stanza. Left unimplemented, but the resolver is written as a
single lookup function so there is one place to extend.

### ProxyJump preserves member identity; the relay does not

On a project instance this matters more than it looks. The ProxyJump splice
is end-to-end: the guest's sshd authenticates the member with **their own
key**, so the member arrives as their own guest user, and per-login mount
namespaces put their own NFS export at `/data`. The whole storage identity
model in security.md keeps working, unchanged, with the gateway doing
nothing but move bytes.

The relay does not have that property. It authenticates with the **per-user
access key** — the gateway never holds the user's private key, so it cannot
be anything else — which means the relay must log in as *that member's*
guest user, with that member's access key in that member's
`authorized_keys.d` drop-in. Get it wrong and every relayed member lands on
one guest user, which silently collapses the per-member export model into a
single identity. So: **cloud-init on a project instance must write one
access-key drop-in per member**, and the relay must pick the right one. Noted
here because it is the kind of constraint that is invisible until it is
violated.

A second-order consequence worth naming: the relay makes the SSH gateway a
holder of credentials that log in as any user on any instance. The portal
already holds exactly this (screenshots, web terminal), so it is not a new
class of exposure — but it is now two deployments instead of one, and the
known `kubectl`-client DEBUG logging that dumps raw Secret bodies to stdout
(operator `--verbose`) is a live bug against both.

### The on-demand path is a policy decision

`ssh marma@ubuntu.w` creates and starts an instance. Under security.md,
**instance start** is where the zone gradient (rule 1) and the concurrency
invariant (rule 4) are evaluated and where volume taint is imprinted — not
instance creation. So the on-demand path must run the same gate as any other
start, and a refusal has to reach the user.

The plumbing for this already exists and should be used rather than
duplicated: `ensure_session` raises `PolicyError`, and the operator records
it on the Session as `status.policyFailed` + `status.statusMessage`. The
gateway's wait loop therefore has three outcomes, not two — ready, still
booting, and *refused* — and a refusal must fail the channel open with the
message attached, which OpenSSH prints to the user verbatim. A gateway that
only knows "ready or not yet" turns a policy refusal into a mystery timeout.

### Readiness converges on sshd

security.md concludes that project instances force `readinessProbe` onto
sshd (streamers start lazily, so no display port is listening at boot).
ProxyJump wants the same thing for its own reason: the splice must not
happen before sshd answers. Today `readiness_port = display_port if
desktop_stream else 22`, so a desktop VM reports Ready on the streamer port
while sshd may still be starting.

Not changed here — that probe was added deliberately to stop the portal
redirecting to a desktop that isn't listening, and flipping it to 22 would
regress that. Instead the gateway retries the splice itself until sshd
answers. When project instances land, both pressures point the same way and
readiness should move to sshd with the display port becoming a per-member
concern.

## sshd in the target

ProxyJump requires the target to speak SSH. Status per runtime:

- **VMs: already done.** cloud-init ([cloudinit.py](../whistler/cloudinit.py))
  installs the user's own public keys *plus* the per-user portal access key
  into `/etc/ssh/authorized_keys.d/<user>` on the root disk. So end-to-end
  auth with the user's own key works on day one. The portal already SSHes to
  VMs directly (screenshots, web terminal) via
  `get_vm_access_private_key` — the network path gateway→VM:22 exists.
- **Pods: need an sshd.** Options, in order of preference:
  1. A "Whistler-compatible" image convention: image ships sshd; Whistler
     mounts/injects `authorized_keys` (user keys + access key) and a signed
     host cert at pod creation. Provide a reference `whistler-test-ubuntu`
     image (~five-line Dockerfile: `ubuntu` + `openssh-server` + key wiring)
     that replaces the raw `ubuntu:latest` test scenario.
  2. sshd **sidecar** sharing the home volume — works with arbitrary images,
     but a shell in the sidecar is not a shell in the workload container
     (different rootfs); only acceptable if the sidecar shell then `nsenter`s
     or we accept the difference. Park it.
  3. Injecting a static sshd (dropbear, like `bin/socat_x64`) — keeps raw
     images working but adds a second maintained injected binary. Fallback
     only if raw-image support ever matters for real.

  Decision: (1). Raw containers were always a test scenario; the reference
  image *is* the test scenario now.
- **NetworkPolicy:** the baseline `isolate-user-pods` ingress carve-outs
  (config.py) must allow gateway → instance :22, same class of carve-out as
  the existing portal→streamer and storage-gateway rules. Zones are
  egress-only and unaffected.

## Host CA (kill TOFU)

Each instance has its own host key → per-instance TOFU prompts, and the
portal currently connects with `known_hosts=None`
([screenshots.py](../whistler/portal/screenshots.py)) — i.e. no host
verification at all. Fix both with an SSH host CA:

- Gateway/operator holds a CA keypair in a Secret (same pattern as
  `WHISTLER_HOST_KEY_SECRET_NAME`).
- At provision time the instance's host key is generated and signed
  (VMs: key + cert delivered via cloud-init `write_files` +
  `HostCertificate` sshd drop-in; pods: mounted into the compatible image).
  Principals: the instance's suffixed name(s).
- Users add one line, once:
  `@cert-authority *.w ssh-ed25519 AAAA...` — no TOFU ever again.
- The portal and the gateway relay validate against the same CA instead of
  `known_hosts=None`.

**The principals are not just the user-facing names.** A verifier checks the
certificate against *the name it dialled* — OpenSSH against the host pattern,
asyncssh in `_validate_openssh_host_certificate` → `cert.validate(CERT_TYPE_HOST,
host)` — and Whistler's own components dial the per-session Service's cluster
DNS name, not `test.w`. Issuing only the user-facing names cost every relayed
connect: `resolve_ssh_target` returned the FQDN, the certificate carried the
bare Service name, and a valid certificate was refused with "could not open a
session", which reads like a missing sshd. `session_service_host` is now the
one place that name is constructed and `session_ssh_principals` is built from
it, so the dialled name and the signed name cannot drift again
([config.py](../whistler/config.py)). Widening the principals does not widen
access: a certificate still answers only for its own session, and changing the
set makes `needs_reissue` true, so the fix reaches a guest on its next boot.

Two things fall out of a certificate living behind a `?` screen. The
`@cert-authority` line is ~90 characters and the launcher runs in xterm
mouse-reporting mode, where a drag goes to the application instead of
selecting text — so the one screen that exists to be copied out of was the one
screen you could not copy out of. Both are fixed: `SshHelpScreen` turns mouse
reporting off while it is open, and the gateway answers two non-interactive
commands so the strings can be redirected rather than retyped:

```
ssh whistler-gateway ssh-config  >> ~/.ssh/config
ssh whistler-gateway known-hosts >> ~/.ssh/known_hosts
```

A closed list of nouns, not a shell: this channel is the gateway process, and
the place to run commands is an instance.

## Session handover (TUI → instance) over SSH

The gateway cannot convert a running TUI session into an end-to-end
connection — the jump is negotiated by the client at connect time. But it
can act as an SSH **client**: on "connect" in the TUI, `asyncssh.connect()`
to the instance with the per-user access key, request PTY + shell, and pump
bytes between the user's session channel and the remote session. AsyncSSH
covers the whole surface: `terminal_size_changed` → window-change,
exit-status propagation, agent re-export, and native port/SFTP forwarding on
the client connection.

This **replaces the `kubectl exec` bridge** (`_run_pod_shell`,
`_inject_static_socat`, `SubprocessTunnelProtocol`, `_bridge_agent`, the
kubectl-backed SFTP in `sftp_support.py`) with
one uniform transport for pods and VMs alike. Not end-to-end crypto — the
gateway participates in both connections — which is exactly what the exec
bridge is today, so no regression; ProxyJump remains the path for purists
and tooling. TUI prints the `ssh test.w` one-liner next to each instance so
users graduate naturally.

## TUI diet

Scope of this rewrite explicitly includes gutting the TUI. The portal is the
configuration surface; the TUI/portal double-maintenance problem (they are
not synced today) is dissolved by making the TUI **read-and-connect only**:

Keeps:
- Instance list (live status, template, zone, age) with some visual flair.
- Connect (SSH relay handover above), and the copyable `ssh <name>.w` hint +
  first-run `~/.ssh/config` stanza.
- Probably: delete/stop of *own* instances (cheap, high-value; decide during
  implementation).

Goes (moves to portal, which already has equivalents):
- `InstanceCreateScreen`, `TemplateEditScreen`, `TemplateViewScreen`
  ([tui.py](../whistler/tui.py)) and every mutation path except (maybe)
  delete. Template CRUD, user admin, zone admin: portal only.

Whether quick-create-from-template stays in the TUI is an open question; the
`ssh marma@<template>.w` on-demand path may make it redundant.

## Username parsing

`_resolve_target` and the `user-<target>` convention are **removed**; the
username is the username, full stop (this also un-breaks usernames
containing `-`). Interim compatibility: keep the old parsing behind a
deprecation flag for one release if existing users' muscle memory warrants
it, then delete. `validate_public_key`'s key checking is unchanged.

## What gets deleted

The pleasant part. Once relay + ProxyJump land:

- socat injection (`_inject_static_socat`, `bin/socat_x64`,
  `_is_command_available`, `_is_file_present`) and
  `SubprocessTunnelProtocol` / `_create_pod_tunnel` (kubectl-exec tunnels).
- The localhost-only branch of `connection_requested`.
- kubectl-exec shell bridging (`_run_pod_shell`) and agent bridging.
- kubectl-backed SFTP plumbing (native on both ProxyJump and the relay).
- The TUI screens listed above (~600 lines of tui.py).

## Implementation phases

1. **Host CA + VM host certs.** ✅ **Done.** [whistler/hostca.py](../whistler/hostca.py)
   is the pure half (generate, sign, renew, `@cert-authority` line);
   `KubeConfigManager.ensure_ssh_ca` holds the CA in a Secret named by
   `WHISTLER_SSH_CA_SECRET_NAME`, and `ensure_session_host_cert` keeps a
   per-session host key in a `<session>-hostcert` Secret owner-referenced to
   the Session. The key is *persisted*, not rebuilt per reconcile — the
   cloud-init Secret is replaced on every reconcile, so generating on the fly
   would change the guest's identity on every reboot, which is the churn the
   CA exists to end. Delivery is `write_files` + a `HostKey`/`HostCertificate`
   drop-in in cloud-init, additive so the image's own key still serves
   clients that don't know the CA.
2. **ProxyJump routing for VMs.** ✅ **Done.** `SSHServer._jump_to_instance`
   (suffix strip → `resolve_ssh_target` → posture check → splice via
   `conn.forward_connection`), the gateway ingress carve-out in
   `_build_ingress_rules` (port-pinned to 22, its own rule so the pin doesn't
   leak onto the portal's), and port 22 on the per-session Service — the
   splice targets that Service's cluster DNS name rather than a pod/VMI IP,
   so an instance keeps one address across reboots.
3. **On-demand path.** ❌ **Built, then removed** — and the removal is the
   right call. Creating an instance from a template *at connection time*
   demos well and behaves badly: a channel open is the wrong place to wait on
   a cold boot, because the client has nothing to show, no way to report why
   it is taking minutes, and no way to say it never will. `ssh
   <template>.w` now simply reports that no such instance exists.
   Creating from a template belongs in the **launcher**, which can offer
   configuration and track the wait — that is the flow to build next. The old
   `user-<template>` username form is gone for the same reason.

   What survives from it and is worth keeping: the splice counter
   (`_JUMP_SPLICES`) and the `whistler/ephemeral` annotation, so an instance
   marked throwaway is reaped once the last connection closes, with a grace
   window (`scp` then `ssh` seconds apart is the common shape, and reaping
   between them would make the second pay for a cold boot). Nothing sets the
   annotation today; the launcher's create flow will.
4. **Relay handover.** ✅ **Done**
   ([whistler/relay.py](../whistler/relay.py)): an `asyncssh.connect` bridge
   with native PTY, window-change, exit-status propagation and agent
   forwarding, verifying the host against the CA instead of trusting
   anything. `_connect_to_instance` is now nothing but resolve → start →
   wait for sshd → relay, with the same three-outcome wait as the jump
   (ready / still booting / refused by policy) and a terminal to explain it
   in. A note for anyone touching it: the relay runs `encoding=None`. A relay
   is a pipe, and asyncssh's default utf-8 decode corrupts binary output and
   raises on invalid sequences — this was a real bug, caught by testing
   against a real sshd rather than a mock.
5. **TUI diet + username cleanup.** ✅ **Done.** tui.py is 1099 → ~450 lines:
   `InstanceCreateScreen`, `TemplateEditScreen` and `TemplateViewScreen` are
   gone, replaced by an instance list showing each instance's `ssh` address
   and an `SshHelpScreen` (`?`) that prints the `~/.ssh/config` stanza and the
   `@cert-authority` line. Connect exits the app with the choice and the
   session relays, returning to a fresh launcher when the remote shell ends.
   Username routing prefers the whole username, so `anna-lisa` can finally
   log in; the legacy split survives behind
   `whistler.ssh.legacyUsernameRouting` (default on) and warns per use.

### The exec bridge is gone, and pods are not reachable yet

Deleted (2026-08-10), ~950 lines: `_run_pod_shell`, `SubprocessTunnelProtocol`,
`_create_pod_tunnel`, `_inject_static_socat` and `bin/socat_x64`,
`_bridge_agent`, the `localhost` branch of `connection_requested`,
`whistler/sftp_support.py`, `whistler/globals.py`, and the PTY/subprocess
plumbing in `WhistlerSession` (master fd, stdin queue, process stdin) along
with the imports that served it.

The consequence is deliberate and worth stating plainly: **an instance must
run sshd to be reachable over SSH.** VMs do. **Pods do not**, so a pod
resolves and then fails to connect. Pods remain reachable through the
portal's web terminal, which has its own `kubectl exec` path and is
untouched.

The way back for pods is a base image rather than a bridge: sshd installed,
the per-session host key/cert mounted (the Secret already exists —
`ensure_session_host_cert` is runtime-agnostic), `authorized_keys` assembled
as `_build_vm_spec` already does, and `_build_pod_spec` wiring. The open
design question there is whether a pod should reach its home through a
per-user NFS gateway like a VM does, rather than mounting the PVC directly —
unnecessary mechanically, but it would make one storage path instead of two,
and it keeps the session from touching the real volume. Deferred.

A VM's **real screen** does not depend on any of this. KubeVirt's `/vnc`
subresource is the emulated display from power-on — BIOS, bootloader, kernel
messages — and `/console` is the serial line; both go browser → portal →
Kubernetes API server → virt-handler, bypassing the pod network and needing
no guest agent
([`whistler/portal/kubevirt.py`](../whistler/portal/kubevirt.py)). The
Selkies stream is a userspace X capture that only exists once the desktop is
up. Two different views, and `viewer: vnc` vs `websockets` picks.

### Still open

- **The zone SSH posture is enforced and the CRD field exists, but no zone
  sets it.** Default `direct` — today's behaviour. It is also one of five
  channels: `ssh: none` is not a contained session.
- **Channel grants are per zone, not per user or group**, so neither the
  internal-helper case nor the kiosk binding is expressible yet. **The
  gateway is the second door**: a kiosk-bound user must be refused here as
  well as in the portal, or the identity half of
  [the kiosk situation](security.md#closing-the-fourth-axis-the-kiosk-situation)
  leaks and the network does all the work. It belongs next to the zone-posture
  check in `_jump_to_instance` — same shape, intersecting the user's grant
  with the zone's ceiling — and unlike the portal the gateway sees the real
  peer address, so a source-network condition needs no header trust.
- **The portal still uses `known_hosts=None`.** The relay no longer does, and
  `relay.known_hosts_for` is the pattern to copy into `screenshots.py` and
  the web terminal.
- **Reaping is process-local.** `_JUMP_SPLICES` lives in the gateway's
  memory, so a restart forgets the counts and leaves an on-demand instance up
  until someone connects and disconnects again. The durable answer is an
  operator-side idle reaper keyed off the same annotation
  (`whistler/ephemeral`), which is already on the CR for exactly that reason.

## Testing notes

- Unit: routing table for `connection_requested` (suffix handling, wrong
  user's instance → prohibited, non-22 port → prohibited, template
  fallthrough), host-cert generation, cloud-init drop-ins
  (extend [test_cloud_init.py](../tests/unit/test_cloud_init.py)).
- Integration (C1): `asyncssh` client can drive the jump directly — open a
  connection to the gateway, then `conn.forward_connection`/open a
  `direct-tcpip` channel to `<instance>.w` and run a second SSH handshake
  through it against the instance sshd. Also: on-demand create via jump,
  relay handover round-trip, host-cert validation against the CA.

## Open questions

- **Should the per-zone ingress upgrade happen?** Moving the gateway's :22
  carve-out from the baseline policy into the per-zone policy would make a
  `none` posture a network fact rather than gateway good behaviour. It costs
  the zone policies their egress-only contract, which is currently a clean
  invariant. Defence in depth against a bug in one function, at the price of
  a simple rule — genuinely arguable either way.
- Grace period + exact signal for ephemeral cleanup under ProxyJump
  (last-channel-closed vs. idle timer)?
- Does quick-create stay in the TUI at all?
- Suffix default (`.w`?) and whether to accept un-suffixed names too.
- Access-key vs. user-key for the relay hop (access key avoids needing the
  user's *private* key, which the gateway never has — it's the only option;
  note it in docs so users understand relay ≠ their key).
- Pod sshd: uid/home interaction with the per-user PVC mount, and whether
  desktop (streamer) pods also get sshd or stay portal-only.
