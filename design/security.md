# Security model: users, volumes, zones, groups

**Status: mostly design sketch.** Zones exist today as egress postures
([`whistler/config.py`](../whistler/config.py), `Zone` CRs); **groups and the
access-channel grant now exist too** ([Group](#group),
[Access channels](#access-channels-the-second-axis), 2026-08-14). Volumes as a
primitive and the [access matrix](#core-model-the-access-matrix) do not — so
the core guarantee this document is named for is still unbuilt, and a group's
read-only volume grant is a mount option rather than a boundary on a VM (it is
already real for [datasets](storage.md), where the proxy enforces it).
**Taint and security levels were the earlier form of that guarantee and were
dropped on 2026-08-19**; passages below that argue from them are marked where
they have not been rewritten. This document records the model and, more importantly, *why* each piece
is shaped the way it is, so the implementation doesn't quietly drop the parts
that carry the guarantee.

Claims marked **verified** are a narrower thing: the *mechanism* was exercised
against the real gateway image and behaved as described. That says the
primitive exists and does what the model needs — not that Whistler uses it.

## The problem

The driving scenario is a lab that gives visiting researchers access to
digitized material. Some data — behind APIs and on volumes — must never be
reachable from a session that also reaches the internet. Researchers are
onboarded in an open zone and *then* moved to a restricted one, so the move has
to be possible; but after that move their working directory has seen restricted
material and must never again be mounted somewhere with internet access.

The requirement is therefore about **history as well as concurrency**.
Forbidding two sessions from sharing a directory *at the same time* does not
close it: the same user can mount the same home in the open zone an hour
later. An earlier draft answered this with history — remember where a volume
has been, and forbid it moving down — and that answer was
[dropped](#what-this-replaces-and-why-taint-was-dropped): it recorded
potential rather than fact, and obstructed legitimate copying. What survives
from it is the observation that history alone was never sufficient anyway. A
volume live in two zones at once is a live bridge between them, and no amount
of remembering closes that while it is happening. The
[access matrix](#core-model-the-access-matrix) states which zones may hold a
volume at all, and the one-live-attach rule keeps it in one place at a time.

The secondary scenario is data-science staff who legitimately need both the
internet and the restricted data. That must be expressible as policy, not as a
special case in code.

## Assumptions

These are not incidental; the model is built on them.

- **The guest is the user's machine.** VM sessions grant the session user
  passwordless root ([`whistler/cloudinit.py`](../whistler/cloudinit.py)), and
  the credentials for the home share live in the guest. This is a deliberate
  choice, not an oversight. On a project instance it is the *members'*
  machine, plural, and every member is trusted with root over it — so members
  are not isolated from each other there. That is a scoping decision, made
  explicitly in [Shared instances](#shared-instances-and-shared-volumes).
- **Therefore nothing enforced inside the guest counts.** Every boundary must
  be server-side: storage the session cannot authenticate to, or a gateway its
  zone cannot route to.
- **The server bounds the identity set; the guest chooses within it.** This is
  the shape every storage guarantee below takes, and it is worth stating on its
  own because it is weaker than it first looks and stronger than AUTH_SYS
  suggests. The gateway decides *which identities a session can write as* — a
  fixed set, one export each. Which of them a particular write uses is decided
  in the guest, and is therefore not a boundary. What no guest can do is
  produce a write as an identity outside the set, because no export exists that
  would emit one.
- **Withholding `sudo` is not a boundary either** — see [User](#user). It is a
  guideline and a development tool, and the model must never lean on it.
- **Subdirectory mounts are not a boundary.** The storage gateway exports one
  tree ([`images/storage-gateway/ganesha.conf.template`](../images/storage-gateway/ganesha.conf.template),
  `Path = /shares/home` published as `Pseudo = /home`) and the client picks
  which path beneath it to mount. A directory layout is organisation. It is not
  isolation, and it must never be documented as if it were. (This was equally
  true of the SMB gateway it replaced, where `mount.cifs` took the path after
  the share name as a client-side *prefixpath*.)
- **The endpoint is not Whistler's to control, and a normal browser is not a
  safe one.** Every guarantee below concerns what the *server* offers a
  session. What the person does with it at their own keyboard — a laptop with
  a filesystem, a clipboard and a second browser tab — is governed by the
  machine they are sitting at, which the deployment supplies and Whistler
  never sees. This is not a gap to be closed; it is the fourth axis in
  [The border has four axes](#the-border-has-four-axes), and stating it is
  what stops a zone being read as self-sufficient.
- **Out of scope:** a malicious administrator. The model constrains mistakes
  and ordinary users, and makes deliberate overrides visible.

### Substrate: the move to NFS (done)

The home was served over SMB; it is now **NFSv4.2** from a per-user
NFS-Ganesha gateway ([`images/storage-gateway/`](../images/storage-gateway/)),
moved primarily to be rid of `nobrl` (see
[Session state](#session-state-cache-config-state)). That changed what the
enforcement primitives are, and the change is not neutral:

- SMB authenticated **per user with a password**, so shares could be scoped by
  credential and a session could simply not hold the secret for a share it may
  not see.
- NFS (AUTH_SYS) has no such thing. The client *asserts* its uid, and the guest
  is root, so it asserts whatever it likes. Squashing (`Squash = All_Squash` +
  `Anonymous_Uid`/`Anonymous_Gid` set to the user's real ids) pins on-disk
  ownership the way `force user` did — verified: a write by guest uid 4242
  lands as the user's uid — but it is **not authentication**. Nor is the
  transport encrypted, as it was under `seal`.

So the boundary now rests entirely on **which export exists, and who can reach
it**: today one export per user, ingress-fenced by NetworkPolicy to that user's
own session pods in that user's own namespace
(`_build_gateway_network_policy`); under this model, per-boundary exports and
per-zone gateways. That is a workable boundary — arguably cleaner than
credential distribution — but it is a *different* one, and **no design below
may assume "the session doesn't have the password"**: there is no longer a
password. Kerberos (`sec=krb5`, and `krb5p` for privacy) would restore
per-principal authentication and encryption if that turns out to be needed.

## The border has four axes

Defining "restricted" is harder than defining a zone, and the reason is that
containment is not one property. It is the conjunction of four, enforced in
four different places — one of them not by Whistler at all. Most of the
confusion about what a zone promises comes from collapsing them.

1. **The data.** Which volumes a session can reach, in which direction, and
   where they have been. Enforced server-side by the published export set and
   the [access matrix](#core-model-the-access-matrix). This is the axis the
   rest of this document is about.
2. **The channel.** Which mechanisms the person is given for moving bytes in
   and out of a session — SSH and its file transfers, the relay, the web
   terminal, the desktop clipboard, screenshots. Enforced by the gateway, the
   portal and the streamer's configuration. See
   [Access channels](#access-channels-the-second-axis).
3. **The person.** How far they are trusted. Two people on the *same*
   instance in the *same* zone may warrant different channels: a member of
   staff helping an external researcher needs a shell; the researcher does
   not get one. Enforced by per-user and per-group grants, the same shape as
   `allowedZones` and the override grants — and now built as exactly that
   ([Group](#group)).
4. **The endpoint.** The machine and software the person sits at. **Whistler
   can neither enforce nor observe this** — but it can be closed by pairing a
   server-side identity binding with a controlled network, which is what
   [the kiosk situation](#closing-the-fourth-axis-the-kiosk-situation)
   describes.

The fourth axis is why this section exists. A locked-down thin client — a
kiosk browser that reaches one desktop URL and has no local storage, no file
manager and no clipboard of its own — is a real control, and for an external
researcher it is the control doing most of the work. But the deployment
supplies it, not Whistler, and a session that is safe on that thin client is
not safe merely because Whistler served it: the same account opening the same
desktop from an ordinary laptop gets an ordinary browser, with a clipboard
and a filesystem. There is no point in a zone with no internet access if the
person viewing it can paste out of the window.

So the honest claim is the conjunction, and **any statement of the form "this
zone is contained" that does not also name the channel set and the endpoint
is incomplete.** The corollary is that the same zone means different things
to different groups, by design rather than by accident: internal staff reach
a restricted instance over SSH and *could* leak from it, which is accepted on
a personnel control — a decision about who is trusted, not a technical
guarantee, and it must never be written up as the latter.

### Closing the fourth axis: the kiosk situation

The fourth axis cannot be closed by Whistler alone, but it can be closed by a
**situation** — a pair of guarantees, one from each side, whose conjunction is
the containment. Neither half is a boundary on its own, and this is the
intended answer for external researchers.

- **Whistler's half — bound identity.** A user (or group) is bound to the
  kiosk: the server will serve them the kiosk surface and nothing else. No
  management portal, no web terminal, no SSH. This is enforced per user at
  every entry point, so it holds whatever client they use and wherever they
  connect from.
- **The deployment's half — a controlled network.** The kiosk is reachable
  only from a network on which *every* client is a controlled device. Not
  "the researcher has been given a thin client", but "nothing else can be on
  this network": managed switch ports, 802.1X or MAC admission, a dedicated
  VLAN, a room.

Whistler's half is the one to be precise about, because it is the one this
document can promise: **the user cannot obtain a channel that was not granted
to them.** That is a real, checkable property, and it is the channel-plane
form of the rule the storage model already runs on — the server bounds the
set, the far side chooses within it. What it does *not* say, alone, is
anything about the device: a granted kiosk surface opens in any browser. The
device claim comes entirely from the deployment's half.

**The residual risk is therefore network admission, not endpoint control**,
and that is the point of the trade. "Control every endpoint a researcher
might use" is impossible. "Ensure only controlled devices are on this VLAN"
is a standard, solved problem with standard tools. The unenforceable axis has
been exchanged for a tractable one; it has not been eliminated.

Each half fails independently, and each failure is nameable:

- If a channel grant is missed at one entry point — the classic being the SSH
  gateway, which is a different door on a different port from the portal —
  then the identity half leaks and the network half is doing all the work.
- If an unmanaged port exists on the VLAN, a laptop plugged into it presents
  the same source address as the kiosk and is served the same surface. The
  network half leaks and the identity half is doing all the work: the user
  gets the kiosk *surface* in an ordinary browser, with a clipboard and a
  filesystem behind it.

#### Source address: the second, weaker check

Both entry points see the client's real TCP peer address, so a grant can also
be conditioned on **where the connection comes from**. This is worth having
as a second, independent check — "the desktop from anywhere, a shell only
from the lab network" is a rule the server can keep — but it is the weaker of
the two and must not be mistaken for the identity binding. An address proves
*location*, never *hardware*: it says a connection came from that network,
not that the machine there is a kiosk.

Two implementation facts, because they decide where the check can live:

- **SSH is the easy case.** The gateway sees the peer address directly
  (`SSHServer.connection_made` already logs it) with no header to trust.
- **HTTP is the awkward one.** The portal sits behind the bundled Traefik
  proxy and reads no client address at all today, so a rule written in the
  portal would match the proxy on every request. It belongs in Traefik's
  `ipAllowList`, or in the portal via deliberately-trusted
  `X-Forwarded-For` — which is only trustworthy if that proxy is the sole
  ingress and overwrites the header, a deployment property to state rather
  than assume.

The shape that gives both checks independently: kiosk on its own hostname or
entrypoint, IP-restricted at the proxy, *and* the portal refusing every
non-kiosk surface to a kiosk-bound user. The proxy already splits viewer
paths from the management catch-all
([`charts/whistler/templates/portal-proxy-config.yaml`](../charts/whistler/templates/portal-proxy-config.yaml)),
so the routing seam exists — but the proxy does not know who the user is, so
path routing alone is all-or-nothing for a deployment. **The identity check is
the boundary; the address check is the location control.**

#### What exists today: the surface, and the binding

`/kiosk` ([`whistler/portal/kiosk.py`](../whistler/portal/kiosk.py), 2026-08-24)
is the **surface** described above, built on the viewer app so its session page
can frame `/connect` same-origin: a login, the user's own desktop sessions as
cards, one full-screen desktop, and an idle timer
(`portal.kiosk.idleTimeoutSeconds`, default 900s) that returns from a session
to the grid and from the grid to a logged-out login screen. There is no
template picker, no launch form, no web terminal and no admin surface on it —
not as a policy check, but because the pages have nothing else on them.

**The binding exists too, since 2026-08-25**: `User.spec.entryPoints` (and
`Group.spec.entryPoints`), a grant naming which doors an account may use —
`kiosk`, `portal`, `gateway`. `[kiosk]` is the binding this section asks for:
the kiosk surface and nothing else. Empty is **no door at all** — the account
cannot sign in anywhere — which is why the operator seeds the grant on the
bootstrap admin and the portal seeds it on users it creates.

It is composed like `allowedZones` and *not* like `channels`: the effective set
is the union of the user's own list and every group's, so **a group that grants
`portal` widens a kiosk-bound member back out.** That is the same rule as every
other grant here and it is the one thing to know before binding somebody — a
binding is a narrow *own* list plus the absence of a widening group, not a flag
that overrides one.

Three doors, three enforcement points, and the reason they are listed
individually is the failure mode named above:

- **The gateway** refuses at *authentication*
  ([`whistler/server.py`](../whistler/server.py) `_accept`), not per channel.
  The launcher TUI, the relay behind it and the jump are one entry point on one
  port, so a check at any one of them is a check the other two could outlive.
  The dev auth bypass does not open it: `WHISTLER_AUTH_ALLOW_ANY` skips the
  *key* check, and a binding is not a credential.
- **The management portal** refuses in `require_user`, which every route on it
  passes through — the dashboard, the admin pages and every htmx fragment of
  them alike. The dev `?user=` shortcut is an identity shortcut, not a grant,
  so it does not get around it either.
- **The viewer app** serves both surfaces, so there the binding is a path
  question (`_required_entry_point` in
  [`whistler/portal/app.py`](../whistler/portal/app.py)): `/kiosk*` needs
  `kiosk`, the desktop paths themselves (`/connect`, `/desktop`, `/vnc`,
  `/ws-vnc`, `/status`, `/screenshot`) need nothing because both surfaces end
  in them, and **everything else needs `portal`** — written as "everything
  else" so a route added later is checked by default rather than exempt by
  omission. That is also what keeps the per-request User CR read off the
  Selkies asset and WebSocket paths, where the volume is.

A refusal is a page naming the other surface, not a redirect to it: behind the
bundled proxy both surfaces are one origin, in a split-port dev run they are
not, and a 303 would land on a 404 instead of an explanation. Non-navigations
get a bare 403, the same reasoning as the lock's 423.

What this does **not** do, and what the honest claim therefore still is: the
binding says which surface Whistler will serve an account, and nothing about
the device it is served to. A kiosk-bound user opening `/kiosk` from an
ordinary laptop gets the kiosk surface in an ordinary browser, with a clipboard
and a filesystem behind it. The device claim is still entirely the deployment's
half — the controlled network — and the conjunction is still the containment.
What has changed is that the identity half is now a check rather than a plan,
so a leak in the network half no longer means *nothing* is enforced.

**The lock is the one part that is enforced rather than merely offered.** The
thin client decides the person has gone and navigates to
`/kiosk/lock?next=<where it was>`; the portal answers with an HttpOnly cookie
and refuses every route on the viewer app but the lock screen while it is set.
That distinction matters here specifically: a locked browser still holds a
valid identity cookie, so a lock that were only a page — a `?locked=1`, a
screensaver drawn over the desktop — would rest on the client again, which is
the failure this whole section is about. Because it is a cookie the guarded
page cannot clear, the lock holds whether or not the client has an address bar.
It is a lock on *the browser*, not a binding on the account: the same account
elsewhere is unaffected, which is still the missing half above.

Two smaller facts follow from where it is built:

- The login form is real but has nothing to check against. Whistler stores no
  passwords (`User` CRs carry public keys), so `verify_credentials` accepts
  any password while the portal's dev auth gate is open and nothing otherwise —
  the same "SSO/OIDC is a follow-up" position the rest of the portal holds.
  That one function is where a real credential lands. **The unlock shares it**,
  so in dev the lock is a working mechanism with no secret behind it. The lock
  screen says so on its face; it must not be described as protecting an
  unattended session until that function does.

  Since 2026-08-25 **the management portal shares it too**: the same form, the
  same function, moved to
  [`whistler/portal/login.py`](../whistler/portal/login.py) — minus the second
  factor below, which guards a screen in a corridor rather than a workstation.
  The portal previously had no login at all (any request while the dev gate was
  open was answered as the user named in `?user=`, defaulting to `user`), so
  this is the first sign-in on that surface; it is still one credential check
  with nothing behind it, and a real store lands for both surfaces at once.
  One boundary did move: reaching the management UI needs its own marker cookie
  (`whistler_portal`), so a kiosk sign-in is not itself a portal sign-in even
  though both stamp the shared identity cookie. That is a separation between
  surfaces, **not** the entry-point binding — the same person can sign in on
  either with the same unchecked password. It is worth having anyway, because
  the binding, when it arrives, has to be enforced at each entry point, and now
  there is an entry point to enforce it at.
- **A second factor is drawn in, at the right point in the flow, and mocked.**
  `/kiosk/otp` stands between the password and the identity cookie, so the
  password alone yields a pending name and no access. The algorithm under it is
  real and standard — RFC 6238 in the standard library, a scannable
  `otpauth://` QR any authenticator app reads, so no app is privileged — which makes the honest
  claim a narrow one: the *algorithm* is settled, and the parts that would make
  it a control are not. The enrolment secret lives in a process-local dict
  rather than a Secret; no counter is recorded, so a code is replayable inside
  its 30-second step; the pending cookie is a client-supplied username rather
  than a signed token; and there is no rate limit on a six-digit field. Each is
  named in `whistler/portal/kiosk.py`. The QR is genuine — `segno` encodes the
  provisioning URI and it is inlined into the page rather than given a URL of
  its own, because that image is the shared secret; every kiosk page is
  `no-store` for the same reason. So a phone really does enrol here, which is
  worth stating precisely because it makes the screen feel more finished than
  the store behind it is. Until
  the store and the rate limit exist this is a screen that shows the intended
  flow, not a second factor — and specifically not a mitigation for the missing
  entry-point binding above, which is an authorisation question that a stronger
  login does not answer.
- The card thumbnails are `/screenshot/<id>`, so a kiosk shows exactly what the
  `screenshots` channel already permits — including nothing, if it was never
  granted. Nothing new leaves the zone for the kiosk's sake.

#### What the kiosk situation does not fix

- **The clipboard is in the streamer, not the portal**, so refusing the
  portal does nothing to it. On a genuine kiosk device it is contained by the
  device having nowhere to paste — but that is device containment again, and
  the identity half provides no device proof. Clipboard-off still has to be
  enforced server-side for this posture. Kiosk makes the channel far less
  likely to be exercised; it does not remove the requirement.
- **Screenshots close for the researcher and stay open elsewhere.** A
  kiosk-bound user cannot fetch `/screenshot/`, but the images still leave
  the zone into portal memory and onto staff browsers. A different threat,
  untouched by any of this.

## Access channels: the second axis

The general rule this model already applies twice — the published export set
is the access-control decision for storage, the splice set is the
access-control decision for SSH — extends to the interaction plane as a
whole: **the set of channels published to a session is the access-control
decision for what a person can carry in and out of it.**

The channels Whistler offers today, and where each would have to be closed:

| Channel | Terminates in | Closed by | Status |
| --- | --- | --- | --- |
| End-to-end SSH (scp, sftp, rsync, `-L`/`-R`) | the guest's sshd | the gateway refusing the splice | implemented (`ssh`) |
| Relay / TUI handover (PTY) | the gateway | the gateway | implemented (`relay`) |
| Portal web terminal | the guest / pod | the portal | implemented (`terminal`) |
| Desktop clipboard (bidirectional) | the streamer | streamer configuration | **declared, not enforced** (`clipboard`) |
| Screenshots | the portal's memory, served over HTTP | the portal | implemented (`screenshots`) |
| The desktop stream itself | the browser | — | always on; it is the point |

**How it is wired** (2026-08-14). The five names above are the vocabulary:
`Zone.spec.channels` is the ceiling, `User.spec.channels` and
`Group.spec.channels` are grants that narrow it, and
`KubeConfigManager.effective_channels` is the intersection every enforcement
point asks. A zone with no `channels` field derives its ceiling from the
legacy `Zone.spec.ssh` posture (`direct` → `ssh`+`relay`, `relay` → `relay`,
`none` → neither) and leaves the other three open, so zones written before
this keep their exact meaning. The checks live at the two gateway paths
(`_jump_to_instance`, `_connect_to_instance`), the portal's terminal page and
websocket, and — for screenshots — in the *capture* loop rather than the HTTP
route, so an ungranted session's display is never read at all and the pixels
never enter portal memory.

`clipboard` is the honest exception and is labelled as such in the admin UI:
closing it needs a toggle in the streamer, Selkies 2.x exposes none that has
been verified, and a grant that silently did nothing would be worse than a
grant that says it does nothing. `ENFORCED_CHANNELS` in `whistler/config.py`
is the machine-readable form of that caveat.

Two of those rows are the interesting ones, because they are the channels
that survive turning off everything a shell can do.

**The clipboard.** A desktop-only posture is the obvious answer for an
external researcher, and it is not by itself a boundary: the streamer syncs
the clipboard in both directions, `xclip` is installed in the image
specifically to make that work, and no flag disabling it is passed
([`desktops/streamer-selkies2/entrypoint.sh`](../desktops/streamer-selkies2/entrypoint.sh)).
Locking the clipboard down in the thin client is enforcement in the client,
which [Assumptions](#assumptions) says does not count. It has to be off
server-side for a session in that posture. Whether Selkies 2.x exposes a
toggle is unverified — the source is fetched at build time — so the fallbacks
are the existing build-time patch pipeline, or simply not shipping `xclip` in
a restricted variant, which is crude and effective given 2.x shells out to
it.

**Screenshots.** The portal grabs the X display of every desktop session on a
timer, keeps a PNG in memory and serves it at `/screenshot/<id>` at full
stored resolution. That is a data path *out of a restricted zone, created by
Whistler itself*, and it survives disabling SSH, the terminal and the
clipboard. It is documented as monitoring, which it is; it is also egress,
which had not been priced in, and the default box is now 960x540 — half a
1080p desktop, where window titles and UI text are readable —
so the egress is of legible pixels, not of a blur.
`WHISTLER_SCREENSHOT_WIDTH`/`_HEIGHT` are currently the only dial and it is
global — a per-zone setting is what this model needs, and
a zone that means what it says probably wants them off entirely.

**Shape of the setting.** A zone carries a channel **ceiling** — the most any
session in it may use — and a user or group grant narrows it from there. Not
a per-zone switch alone: the whole point of the third axis is that the
internal helper and the external researcher meet in the same zone, on the
same instance, and must not get the same channels. A maximally restricted
zone sets its ceiling to the desktop stream alone, and no grant can widen it.

## Empty means nothing, everywhere

**Decided and implemented 2026-08-25.** Every allow-list in Whistler grants
exactly what it names. An empty list — `allowedVolumes`, `allowedZones`,
`allowedGpuTypes`, `entryPoints`, on a `User` or on a `Group` — grants nothing.

It used to mean the opposite. Empty was read as "the admin has expressed no
opinion", so an empty list was *no restriction*, and every enforcement point
carried the same guard:

```python
if allowed and requested not in allowed:      # before
    raise PolicyError(...)
```

That `if allowed` reads like a null check and acts like a policy, and it made
the safest-looking User CR in the cluster — the one with no lists on it at all
— the most permissive object in the system. The account nobody had configured
could enter through any door, run in any zone, mount any volume in the catalog
and request any GPU on any node. A newly defined S3 dataset was writable, on
the day it was defined, by every user who happened to be in no group. None of
that was anyone's decision; it was the default falling out of a convention.

The rule is now the matrix's rule, which was right first: **an absent grant is
not a missing opinion, it is a no.** The guard is gone from every enforcement
point, and the composition rule loses a clause rather than gaining one — *the
union of the user's own list and every group's is what they hold*, full stop.

Three consequences worth stating, because each is a place the change is
visible:

- **The implicit default zone is gated like any other.** A template with no
  `zone` does not run outside the zone model; it runs in `default`. That is
  now the grant it is checked against, closing the one path by which a user
  granted no zone could still start a session.
- **A brand-new account has to be given a door.** Two creation paths seed the
  two grants that decide whether an account is usable at all — every entry
  point and the `default` zone (`NEW_USER_ENTRY_POINTS`, `NEW_USER_ZONES`):
  the operator's `ensure_bootstrap_admin`, so a fresh install is not locked
  out of itself, and the portal's *New User* form. Volumes and GPU types are
  **not** seeded: those are grants an admin means to make. The seed is a
  creation default and never a floor — narrowing it afterwards sticks, or the
  kiosk binding would be undone by the next operator restart.
- **Losing the Group catalog now fails closed.** `_load_groups` keeps the
  previous catalog on an API error, which used to be about not *widening*
  members back to their own empty lists. The direction has flipped: a lost
  catalog would now cut a project's members off from their own sessions, which
  is the right way round but is exactly why the previous catalog is still kept
  rather than dropped.

**There is no migration.** Existing `User` CRs with empty lists hold nothing
from the moment this ships — including admins, whose account was created
before the seed existed. The way back in is `kubectl`:

```bash
kubectl -n whistler patch usr <name> --type merge -p \
  '{"spec":{"entryPoints":["kiosk","portal","gateway"],"allowedZones":["default"]}}'
```

A backfill was considered and rejected: it would write the old implicit grants
onto every account as explicit ones, which is a worse record than none — an
admin reading `allowedZones: [default, restricted, secure]` cannot tell
whether anybody decided that. The point of the change is that a grant is
something somebody wrote down.

## Core model: the access matrix

Access to data is one table, and the table is the whole model.

> **(subject, zone, volume) → `allowed` | `read-only`**
>
> An absent entry is **no access**. There are no defaults.

`subject` is a user or a group. `volume` is any volume kind — a home volume, a
shared volume, a dataset. `zone` is the zone the instance runs in. A user's
effective access to a volume in a zone is the most permissive entry across
their own table and the tables of every group they belong to, ordered
`allowed` > `read-only` > absent.

That is the entire rule. What follows is why each part is shaped that way.

### Every allow is explicit

An empty cell means the admin has not said yes, so the answer is no.

This used to be an *inversion*: everywhere else in Whistler an empty allow-list
meant no restriction — the admin had expressed no opinion, so the user was
unrestricted — and the matrix was the one field that read the other way. That
inversion is gone as of **2026-08-25**: `allowedVolumes`, `allowedZones`,
`allowedGpuTypes` and `entryPoints` all now grant exactly what they name and
nothing when they name nothing. See [Empty means nothing, everywhere](#empty-means-nothing-everywhere).

What survives is the shape of the field. A cell is `(zone, volume, mode)`, not
a name, so this is still a table and not an allow-list, and the UI renders
**the whole grid including its empty cells** — a policy visible only through
its exceptions is one nobody audits.

What it costs: a subject's table is zones × volumes, and a new zone or a new
volume starts closed everywhere until someone opens it. Accepted knowingly.
The failure mode of a permissive default is data leaving a restricted zone
quietly; the failure mode of this one is a user who cannot mount something and
says so within the minute.

### Why groups compose as a union

A user's grants are unioned with every group's, most-permissive-per-cell. This
is the same direction as every other allow-list in Whistler, and here it is
also simply what an admin means: joining a group is a deliberate act whose
purpose is to confer access, and a user who already held access does not lose
it by joining a project.

The hazard worth naming is a different one. It is not that a user gains access
by joining; it is that **one member's access reaches another member**. That
needs an object several members share, and today none exists — every instance
belongs to exactly one user, so a member's grants only ever apply to their own
sessions.

**Tripwire for [project instances](#project-instances-several-members-one-machine-no-desktops).**
A shared instance must not resolve its access as the union of its members'
tables; that is exactly the leak above, and it would arrive silently as a
consequence of the composition rule rather than as a decision anyone made. A
project instance needs its own subject entry in the matrix — the project's
access, not its members'. The model is safe today only because the feature is
absent, which is a reason to write the constraint down now rather than
rediscover it then.

### One live attach

A volume is attached to at most one **running** instance at a time, whatever
the mode.

The reason is not security, and saying so matters because a rule believed to
be a boundary gets relied on as one. A home is an ext4 image on a block
device, and ext4 is not a cluster filesystem: mounted read-write by one guest
and read-only by another at the same moment, the reader sees inconsistent
metadata — stale directory entries, failed mounts, kernel errors. Hypervisor
read-only (KubeVirt's `disk.readonly`, confirmed present in the deployed
version) stops the reader *corrupting* the image; it does nothing to make the
reader's view *coherent*.

Evaluated at **start, not creation**. A created-but-stopped instance holds no
attachment and must neither claim one nor block one. This is the same
placement the taint model needed for its own reasons, and the on-demand SSH
path must run the same gate as any other start
([proxyjump.md](proxyjump.md#the-on-demand-path-is-a-policy-decision)).

The consequence for the two-zone user is worth stating in the concrete: they
may have an instance in each zone, each with its own home volume, and may
mount the open home read-only inside the restricted instance — **but not while
the open instance is running**. Stop it, and the read-only mount is available.

### What `read-only` is worth, per volume kind

It means something different in each case, and one of them is not a boundary
at all:

| Volume kind | Mechanism | Holds against root in the guest? |
| --- | --- | --- |
| Dataset (S3) | The proxy runs `--read-only`; `ro` and `rw` are separate proxies with separate fencing | **Yes** |
| Home volume / block disk on a VM | KubeVirt `disk.readonly` — enforced by the hypervisor | **Yes** |
| PVC volume in a container session | `readOnly` mount, kernel-enforced | **Yes** |
| PVC volume on a VM | A mount option the guest chooses | **No** |

The last row must be labelled wherever it appears, the way `clipboard` is
labelled in `ENFORCED_CHANNELS`: recorded, not enforced. A read-only grant a
root user can drop is a statement of intent, and presenting it as a control is
worse than not offering it.

### What this replaces, and why taint was dropped

An earlier version of this model gave each volume a **taint** (the set of
zones it had been mounted in writable) and each zone a **security level**,
forbidding a volume from moving down the gradient. It is recorded here as
rejected, with the argument, so it is not reinvented.

- **Taint records potential, not fact.** That a volume was attached writable
  in a restricted zone is not evidence that restricted data was written to it.
  The label is a proxy for a thing nobody observed.
- **It obstructs legitimate work.** Copying validated data out of a restricted
  volume is a normal administrative act, and under taint it required clearing
  a label first.
- **It is not a boundary against the person it constrains.** Anyone who can
  read both sides can `scp` through an intermediate host. The label stops the
  convenient path, not the capable one.
- **A control that is routinely overridden trains people to override it.** An
  uncleared taint that everyone has learned to work around is worse than no
  taint, because it still looks like a boundary to whoever reads the schema.

**What is lost, stated plainly:** the matrix has no memory. If restricted data
has been written into a volume and an admin later marks that volume `allowed`
in an open zone, nothing objects. Under taint that flip demanded an explicit
declassification. The matrix's answer is that this is an administrative act in
both models — the difference is only whether the system pretends to have
checked something. It should be visible in the audit trail of who changed
which cell, not dressed up as an automatic rule.

### What this model does not claim

It is **not** a containment boundary against a person holding shells in two
zones. Such a person can carry data between them through their own client
regardless of what the table says, and
[Zones fence the network, not the data](storage.md#zones-fence-the-network-not-the-data)
already says so about zones. The matrix closes the *storage* channel — the one
Whistler would otherwise provide silently by handing the same volume to two
zones — and closes nothing else.

It becomes a real boundary in exactly one case: when the user does **not**
have a shell in one of the two zones, because then no other channel exists to
carry the data. Which gives a rule the implementation can actually check:

> A cross-zone volume grant should not exceed the channels the user already
> holds across those zones.

When it does, the grant is opening a path that was closed. Whistler can
compute that from [access channels](#access-channels-the-second-axis) and warn
at the point of granting; it should not refuse, because an admin may mean it.

### How it subsumes today's grants

The matrix replaces `allowedVolumes` and the group volume `mode`/`access`
grants rather than layering over them. Two mechanisms with opposite defaults
governing the same question is the confusion this model exists to remove.

A dataset's `readOnly` field stays what it is — a **ceiling** on the volume
itself, not a cell — and composes as the more restrictive of the two, so a
read-only dataset cannot be widened by a matrix cell that says `allowed`.

**Migration.** Existing grants are rendered into the matrix on upgrade: for
each volume a user may mount, an entry at the granted mode in each zone that
user may enter. That reproduces today's access exactly, which is the point —
nobody is locked out by an upgrade — and it should be said out loud that the
result is as permissive as today, so the seeded grid is a starting point to
tighten, not a policy anyone chose.

## User

Users are `User` CRs (`usr`) today, admin-managed through the portal, with
allow-lists and per-session override grants.

The change this model asks for is that **the home stops being special**. Today
each user gets exactly one per-user PVC for `$HOME`
([`whistler/config.py`](../whistler/config.py), `_ensure_pvc`), created
implicitly. Under this model a home is an ordinary volume that happens to be
bound to a user and mounted at `$HOME`, and a user may own several. Instance
creation chooses which one to use as the home.

That single change removes the need for a rule about homes. A user working in
two zones ends up with two home volumes not because a policy says "one home per
zone", but because a single shared home would need an `allowed` cell in both —
which an admin can grant, and which the matrix then shows them they have
granted. The mechanism is the same one that governs every other volume, and
the choice is visible rather than implied.

Existing `User` fields (allowed zones, override grants) stay as they are, and
a `channels` grant joins them. What a user may mount comes from the
volume/zone rules and from [group membership](#group), which is built: every
`User` allow-list is now resolved as the union of the user's own field and
their groups'.

### `sudo` rights

Grantable **per user and per instance**, and explicitly *not* a security
boundary. Recorded here so nobody later mistakes it for one.

Its two real purposes:

- **Without `sudo`**, a nudge rather than a wall: users install project
  dependencies into user space instead of the instance root, so their work
  survives an image rebuild instead of evaporating with the root filesystem.
- **With `sudo`**, a development tool: someone building a new environment can
  experiment with the global setup live, and only bake an image once the recipe
  has settled — instead of a rebuild per iteration.

Neither purpose is confidentiality, and no rule in this document may depend on
a session lacking root. A user without `sudo` in a VM they otherwise control
should be assumed able to get it; the guarantees have to hold anyway.

## Volume

A new primitive, and the place the guarantee actually lives.

- **Created explicitly** in the admin interface, backed by a PVC, with an
  optional explicit storage class.
- **Owned** by a user or (later) a group.
- **Fields**, at least: name, owner, size, storage class, access mode, and a
  pinning/shareability property (below).
- **Which volume a session gets is chosen at instance creation** and fixed for
  that instance; a user may own several, and homes are ordinary volumes that
  happen to be mounted at `$HOME`.
- **Attachment is recorded by the operator**, before the session starts, and
  is what the one-live-attach rule is evaluated against — not anything in the
  guest.

**The matrix governs every volume a session mounts, not just the home.**
Exempting scratch volumes would make the copy-out trivial: mount a clean
volume in the restricted zone, copy into it, mount it in the open zone.

**Pinned vs shareable.** Distinct from the matrix, and the thing "no shared home
directories" was reaching for: a volume may be *pinned* to a single instance, or
be of a kind that can be attached to several. A zone can require that everything
mounted in it be pinned. This is a property of the volume's kind — "can this be
shared at all" — not a statement about two sessions running at once.

**Enforcement.** A matrix the operator consults is defeated by anyone who can
reach a gateway from another zone, since the guest has root. The table is the
policy; the mechanism has to be one of:

- per-boundary **gateway pod**, with the zone's NetworkPolicy making the others
  unroutable — the only one of these still available now that the substrate is
  NFS, and therefore the one to design for. Its finer-grained form, one
  *export* per identity rather than one gateway per boundary, is what
  [Shared instances](#shared-instances-and-shared-volumes) is built on; or
- ~~per-boundary **share with its own credentials**~~, which the SMB gateway
  could have done and **NFS/AUTH_SYS cannot** — see
  [Substrate](#substrate-the-move-to-nfs-done). Recorded as closed, not as an
  option.

Neither the field nor the mechanism works alone.

### Session state: cache, config, state

Per-session desktop state is where volume layout, the concurrency hazards and
the access rules all meet. The split:

- **`XDG_CACHE_HOME` never goes on a shared volume.** It is discardable by
  definition, it is where most of the concurrency hazard lives (SQLite indexes,
  thumbnails, font and shader caches, browser disk caches), and network storage
  is bad at exactly its many-small-files pattern. An `emptyDir` on node
  ephemeral storage — not memory, since caches grow — or the equivalent scratch
  disk for VMs.
- **`XDG_CONFIG_HOME` and `XDG_STATE_HOME` must persist**, and can co-exist
  between sessions to a degree — much of it is idempotent or last-writer-wins
  in ways nobody notices. `~/.config/dconf/user` is the known exception: dconf
  rewrites the whole database on any change and coordinates through a
  machine-local flag file, so two sessions on two hosts silently clobber each
  other's settings.
- **One PV per instance for config/state** is preferred over having the gateway
  selectively export one instance's subdirectory from a shared PV. The
  selective-export version is possible but puts the isolation back in the
  gateway's path handling, which
  [Assumptions](#assumptions) says is not a boundary. A separate volume gets the
  same access rules as everything else for free, and one fewer special case.

The move to NFS removed the need for `nobrl` and with it the cross-host locking
hole — SQLite-backed state (browser profiles, keyrings) stopped being a
corruption risk when two sessions share a home. That was a fix for a real bug,
not a security control, and the two should not be conflated: it bought no
isolation, and it cost the per-share credential
([Substrate](#substrate-the-move-to-nfs-done)).

## Zone

Zones already exist as named egress postures: one egress-only NetworkPolicy per
user namespace, pods selected by a zone label stamped at build time, unknown
zones failing closed, `default` always present, changes taking effect on reboot.
All of that stays.

What this model adds to the `Zone` CR:

- **A pinning requirement** — may this zone mount shareable volumes at all.
- **A channel ceiling** — the most any session here may use
  ([Access channels](#access-channels-the-second-axis)). A ceiling, not a
  setting: the per-user grant narrows it, and nothing widens it. **Done**:
  `Zone.spec.channels` names the full set, and the older `Zone.spec.ssh` —
  which named one of the five because it was the one the gateway work needed
  ([design/proxyjump.md](proxyjump.md)) — is folded in as the fallback when
  the new field is absent.
- Possibly: permitted storage classes, and whether read-only cross-level mounts
  are allowed here.

The egress posture, the data posture and the channel ceiling are three faces
of the same object, and that is the point: a zone is low-level *because* it
reaches the internet, and a zone that forbids the internet while permitting
`scp` has not forbidden anything. Separate primitives would let them drift
apart.

**Live edits.** The current split — an edited zone re-fences running sessions in
place, while zone *membership* changes need a restart — is worth preserving as
is; instant re-fencing is a feature. Revoking a matrix cell is the awkward
case, since it can retroactively invalidate a mount that is already live, but
it does **not** have to take effect instantly: flag the affected running
instances as needing a reboot (and surface a warning at edit time), then let
the rule apply at next boot. A revocation that killed live sessions would be
worse than the exposure it closes, and an unreadable "why did my session die"
is worse than a visible "this instance must restart". Note this is the one
place the model is knowingly not fail-closed, and the trade is deliberate.

## Group

**Implemented** (2026-08-14). A `Group` CR (`grp`) holding users and shared
settings — available volumes with per-member access modes, zones, GPU types,
per-session override grants and the channel grant. Admin-managed in the
portal's Groups section, or rendered from `whistler.groups` values as
Helm-owned CRs, exactly like zones. Membership lives on the group and nowhere
else — there is no `groups` field on `User` — so a project is edited in one
place.

**The composition rule is a union, and it is the same rule for every field:**
the union of the user's own list and every group's is what they hold, and
nothing else. Since 2026-08-25 empty is empty ([Empty means nothing,
everywhere](#empty-means-nothing-everywhere)), which sharpens what a group is
for rather than changing it: a user with no list of their own holds *exactly*
what their groups grant, so a group is the only thing that can widen them,
while a user who already had a list *gains* the group's, because grants add
up. Override grants OR together; nothing a group says can take away what a
user holds in their own right. `KubeConfigManager.get_user_allowed_volumes`
and its siblings answer with the resolved set, so `_apply_policy` and the
portal never see the two sources separately (the admin UI reconstructs the
provenance for display, and its checkboxes deliberately edit only the user's
own fields — saving the resolved set would copy a project's grants onto a
member who would then keep them after leaving).

The field that carries weight is **membership with a per-volume access mode**:
for each volume the group can reach, which members get `rw` and which get `ro`
(`mode` for the default, `access: {user: rw|ro}` for exceptions, `mode: none`
for a volume only the named exceptions reach). That list is not merely a UI
affordance — it is what the gateway's export list will be rendered from, which
is where the enforcement has to end up
([Shared instances](#shared-instances-and-shared-volumes)).

**Where `ro` is enforced today, and where it is not.** It becomes `readOnly`
on the volume mount: real for a container session, **advisory for a VM**,
whose user has root and can remount. Read-only has to be
[a server-side decision](#read-only-is-a-server-side-decision-or-it-is-nothing)
to be a control at all, and that means rendering it into the export set — not
built yet.

**The `rw`-wins rule above is the single-user rule and does not generalise.**
It is correct here because every grant belongs to one identity on a machine
only that identity uses. On a shared project instance the rule inverts to the
meet — any member's `ro` makes the volume `ro` for the whole instance — for
reasons given in [The meet rule](#the-meet-rule-any-member-ro-means-ro). Two
opposite rules, one principle: resolve to what is true of *the machine the
volume is published to*.

A group also carries the **channel grant** — which of the zone's permitted
channels its members actually get. This is where the third axis lives, and it
is the reason the channel ceiling cannot be a per-zone switch on its own:
"lab staff" and "visiting researchers" meet in the same restricted zone, on
the same instance, and the whole design depends on them not getting the same
doors. Two groups, one zone, different channel grants — no special case in
code, and the grant can be conditioned on source network
([source address](#source-address-the-second-weaker-check)).

Two encodings are load-bearing and easy to lose:

- **Absent is not empty** for `channels`. No field means "this source narrows
  nothing"; an empty list means "nothing but the desktop stream". Both have to
  be writable, which is why the portal has a *restrict* toggle beside the
  boxes and why `set_user_channels(None)` removes the field rather than
  writing `[]`.
- **A member named in a volume's `access` map need not be in `members`.** It
  is the natural way to hand one outsider a read-only look at a project
  without making them a member of it.

Not done: nesting (deliberately — none), a group's rows in the
[access matrix](#core-model-the-access-matrix), which wait on the matrix
existing at all, and any notion of who may edit a group beyond "an admin".

## Shared instances and shared volumes

A **project instance** is one session several members connect to. It mounts a
shared project volume, and each member's own home. Everything is on one guest,
with one IP and one kernel, and any member may hold root on it. This section
says what the server can still guarantee there, because it is the case where
the single-user mechanisms stop applying and the temptation to hand-wave is
strongest.

This section is about **storage** and holds wherever members have their own
logins. What it takes to give them those logins — separate desktops, on demand,
on one machine — is
[Project instances](#project-instances-several-members-one-machine-no-desktops),
and none of it exists yet. The mechanisms here do **not** apply to a
shared-screen session (one Selkies pipeline broadcast to many viewers), which
runs as a single Unix user and therefore has a single identity by construction.

The requirement is deliberately *not* member-versus-member isolation. It is
**containment of the identity space**: a project instance may write as any of
its members — including a member impersonating another, including root — and
must never be able to produce a file owned by anyone outside the project.

### The mechanism: one export per member, all onto the same directory

For a volume shared by `{alice, bob, carol}` the gateway publishes three
exports with the **same `Path`**, differing in `Export_Id`, `Pseudo`,
`Filesystem_Id` and `Anonymous_Uid`/`Anonymous_Gid`, each `Squash =
All_Squash`. The guest mounts all of them.

```
EXPORT { Export_Id = 1; Path = /shares/proj; Pseudo = /as/alice;
         Squash = All_Squash; Anonymous_Uid = 1001; Anonymous_Gid = 5000;
         Access_Type = RW; Filesystem_Id = 1.1; ... }
EXPORT { Export_Id = 2; Path = /shares/proj; Pseudo = /as/bob;    ... 1002 ... }
EXPORT { Export_Id = 3; Path = /shares/proj; Pseudo = /as/carol;
         Access_Type = RO; ... }
```

Identity is therefore a property of **which export a write goes through**, not
of anything the client asserts and not of where it comes from. The set of
exports *is* the whitelist; adding a member is adding an export. Nothing here
depends on file modes, on the client's asserted uid or gid list, or on the
guest behaving.

**Verified** against [`images/storage-gateway/`](../images/storage-gateway/) —
one guest, one source IP, root in the guest:

| Guest action | On-disk result |
| --- | --- |
| write via `/as/alice` | `1001:5000` |
| write via `/as/bob` | `1002:5000` |
| write asserting uid 4242 | `1001` — squashed |
| `chown` to a non-member (1009) | stays `1001` |
| `chown` to root | stays `1001` |
| write via `/as/carol` (`RO`) | refused, "Read-only file system" |
| client not in any export | mount refused outright |

**Which export a given process writes through is the guest's business.** The
intended arrangement is a per-login mount namespace (`pam_namespace` or
equivalent) putting that member's export at a fixed path. That is
**convenience, not a boundary** — root can write through any of the mounts —
and it must never be documented as if it were. It doesn't need to be a
boundary: acting as another member is explicitly permitted.

### Publishing and revoking exports while the gateway runs (measured)

The export set is the access-control decision, so the question that decides
whether it can carry policy at all is: **can it change without restarting the
gateway, and without restarting the guests mounted on it?** Measured against
the running per-user gateway (nfs-ganesha 6.5, Debian trixie), by driving the
DBus interface the readiness probe already uses:

| Operation | Result | Restart needed |
| --- | --- | --- |
| `AddExport` — second export of the *same* `Path`, new `Export_Id`/`Pseudo`/`Filesystem_Id` | `"1 exports added"` | none |
| `UpdateExport` — flip `Access_Type` `RW` → `RO` on a live export | `"1 exports updated"` | none |
| `RemoveExport` by id | ok; export list back to its prior state | none |

So the mechanism is there and needs no new machinery in the image:
`dbus-daemon` is already started by
[`entrypoint.sh`](../images/storage-gateway/entrypoint.sh) — it exists for
`gateway-ready`, and `org.ganesha.nfsd.exportmgr` exposes
`AddExport`/`UpdateExport`/`RemoveExport`/`ShowExports`/`DisplayExport` on the
same bus. What is missing is the *caller*: nothing in Whistler writes export
fragments or drives that bus today.

Two things the measurement does **not** settle, and they should not be
asserted until they are:

- **When a mounted client notices an `UpdateExport`.** The server applies it
  to subsequent operations, but an NFS client has an attribute cache and may
  hold open file handles, so an `RW` → `RO` flip is not instantaneous from
  the guest's side and a write already in flight may still land. Fine for
  "membership changed, tighten it", not a revocation primitive.
- **`RemoveExport` under a live mount is disruptive by design.** The client's
  filehandles go stale (`ESTALE`); there is no graceful "unpublish" for a
  guest that is using it. So *revocation while running* is the hard case —
  which is the same conclusion
  [Membership changes](#membership-changes-on-a-running-instance) reaches from
  the guest side.

One gotcha, since it cost a failed attempt: **the VFS FSAL cannot export a
path on overlayfs.** It addresses files by handle, and `name_to_handle_at` on
overlayfs returns `EOPNOTSUPP` — ganesha reports `Could not get handle for
path ..., error Operation not supported` and refuses the export. Exports must
live on the PVC, never on the container's own filesystem.

### Read-only is a server-side decision, or it is nothing

A `readOnly` mount option chosen by the guest is worth exactly nothing when
the guest's user has root — they remount it. This is not a subtle point and
the model must not lean on it anywhere: **`ro` means `Access_Type = RO` on the
export the write would go through**, enforced by the gateway, as the
`/as/carol` row above shows.

The consequence for [Group](#group) is concrete: a group's per-member `ro`
grant is only a real control once it renders to an export. Whistler currently
translates it into a `readOnly` volume mount, which *is* enforced for a
container session (the kubelet mounts it read-only and an unprivileged
container cannot remount it) and is **advisory for a VM**. Until the export
set exists, a `ro` grant on a VM session is a statement of intent, and the
admin UI says so rather than implying a boundary.

### One mountpoint, and ordinary POSIX modes inside it

The arrangement to aim for is that the shared volume is at **the same path for
everyone** — `/data` — with permissions inside it expressed as ordinary file
modes: a `0700` subdirectory is that member's own, a group-writable one is the
project's. This is not asking for a security guarantee. It is asking that the
obvious thing be true, so people do not leak things to each other by accident;
root defeats it and that is fine.

Per-login mount namespaces give this directly: every member's `/data` is the
same path, bound from their own export. And because `All_Squash` sets the
credential the server *evaluates access with* — not merely the ownership it
stamps on writes — the modes are enforced by the server, which is better than
the requirement asks for. Verified, one directory tree, two members, guest root
asserting a junk uid throughout:

| Subdirectory of `/data` | alice | bob |
| --- | --- | --- |
| `shared` — `2770 alice:project` | read + write | read + write |
| `alice-private` — `0700 alice:project` | read + write | **denied** |

**The limit: one group per member, per volume.** A squashed credential is
exactly `(Anonymous_Uid, Anonymous_Gid)` with **no supplementary groups**, and
`Manage_Gids` does not change this — verified explicitly, with and without it,
against a directory owned `9999:secret` mode `0770` where only a supplementary
group membership could have opened it. Both settings denied a member who is in
`secret` in the gateway's own group database. So a subtree inside `/data`
group-owned by a *subset* of the project is not expressible.

**Express subgroups as volumes, not as groups inside a volume.** A subgroup
gets its own volume with its own export set, published only to its members, and
mounted at its own path. That costs the single-mountpoint property for that
subtree and needs no new mechanism at all — it is the same rule as everything
else here, that the published export set is the access-control decision. The
alternative that would preserve one mountpoint is dropping the squash for
pass-through uids plus `Manage_Gids`, which does restore full multi-group
POSIX — and immediately forfeits identity containment, since the client then
asserts its own uid and can assert a non-member's. That trade is the whole
reason [Kerberos](#open-questions) exists as an option: it is the only way to
have both.

### Why not the obvious alternative

The first design that comes to mind is one export with pass-through uids
(`Squash = Root_Squash`), `Manage_Gids = true` so the server resolves each
uid's groups from its own database instead of trusting the client's list, and a
volume root at `2770 root:project`. A forged non-member uid then fails the
directory check, because the server won't agree that uid is in the group.

It nearly works, and it is rejected because the containment rests on **mode
bits on every directory in the volume**. One member creating a `0777`
subdirectory reopens the hole, silently and legitimately. Guest-settable state
carrying a guarantee is exactly what [Assumptions](#assumptions) refuses. The
export-set version has nothing to misconfigure: a non-member uid is not
expressible.

### A home mounted in several instances at once

**A home volume can be mounted in several running instances simultaneously**,
and the model must treat that as normal rather than exceptional. It is what
rule 4 governs, and it is why the volume's *kind* (pinned vs shareable) is a
field rather than an assumption.

What holds: every consumer goes through one gateway, so byte-range locks are
coherent across instances — the same guarantee verified across exports applies
across sessions, because it is the same server. That is the property `nobrl`
used to destroy, and it is what makes a concurrently-mounted home merely
awkward rather than corrupting.

What does not hold is anything that assumes a single writer:
[Session state](#session-state-cache-config-state) stops being a preference and
becomes a requirement. `~/.config/dconf/user` is the sharp edge — dconf rewrites
the whole database on any change and coordinates through a machine-local flag
file, so two live sessions silently clobber each other's settings, and no
storage-layer fix reaches it. `XDG_CACHE_HOME` off the shared volume, and
config/state on a per-instance volume, are what make concurrent mounts usable.

### Per-user homes on a shared instance

Same mechanism — each member's home is its own volume, exported once with that
member's identity, mounted at `/home/<member>`. Keeping members' processes out
of each other's homes is then the guest's job, and a member with root can read
another member's home.

**This is accepted, not prevented**, and it is the single most likely thing for
a user to assume is false. It should be visible in the UI when a project
instance is created, not buried here. If it ever must be false, the mechanism
is Kerberos: per-uid GSS contexts are the only way one host holds several
*verified* identities at once, and going there also means no root on the
instance, since root can read another member's credential cache.

### What carries over from the single-user case, and what doesn't

- **Locking survives.** A whole-file `F_WRLCK` taken via `/as/alice` was
  refused via `/as/bob` — same server, same file, shared state. The SQLite
  property the NFS move bought is intact on a shared volume.
- **But only with one gateway pod per volume.** NFSv4 lock state is per-server;
  several ganesha processes over one filesystem would not see each other's
  locks and would reintroduce exactly the corruption the move to NFS removed.
  Many exports, one server.
- **`Filesystem_Id` must differ per export.** With the same fsid on two exports
  of one path the client misbehaved. Verified the hard way.
- **Serve every consumer through the gateway.** A pod session mounting the PVC
  directly does not participate in ganesha's lock state, so a volume shared
  between gateway-served VMs and direct-mounting pods loses coherence again.
  As a consequence of routing everything through one gateway, the PVC is
  mounted exactly once and RWO suffices — gateway-mediated sharing does not
  need RWX storage.
- **File modes are guest-chosen.** A member's umask decides whether their files
  are group-writable, so collaboration depends on guest configuration; the
  server-side fix is NFSv4 ACLs with an inheritable ACE
  (`Disable_ACL = false`) rather than mode bits.
- **Membership changes are not retroactive.** Removing a member stops new
  writes as that identity; their existing files stay theirs. This interacts
  with declassification, where "who wrote this" now has several answers.

### Interaction with the access matrix

The matrix is unaffected by multi-member export sets, because its `zone`
dimension is orthogonal to identity: a volume several members can reach is
still a volume in exactly one zone at a time, and the cell that governs it is
the same cell. What multi-member access changes is *who* may write, which the
export set decides — not *where the data may go*, which the matrix decides.

The one place the two must be read together is the
[tripwire](#why-groups-compose-as-a-union): a shared instance must take its
access from its own subject entry, never from the union of its members'.

The general statement the export-set model licenses is that **the set of
exports published to an instance is the access-control decision** — it encodes
which identities that instance may write as *and* whether each volume is `rw`
or `ro` there. Rule 3 then needs no separate machinery: an instance that may
only read a volume is simply published an `RO` export of it and no `RW` one.

## Project instances: several members, one machine, no desktops

**Status: not built; the shape is decided (2026-08-15).** The storage model in
[Shared instances](#shared-instances-and-shared-volumes) is the part that is
worked out; everything here is the machinery around it.

**The shape**, and each clause is a constraint that removes work rather than
adding it:

- **Always a VM.** Not a container, not Kata. A project instance is a machine
  with several real logins, systemd, sshd and root — which is what
  [container_workloads.md](container_workloads.md) already says a VM is for.
  There is no second runtime to design storage or identity for.
- **Created with a member list**, which is the input everything else derives
  from: the export set, the guest's users, the `authorized_keys` drop-ins, and
  who the gateway will splice an SSH channel to.
- **SSH and the web terminal only — no desktops.** This is the deliberate
  simplification and it deletes most of the machinery below: no per-member
  Xvfb, no per-member Selkies, no display or port allocation, no on-demand
  streamer supervision. A member who wants a GUI gets their own single-user
  desktop VM; the project instance is the shared *server*.
- **One gateway, in the project's own namespace**, exporting the project's
  storage only — including each member's *project-local* home. Personal home
  PVCs stay on their own per-user gateway and are not mounted here
  ([below](#the-gateway-topology-one-per-project-and-it-holds-only-project-storage)).
- **Homes mounted at login, not at boot**, so a member who never logs in costs
  nothing — no NFS session, no published export ([below](#mounting-homes-on-demand)).
- **A volume is read-only for the instance if it is read-only for _any_
  member** ([below](#the-meet-rule-any-member-ro-means-ro)).

**The retired alternative — separate desktops on one machine — is kept below**
because the analysis is sound and the case may come back: it is what a
"virtual lab room" would need. What killed it is not that any single piece was
infeasible, but the count: `whistler-streamer@<member>.service`, deterministic
per-member display/port slots agreed between operator, guest and portal,
cloud-init growing an N-user shape, on-demand start *and* idle stop for each
streamer, and N GNOME sessions contending for one GPU. That is a large amount
of new, stateful, in-guest machinery whose only purpose is to avoid telling
someone to open their own desktop VM.

### What already fits

- **The session unit is already per-member.** `whistler-desktop@.service` is a
  template with `User=%i`; running several instances of it is what it was
  written for.
- **Per-member storage identity is solved** — one export per member, `/data` at
  one path via per-login mount namespaces, POSIX modes enforced server-side
  ([above](#one-mountpoint-and-ordinary-posix-modes-inside-it)).
- **A guest control path exists.** The portal already holds a per-user VM access
  key and drives SSH into running guests for screenshots, so there is a way to
  act inside a booted VM without inventing an agent.

### The meet rule: any member `ro` means `ro`

For a single user, grants **join** — the most permissive wins, because every
grant belongs to the same identity and a user who is `rw` on a volume in one
project does not lose that by being a read-only guest in another
([Group](#group), `get_user_volume_modes`).

On a project instance the rule **inverts**: a volume is published read-only if
it is read-only for *any* member. Take the meet, not the join.

The reason is that a project instance is one kernel with one root, and
[Shared instances](#shared-instances-and-shared-volumes) is explicit that
member-versus-member isolation is *not* claimed there — any member may act as
any other. So publishing a volume `rw` because alice is allowed to write it
hands write access to carol too, and carol's `ro` grant would be decoration.
The only way "carol may not write this" survives on a shared machine is if
nobody can write it *from that machine*.

Two consequences worth stating, because they are the cost of the rule:

- **A member's own single-user session is unaffected.** Alice still writes the
  volume from her own VM; she just cannot from the project instance while
  carol is a member of it. That is the correct place for the restriction to
  bite, and it is visible rather than silent.
- **Adding a read-only member downgrades the instance**, which is a surprising
  thing to happen on an "add member" action. It has to be surfaced at the
  moment of the edit — "adding carol makes /data read-only on this instance" —
  and it is one more reason membership changes want a restart boundary rather
  than a live `UpdateExport`.

### Why the export set becomes per-instance

Under the single-user model an export is a function of *the member*: alice's
home, squashed to alice's uid. The meet rule breaks that. `Access_Type` now
depends on **who else is on the machine the export is published to** — the
same project volume is `RW` on an instance whose members are all `rw`, and
`RO` on one that also has carol — so the same (member, volume) pair needs two
different exports at once. The export is therefore keyed by **(instance,
member, volume)**, and the gateway's export list becomes a projection of the
running instances rather than of the user list.

### The gateway topology: one per project, and it holds only project storage

**Decided (2026-08-16).** A project instance gets **its own gateway**, in the
project's own namespace, and that gateway exports **only the project's
storage** — never a member's personal home PVC.

**Why its own gateway.** Not primarily for simpler networking: because it
keeps the fence exactly the shape it already has.
[`_build_gateway_network_policy`](../whistler/config.py) admits
`podSelector: {app: whistler-desktop}` with **no `namespaceSelector`**, which
in NetworkPolicy means *pods in the policy's own namespace*. The per-user
boundary is therefore really the per-user namespace. Give a project its own
namespace holding its instance and its gateway, and that policy applies
verbatim, with no cross-namespace rule anywhere. The alternative — the project
instance dialling each member's per-user gateway — needs ingress from a
namespace that is not that user's, on *every member's* gateway, which widens
the one control that is the entire boundary on AUTH_SYS
([Substrate](#substrate-the-move-to-nfs-done)). One project gateway is the
narrower arrangement, not merely the tidier one.

**Why not personal homes on it.** The obvious extension — let the project
gateway mount each member's home PVC too, so the instance talks to one server
for everything — breaks the invariant that
[concurrent home mounts](#a-home-mounted-in-several-instances-at-once) rest
on: *"every consumer goes through one gateway, so byte-range locks are
coherent across instances — because it is the same server."* NFSv4 lock state
lives in the server. Two ganesha processes exporting the same directory tree
are two independent lock domains that cannot see each other, so a member with
their own VM up *and* a project instance up would have `$HOME` writable
through two servers with no shared locking — which is precisely the corruption
`nobrl` caused and the reason NFS replaced SMB. **Many exports over one
server is the design; many servers over one tree is not.**

It is also blocked in practice today: the home PVC binds `ReadWriteOnce` on
`local-path`, whose PV carries a `nodeAffinity` to the node that holds the
directory (verified on the dev cluster: `whistler-data-marma` → `wkstn`). A
second gateway pod must be co-scheduled to that node or stay `Pending`. RWX
storage would remove that obstacle and would **not** remove the locking one.

**So project instances get project-local homes.** Each member's home *on a
project instance* lives on the project's own storage, served by the project's
own gateway with that member's squash identity. Their personal home stays on
their personal gateway, mounted by their own single-user sessions, and moves
in and out over the SSH they already have (`scp`, `rsync`) — the login-node
arrangement, and normal for a shared server.

Three things fall out, and the third is the one worth having:

- One PVC is never mounted by two gateways, so lock coherence holds by
  construction rather than by scheduling luck.
- The project namespace is self-contained: instance, gateway, storage, one
  fencing policy of the existing shape.
- **A personal home never enters a project machine.** Under the
  mount-the-real-home version it would have to be granted in the project's
  zone, which — with the project restricted and the member's own work open —
  is the cross-zone grant the
  [matrix](#core-model-the-access-matrix) exists to make deliberate and
  visible. This arrangement never raises the question.

The cost is honest and small: dotfiles and environment do not follow a member
onto a project instance. Seeding a project home from a dotfiles repo at
creation is the obvious mitigation, and it is a convenience feature rather
than part of the model.

#### The rule holds for every storage class (measured)

The hypothesis worth testing was **transitivity**: if each layer honours
locks, a stack of NFS layers should too, so an NFS-backed PVC would move the
lock domain down to the backing NAS and let several gateways share one tree.
Measured on 2026-08-16 against a three-layer rig — ganesha as a stand-in NAS,
an in-tree `nfs:` PV so the kubelet performs the mount, a second ganesha as
the gateway, and clients reaching one file by both paths. **Transitivity does
not hold**, and two findings came out of it. The second is the one that
decides the design; the first decides whether the design can run at all.

**1. The VFS FSAL cannot export an NFS-backed PVC.** With the PVC mounted at
`/shares/home` as `nfs4` (`vers=4.1`, `local_lock=none`, readable, healthy),
ganesha refuses to build the export:

```
vfs_create_export :FSAL :CRIT :resolve_posix_filesystem(/shares/home)
                                returned No such file or directory (2)
```

It is not the handle problem overlayfs had — it is that an NFS filesystem
never enters ganesha's POSIX filesystem table at all. **This is a production
blocker, not a project-instance concern**: `csi-driver-nfs` mounts the share
on the node and bind-mounts it into the pod exactly this way, so the storage
gateway as it exists today cannot run on an NFS-backed storage class. Note
the failure is *quiet in the worst way* — ganesha still binds 2049 and logs
`NFS SERVER INITIALIZED`, which is precisely the case
[`gateway-ready`](../images/storage-gateway/gateway-ready.sh) was written to
catch. It catches it. Nothing else would.

**`FSAL_PROXY_V4` looked like the working shape and is not.**
`nfs-ganesha-proxy-v4` is packaged (the image installs only
`nfs-ganesha-vfs`); pointed at the backing server with `Srv_Addr` it exports,
mounts, serves I/O, preserves the squash identity (a root client writing
through the proxy produced a file owned `1000:1000` on the backing store) and
enforces byte-range locks among its own clients — **including past-EOF locks
at SQLite's `PENDING_BYTE` (0x40000000), the exact pattern cifs answered
`EACCES` to.** Every one of those passed.

Then a real SQLite workload put it on the floor:

```
Fatal glibc error: malloc.c:2601 (sysmalloc): assertion failed:
  (old_top == initial_top (av) && old_size == 0) || ...
```

**Heap corruption in the FSAL, crash-looping the server.** The visible
symptoms came first and were misleading — `sqlite3.DatabaseError: database
disk image is malformed` on a freshly created database, then a hang, then a
pod that would not terminate because its `hard` mount pointed at a server
that kept dying. Plain file I/O through the same path is *correct*
(sequential 1 MiB write/read, 50 random 4K read-modify-write cycles with
`fsync`, mmap read/write, and read-after-write across descriptors all verify),
and the identical SQLite test against plain ganesha VFS-over-`local-path` —
today's production shape — passes with `integrity_check: ok`. Same guest,
same mount options, same ganesha 6.5. Only the FSAL differs.

**Root-caused 2026-08-16 under valgrind:** PROXY_V4 `memmove`s a READ reply
from the backing server into the buffer `fsal_read2` allocated for the
client's request **without clamping the copy to that buffer**, landing `0
bytes after a block of size 8,192`. The glibc assertion above is the delayed
detection in an unrelated thread, not the fault. It also explains why the
checks above passed: they were all large, `rsize`-aligned reads where reply
length equalled request length, and the overflow needs those to diverge —
**a gateway can pass a block-I/O smoke test and still be unusable.** Full
analysis, and a runnable rig, in
[`images/storage-gateway/proxy-v4-heap-bug.md`](../images/storage-gateway/proxy-v4-heap-bug.md).

**A second finding there is a confidentiality problem in its own right**, and
it would survive a fix to the crash: on every OPEN, PROXY_V4 sends
**uninitialised heap bytes** to the backing server (valgrind: `Syscall param
write(buf) points to uninitialised byte(s)` in `proxyv4_compoundv4_execute`,
out of a 2 MB per-export buffer `malloc`'d and never zeroed). Since AUTH_SYS
is unencrypted and the backing NAS is outside this cluster's trust boundary,
that leaks gateway process memory — belonging to *whichever* user's gateway it
is — onto the storage network. Any future adoption of PROXY_V4 has to clear
this, not just the crash.

So `FSAL_PROXY_V4` in ganesha 6.5 (Debian trixie) is **not production
viable**: it corrupts its own heap under an ordinary database workload, which
is what a home directory is full of.

**A newer ganesha is not the way out, and 2026-08-16 settled that too.** The
offending `memcpy` is byte-identical in **6.5, 9.14 and 14.1** — the last
released 2026-08-05, i.e. current upstream. Worse, the newest is the one to
avoid: 6.5 and 9.14 crash, while **14.1 usually survives and silently returns
bytes that are not the file** (RPC framing from the proxy's own receive
buffer, with the backing store's copy provably intact). A crash-looping
gateway announces itself; a lying one does not. Any future attempt to revisit
this must re-run the read-back check in
[`proxy-v4-repro/`](../images/storage-gateway/proxy-v4-repro/) rather than
trusting a version bump — and must not treat "no crashes" as evidence.

**The conclusion is therefore a hard one: there is currently no working way to
run the storage gateway on an NFS-backed storage class.** VFS will not export
one; PROXY_V4 exports it and then returns the wrong data, crashing or not.
Anything that depends on `csi-driver-nfs` homes — `sharedHomeStorageClass`
included — needs a different FSAL, a fix landed upstream, or a substrate that
is not NFS underneath. **The home-as-virtual-disk arrangement below needs none
of them**, which is what makes it the answer rather than a workaround.

**2. Ganesha terminates locks; it does not forward them.** Measured for
`FSAL_PROXY_V4` — the VFS-over-NFS path could not be measured because, per
finding 1, its export does not build. With a holder holding an exclusive
`fcntl` byte-range lock through the proxy gateway:

| Client | Path to the file | Result |
| --- | --- | --- |
| control | same gateway as the holder | **REFUSED** (`EAGAIN`) |
| test | straight to the backing NAS | **ACQUIRED** |

The control is what makes the test mean anything: conflicts *are* detected,
ganesha enforces locks among its own clients — and the lock never reached the
backing server. So each ganesha is its own lock domain **whatever is behind
it**. The transitive argument fails not because a layer dishonours locks but
because ganesha does not pass them down; it answers them.

**3. This is not a Ganesha quirk — it is what re-export means.** The kernel's
own [NFS re-export
documentation](https://docs.kernel.org/filesystems/nfs/reexport.html) is
blunter about the same limit, for `knfsd`:

> Clients are not allowed to get file locks or delegations from a reexport
> server, any attempts will fail with operation not supported.

So the in-kernel server does not fare better; it fares *stricter*. Ganesha
grants a lock that is real among its own clients and meaningless outside
them; knfsd refuses to grant one at all. **The kernel's behaviour is the
safer of the two and would be catastrophic here** — no byte-range locks is
the `nobrl` situation that drove the move off SMB in the first place
([Substrate](#substrate-the-move-to-nfs-done)), so "just use knfsd" trades a
silent hazard for a loud regression. Neither implementation makes stacked NFS
lock-coherent, because nothing can: state lives in the server a client is
talking to.

The same page states the general form of the hazard, beyond locks:

> Open DENY bits are not enforced between clients accessing different reexport
> servers

**The rule, stated exactly.** The previous draft of this section said "one
server per tree, for every storage class", which is broader than what was
measured. Precisely:

- **Stacking NFS on NFS never preserves lock state.** Measured for
  `FSAL_PROXY_V4`, documented for `knfsd`. This is the rule that matters,
  because it is the arrangement an NFS-backed storage class produces.
- **One gateway per tree is therefore required whenever the tree can be
  reached by two servers** — which is every multi-gateway design discussed in
  this document.
- **Coherence, when it exists, comes from a shared filesystem below the
  servers, never from stacking them.** Two ganeshas on one node over one local
  mount would conflict correctly, because the VFS FSAL takes ordinary `fcntl`
  locks and the kernel arbitrates. The distributed form of that is
  `FSAL_CEPH` over CephFS, where Ceph's client library carries cluster-wide
  locking — the mainstream active/active ganesha deployment, and the only
  shape in which several gateways over one tree is a supported idea rather
  than a hopeful one.

Three further constraints from the same page, none of which Whistler pays
today but all of which arrive with a re-export substrate:

- **`fsid=` becomes mandatory** on any re-export, with a distinct UUID per
  export.
- **Filehandles grow by 22 bytes** (plus padding) per level of re-export,
  against NFSv4's 128-byte ceiling. One level is comfortable; the constraint
  is worth knowing before anyone nests a second.
- **Reboot recovery does not compose**: "the NFS protocol's normal reboot
  recovery mechanisms don't work for the case when the reexport server reboots
  because the source server has not rebooted, and so it is not in grace."
  Whistler restarts gateway pods routinely — the tuned `Lease_Lifetime` /
  `Grace_Period` in
  [`ganesha.conf.template`](../images/storage-gateway/ganesha.conf.template)
  exists for exactly that — so if `FSAL_PROXY_V4` is adopted, what a gateway
  restart costs a client holding locks needs re-measuring rather than
  inheriting the 9s stall measured on the local-PVC setup.

#### Node pinning puts the gateway's NIC in every session's I/O path

A per-node PV pins its gateway: `local-path` binds `WaitForFirstConsumer`, so
the volume is created wherever the first consumer lands and its `nodeAffinity`
holds it there for good (verified: `whistler-data-marma` → `wkstn`). The
gateway pod must then run on that node forever.

The cost is **bandwidth, not scheduling**. A VM never mounts the PVC — it
mounts NFS — so the guest is scheduled freely and the GPU node stays
available. But every byte it reads or writes crosses the *gateway's* network
interface. A gateway pinned to a 10G node serves a session on a 100G node at
10G, and no amount of free scheduling on the compute side changes that: the
storage path is only ever as wide as the narrowest link in it, and the pin
decides which link that is. Where the first instance happened to start
therefore sets the storage bandwidth of every later one. (*Pod* sessions
mount the PVC directly and are pinned outright — the existing single-user
constraint, unchanged.)

This is not new with project instances; the per-user gateway has it today.
What is new is that it is now the deciding argument for the gateway's
lifecycle.

#### Gateway per instance: the bandwidth answer, and what it costs

If a gateway serves exactly one instance it can be **placed with that
instance** — same node, so NFS traffic never leaves the box, which beats
"a fast link" outright. Overhead is one small ganesha pod per running
instance, and the normal case is a user with one or a few instances at a time.

It also *simplifies* three things that the per-project shape made awkward:

- **The export set stops needing an instance key.** [Why the export set
  becomes per-instance](#why-the-export-set-becomes-per-instance) had exports
  keyed by (instance, member, volume) because `Access_Type` depends on the
  co-tenants. With one gateway per instance the instance is implied by *which
  server you are talking to*, and each gateway's export list is just its own
  members. The meet rule becomes a local calculation.
- **Fencing becomes 1:1** — the tightest form the NetworkPolicy can take, one
  instance to its own gateway, instead of today's "any session pod in this
  namespace".
- **Lifecycle becomes ownership.** The gateway is created and reaped with the
  session CR through `ownerReferences`, rather than being a long-lived
  per-user Deployment nothing tears down.

Two conditions and two costs, and the first condition is decisive:

1. **It requires network-backed storage to deliver anything.** With
   `local-path` the PVC's `nodeAffinity` pins the gateway regardless of how
   many gateways there are, so per-instance gateways buy exactly nothing —
   they all land on the same node anyway. The bandwidth argument only cashes
   out once the volume can be mounted from any node.
2. **It cannot serve a home that two instances may mount — measured.** A user
   running two instances against one home is [normal, not
   exceptional](#a-home-mounted-in-several-instances-at-once), and per-instance
   gateways make that two servers over one tree. The hope was that an
   NFS-backed store would resolve the locks underneath; it does not
   ([above](#the-rule-holds-for-every-storage-class-measured)) — ganesha
   answers locks itself and never forwards them, so two gateways are two lock
   domains whatever is behind them. **The failure mode is quiet**: it needs
   two concurrent sessions with something SQLite-backed open in both, so it
   will not show up in testing and will show up as a corrupted profile later.
   This makes the fallback below the *default* arrangement rather than a
   contingency.
3. **Scheduling order inverts.** To co-locate, the gateway must learn the
   instance's node, so compute is placed first and storage follows it
   (`nodeSelector` from the VMI's node) — the reverse of today, where storage
   pins and compute is free. The guest then races its own gateway at boot;
   `hard` mounts and cloud-init's retry cover it, but it is a real ordering
   problem rather than a detail.
4. **Cold start grows.** Every instance start waits for a fresh ganesha to pass
   its readiness gate, where today the per-user gateway is already warm.

**Step 2 did fail, so this is the arrangement**: per-instance gateways for
*project and scratch* volumes — single-instance by nature, and the bulk
traffic, so they get the co-location and the bandwidth — with the home on the
one per-user gateway that owns its lock domain. A session then talks to two
servers: its own instance gateway for project data, its user gateway for
`$HOME`. That is one more NetworkPolicy rule and no new correctness question,
and it puts the wide traffic on the path that can be placed for it.

### What is single-tenant today

**Retired with the no-desktops decision above** — kept because it is the
inventory a future "several desktops on one machine" would have to work
through. Each of these is a concrete artifact that assumes exactly one user
per session; the SSH-and-terminal shape needs none of them touched.

- **The streamer.** `whistler-streamer.service` is a plain unit, baked enabled,
  `WantedBy=multi-user.target`, one Xvfb on `DISPLAY=:0`, one `SELKIES_PORT`
  read from `/etc/whistler/streamer.env`. It has to become
  `whistler-streamer@<member>.service` with a per-member display and port, and
  `whistler-desktop@%i` has to want *its own* streamer instance rather than the
  singleton it names today.
- **Display and port allocation.** Needs to be deterministic — the operator, the
  guest and the portal must agree without negotiating. Allocate from a **stable
  per-member slot recorded on the instance**, not from position in the member
  list, or adding a member renumbers the displays of everyone after them.
- **cloud-init.** Creates one user, enables one desktop unit, writes one
  `streamer.env`. It would need the whole member list: N users with N primary
  groups, N `authorized_keys` drop-ins, N units. Size is not the problem (the
  userData already travels via a Secret, so the 2048-byte inline cap does not
  apply); the problem is that cloud-init is **boot-time only** — see membership
  changes below.
- **The Service and the readiness probe.** `_build_session_service` publishes a
  single display port, and readiness gates on it
  (`readiness_port = display_port if desktop_stream else 22`). With streamers
  started lazily, *no* display port is listening at boot, so **readiness has to
  become sshd**, and the Service has to publish N ports.
- **The portal proxy.** `_resolve_desktop_base` maps a session to one
  `displayPort`, and `get_user_desktop_sessions(user)` scopes sessions to their
  single owner. Both assume one user per session. It needs
  `(session, member) -> port`, and it must refuse to route a member to another
  member's port. **This is the access control for the display plane** — there is
  nothing else enforcing it, since all the streamers listen on the same guest.

### Mounting homes on demand

Do not mount every member's home at boot. The guest-side mechanism is cheap:
`x-systemd.automount` on the per-member fstab entries, so the kernel triggers
the mount on first access, with `x-systemd.idle-timeout` to drop it again. The
entries are inert until touched, and a member who never logs in never causes an
NFS session.

The part that actually matters is on the other side. **Taint must be imprinted
when the export is published, not when the guest mounts it** — the operator
cannot observe a mount that happens inside a guest, and a rule enforced against
something it cannot see is not a rule. What the operator does control is which
exports it publishes to an instance.

So lazy mounting is only honest if **publication is lazy too**: the operator
adds the member's export when that member first connects, and records the
attachment at that moment. The conservative alternative — publish every
member's home at instance start — is safe and defeats the purpose. This also
sharpens the one-live-attach rule: "currently attached" should be read as
"currently published to a running instance", which is the operator's own
state, rather than as anything happening inside a guest.

**Lazy publication is now known to be possible**: `AddExport` over DBus works
on a running gateway with no restart of the gateway or the guest
([measured](#publishing-and-revoking-exports-while-the-gateway-runs-measured)).
The sequence for a member's first login is therefore: operator publishes the
export and records the attachment → the guest's automount unit fires on first
access → the member has their home. Both halves are lazy and the observable
one (publication) is the one carrying the rule.

Two design notes fall out of the protocol:

- **Mount the pseudo-root once, not each export.** NFSv4 has a single
  pseudo-filesystem per server, so a guest that mounts `/` sees exports appear
  as subtrees under their `Pseudo` paths as they are published — no new mount
  command per member, and nothing to re-run when the set changes. The
  single-user gateway mounts `<gw>:/home` specifically, which is right for one
  export and wrong for a set that grows at runtime.
- **`x-systemd.automount` still earns its place** for the idle-timeout half:
  dropping an idle member's mount is what keeps a logged-out member from
  holding an NFS session open.

### Starting streamers on demand

**Moot under the no-desktops decision** — recorded with the rest of the
retired desktop analysis. An Xvfb plus a Selkies per member is the expensive
part, especially with GNOME on llvmpipe or a single passed-through GPU, so a
streamer should start when its member first opens the desktop. Three ways, in
descending order of how much they need to be proven:

1. **systemd socket activation** on the member's port. The clean answer on
   paper, and it puts the trigger exactly where the first connection lands. It
   requires Selkies to accept a passed listening socket (`LISTEN_FDS`), which
   is unverified and probably absent — check before designing around it.
2. **Portal-driven start over the existing SSH path**, when the member opens
   the desktop. Uses machinery that already exists, at the cost of the portal
   knowing about guest internals.
3. **A small in-guest supervisor** the portal pokes. Most control, most new
   code, another thing to keep alive.

Stopping matters as much as starting: the streamer unit is `Restart=always`
today, which would fight any attempt to stop an idle one. A per-member streamer
wants `Restart=on-failure` plus an idle timeout, or an explicit stop when the
last client disconnects.

### Membership changes on a running instance

cloud-init runs at boot. Adding a member to a *running* project instance means
creating a user, a group, an `authorized_keys` drop-in, a mount unit and a
streamer unit inside the guest — none of which cloud-init can do afterwards.

The cheap and consistent answer is that **membership changes take effect on the
next boot**, which is exactly how zone membership already behaves. Adding a
guest-side agent to do it live is the alternative, and it should be a deliberate
decision rather than something that arrives by accident.

The storage half no longer forces the decision either way: exports can be
added, tightened and removed live
([measured](#publishing-and-revoking-exports-while-the-gateway-runs-measured)).
That makes the *guest* half the whole constraint — the user account, the
`authorized_keys` drop-in and the mount unit — which is a much smaller thing
to build an agent for later, and a good reason not to build one now.

Removal is the sharper case and does *not* decompose the same way. Revoking a
member has one part that is immediate and server-side — withdrawing their
export, which stops writes as that identity at once — and one part that is not:
their `authorized_keys` and their running session live in the guest. A
revocation that only removes the export leaves a logged-in member with a live
desktop and no storage, which is a confusing failure rather than a safe one.

### What this costs, in the model's own terms

- **`sudo` becomes all-or-nothing.** On a shared machine, granting one member
  root gives them every other member's data on that box. The per-user,
  per-instance grant in [`sudo` rights](#sudo-rights) cannot mean on a project
  instance what it means on a single-user one. Either no member gets it, or
  every member is told plainly that all of them effectively have it. Note this
  is *why* the meet rule exists: root on the box is reachable, so the only
  durable statement about a volume is one that holds for the whole machine.
- **The guest kernel becomes a boundary**, for the first time in this document,
  and [Assumptions](#assumptions) says it is not one. Members are separated by
  uid inside a single kernel, so a local privilege escalation is a cross-member
  breach. On single-user instances a kernel escape buys the attacker nothing —
  they already own the machine. Here it buys them the project. This has to be
  recorded as **accepted**, with the compensating control being that membership
  is small and known, not that the kernel holds.
- **Resource contention is a security-adjacent concern.** N desktops share CPU,
  RAM and one GPU, so a single member can starve the rest. Per-member cgroup
  limits, or an admission rule relating member count to instance size.
- **Attribution gets weaker.** Audit now has N candidate writers per volume
  ([Interaction with the access matrix](#interaction-with-the-access-matrix)).

### What this does *not* need

Worth stating, because each is a plausible-sounding detour:

- **Not Kerberos.** Per-member exports still give identity containment, and the
  members already share a kernel — the storage layer is not the weak link here,
  so buying cryptographic identity for it would be spending in the wrong place.
- **No new access machinery.** One instance sits in one zone, so the
  [matrix](#core-model-the-access-matrix) applies unchanged; only the
  "published, not attached" reading above is a refinement.
- **Not RWX storage.** The gateway remains the only mounter of each PVC — an
  invariant the [one-gateway-per-project decision](#the-gateway-topology-one-per-project-and-it-holds-only-project-storage)
  preserves rather than strains. Worth being precise about why RWX is not the
  missing piece: it would let two gateways mount one PVC, and two gateways
  over one tree are two lock domains. The obstacle is NFSv4 lock state, not
  Kubernetes access modes, so buying RWX would buy the wrong thing.

## When the only storage class is NFS

**The constraint (2026-08-16): `csi-driver-nfs` is the only production storage
class available, so Whistler has to work on it.** The findings above say the
gateway cannot. The way out is to notice that the blocker is far narrower than
"storage is broken":

- **Container and Kata sessions are unaffected.** They mount the home PVC
  directly (`_build_volume_wiring`), and mounting a `csi-driver-nfs` PVC into
  a pod is exactly what that driver does. No gateway is involved and nothing
  changes.
- **Only *VM homes* are blocked**, because a VM cannot mount a PVC — which is
  the sole reason the storage gateway exists at all.

**virtiofs is still not the answer**, and the rejection recorded in the
gateway image is not stale: kubevirt#13028 is closed as *not supported*.
PVC-backed virtiofs needs a **root container** in `virt-launcher`, and the
maintainer's summary is that *"virtiofs requires extra privileges in order to
change the uid/gid inside the fs"*, with rootless PVC virtiofs waiting on
enhancements (kubevirt/community#313). The qemu uid is hardcoded to 107.

### The home as a virtual disk

> **Adopted 2026-08-17, with one amendment: the image is per *instance*, not
> per user** — a home that follows a user is a channel between zones. The
> decision, the S3 tier for shared data, and the reach-versus-identity rule
> that governs both now live in **[storage.md](storage.md)**. The rest of this
> section is the reasoning that led there.

The arrangement that works on `csi-driver-nfs` today, with no new component
and no upstream dependency: give a VM its home as a **second virtio-blk disk**
backed by an ordinary PVC — `volumeMode: Filesystem` on the NFS share puts a
`disk.img` there — and let the guest format it and mount it at `$HOME`.

This is not exotic: **it is the mechanism the VM root disk already uses**, and
`_build_vm_spec` already emits `disks`/`volumes` and already attaches a
PVC-backed root via `dataVolumeTemplates`. The docstring's "the user's home
PVC is NOT attached" is the line that changes.

What it deletes is most of this document's storage difficulty:

- **No ganesha on the VM path** — no NFS-on-NFS, no FSAL to crash, no
  `resolve_posix_filesystem` refusal.
- **No identity problem.** A block device is opaque to the host and owned
  exclusively by one VM, so there is nothing to squash and no AUTH_SYS
  credential to forge. The uid question disappears rather than being contained.
- **No 2049 fencing surface on the VM path.** Today that NetworkPolicy is
  "the entire boundary" for storage; here isolation comes from the PVC binding
  and the hypervisor, which is a stronger boundary, not a weaker one.
- **Locking becomes ordinary local kernel locking** on ext4. Every question in
  this section — lock domains, re-export, `local_lock`, SQLite — stops
  applying.

The costs, stated plainly:

- **A home stops being one object across session types.** Today `Squash` lands
  VM writes on the PVC as the user's real uid, "consistent with pod sessions
  mounting the same PVC directly"; with a disk image a pod session sees an
  opaque file. The honest resolution is that **the VM's home is the home**,
  and container sessions — already described as throwaway workspaces — get
  their own volume. That is closer to the stated runtime split than the
  current shared-PVC arrangement is.
- **No shared homes**, because a block device cannot be safely multi-attached.
  Shared *volumes* on VMs still need a file-level share, so they still need
  either a working gateway or virtiofs — but that is a later feature and does
  not block single-user VMs now.
- **Admin loses file-level inspection and backup** of a home; it is an image.
- **Resizing** needs a guest-side step after the PVC grows.

If the gateway is wanted back later, two avenues remain and both are
uncertain: root-causing ganesha's refusal to put an NFS mount in its
filesystem table, or a ganesha newer than Debian trixie's 6.5 in which the
`FSAL_PROXY_V4` heap corruption may be fixed. Neither is on the critical path
for a deployment that takes the disk-image route.

## Shared homes as a deployment option

**Proposed 2026-08-16.** Rather than Whistler guaranteeing that a shared home
works, make it an **option the deployment enables and owns**: `allowSharedHome`
in values.yaml gates whether the option appears at all, and
`sharedHomeStorageClass` names the class it is backed by. Sharing a home is
then a deployment's decision with a deployment's storage behind it, and the
advisories that go with it — what may be run against a shared home, what
co-ordination users are expected to observe — belong to onboarding rather
than to code. **Shared homes stay explicitly denied on project instances**,
consistent with [the meet rule](#the-meet-rule-any-member-ro-means-ro).

Three things this has to reckon with:

- **A shared home and a per-instance gateway are in direct conflict.** If
  every instance has its own gateway, two instances mounting one shared home
  is two servers over one tree — the arrangement
  [measured broken](#the-rule-holds-for-every-storage-class-measured). No
  storage class fixes it, because the fault is the stacking, not the backing.
  The resolution is to make the gateway **per *tree*, not per instance**: a
  private home gets a gateway co-located with its instance (the bandwidth
  win), and a *shared* tree gets exactly one gateway that every consumer of
  it mounts. An instance then mounts several gateways — one per tree it can
  see — which costs a NetworkPolicy rule per tree and keeps every tree
  single-served.
- **"Non-home shared volumes don't have the locking problem" is true of
  exposure, not of mechanism.** Two gateways over a shared *project* volume
  are as incoherent as two over a home; what differs is that project data is
  usually files people co-ordinate on socially, while a home is full of
  SQLite that corrupts quietly. Worth writing in the advisory that way, so
  nobody later reads "project volumes are fine" as a property of the storage
  rather than of the workload.
- **Sidecar is the wrong word for a VM.** KubeVirt does not let an arbitrary
  container be added to the `virt-launcher` pod (the `hooks.kubevirt.io`
  sidecar mechanism is for domain-XML mutation, not for running services), so
  a per-instance gateway is a *separate pod* co-scheduled with `podAffinity`
  toward the VMI — which also means compute is placed first and the gateway
  follows.

And the blocker above applies directly: `sharedHomeStorageClass` pointing at
`csi-driver-nfs` does not work today, in either FSAL.

### Local locking is not an option here, and is not needed

The tempting shortcut is to mount with `local_lock=all` — keep every lock
inside the guest, let cross-instance coordination be the user's problem, and
buy shared homes with advisories. **It is not available.** `local_lock` and
`nolock` live in the *"Options for NFS versions 2 and 3 only"* section of
`nfs(5)` and are defined in terms of the **NLM sideband protocol**, which
NFSv4 does not have — locking is integral to the v4 protocol. Requesting it
on a v4.1 mount is silently ignored: a mount asking for `local_lock=all` comes
up reporting `local_lock=none` (verified).

Getting it would mean going back to NFSv3, which gives up the reasons v4 was
chosen in the first place
([`ganesha.conf.template`](../images/storage-gateway/ganesha.conf.template)):
NLM and statd mean rpcbind and a set of sideband ports, where v4 has "exactly
one port to fence" — and on AUTH_SYS that fencing NetworkPolicy *is* the
boundary. It would also lose the v4.1 backchannel property that lets the
fencing stay ingress-only. A convenience feature is not worth widening the one
control the storage model rests on.

**It is also unnecessary**, because server-side locking through a single
gateway already delivers what local locking was wanted for, and more:

- *within* an instance, locks work — measured, including SQLite's past-EOF
  `PENDING_BYTE` pattern;
- *across* instances that share a tree **through the same gateway**, locks
  also work — that was the control in the experiment above.

What local locking would have bought is tolerance for *several gateways over
one tree* — and it would not have bought that either: two guests locking
locally have no coordination at all, which is precisely the corruption the
arrangement was trying to permit. So the answer is the same one the rest of
this section reaches: **one gateway per tree**, and a shared home is a tree
with one gateway that all its consumers mount.

The advisories still have a job, just a smaller and more honest one: they
cover what locking cannot fix in any configuration — `~/.config/dconf/user`
coordinating through a machine-local flag file, `XDG_CACHE_HOME` on shared
storage, two desktops against one home
([Session state](#session-state-cache-config-state)). And "don't start two
instances at once, even though you technically can" is worth *enforcing*
rather than advising: refusing to start a second instance against a
shared-home tree is a rule the operator can actually keep, and a rule beats
a sentence in an onboarding document.

## The storage-class experiment (done, 2026-08-16)

Run, with results in
[The rule holds for every storage class](#the-rule-holds-for-every-storage-class-measured).
Recorded here because the rig is worth rebuilding whenever the storage
substrate changes, and it needs **no privileged pod**: an in-tree `nfs:`
PersistentVolume makes the *kubelet* perform every mount, and the existing
[`images/storage-gateway/`](../images/storage-gateway/) image serves as the
stand-in NAS. Three layers, all namespaced:

1. **backing** — the image, exporting a `local-path` PVC. Stands in for the NAS.
2. **gateway** — the image again, consuming an in-tree `nfs:` PV pointed at
   *backing*. This is the `csi-driver-nfs` shape exactly: kubelet mounts NFS,
   pod sees it at `/shares/home`.
3. **clients** — `python:3-slim` pods with in-tree `nfs:` PVs, one per path
   under test, taking `fcntl` byte-range locks.

Two lessons about the method, both learned the hard way:

- **Always run the positive control.** "The conflicting lock succeeded" and
  "this rig cannot detect conflicts at all" look identical. A second client on
  the *same* gateway is the control, and it must come back `REFUSED` before
  the cross-path result means anything.
- **Verify the lock is still held when the test runs.** A holder that has
  already released turns the whole experiment into a measurement of nothing;
  the first run here did exactly that and had to be discarded.
- **Tear the clients down before their servers.** A force-deleted pod skips
  the unmount and strands a `hard` NFS mount on the node whose server no
  longer exists, which then blocks the namespace and the PVs from finalising.
  Delete client pods gracefully first, or expect to revive the server address
  to let the unmount complete.

## Open questions

- **An instance with no persistent home is a data-loss trap.** It is the
  cleanest onboarding path and worth having, but the failure mode is an admin
  forgetting to attach a home volume and a user's work vanishing at the next
  reboot, silently and unrecoverably. It needs a deliberate guardrail —
  instances default to requiring a home, "no persistent home" is an explicit
  choice, and the session says so visibly.
- **Can the desktop clipboard be turned off server-side?** A desktop-only
  posture is not a boundary until it can
  ([Access channels](#access-channels-the-second-axis)). Unverified whether
  Selkies 2.x has a flag; the fallbacks are the build-time patch pipeline or
  dropping `xclip` from a restricted image variant. This blocks the external-
  researcher case, so it is the first thing to check, not the last.
- **Screenshots need a per-zone setting, not a global width.** They are
  monitoring *and* egress, and today the only dial applies to every session
  in the cluster.
- **Channel grants conditioned on source network** — worth having, worth
  being honest that it correlates with the endpoint rather than proving it.
- **Audit trail and UI for the matrix.** Who changed which cell, and when. It
  carries more weight now than under taint: opening a cell is the whole
  declassification story, so the record of it is the only thing left that says
  a deliberate act took place.
- **Two restricted collections that must not mix.** The matrix expresses this
  naturally where a gradient could not — they are simply different volumes
  with no shared `allowed` zone — which is one of the reasons it replaced the
  levels. Worth confirming against a real case before claiming it is solved.
- **Kerberos, and the three things that would make it necessary.** Per-member
  exports cover the identity containment this model actually requires, so
  `sec=krb5` is not on the critical path. It becomes the answer — and the only
  answer — if any of these turns up:
  1. **Multi-group POSIX inside one volume.** Squashing pins one gid and drops
     supplementary groups; pass-through restores them and forfeits containment.
     Only real per-user credentials give both
     ([One mountpoint](#one-mountpoint-and-ordinary-posix-modes-inside-it)).
     Expressing subgroups as separate volumes is the cheaper way out, and is
     the current answer.
  2. **Wire encryption.** `krb5p` is the only route back to what `seal` gave
     under SMB. Independent of everything else here, and still open.
  3. **Member isolation that survives root**, which also means no root on the
     instance, since root can steal another member's credential cache.

  Investigating it means a KDC, principal and keytab lifecycle, clock sync, and
  `rpc.gssd` in every guest image — so it should be triggered by one of the
  three above, not adopted pre-emptively.
- **Export lifecycle.** Adding or removing a member, or flipping a volume
  between `rw` and `ro`, means editing exports and reloading ganesha. A reload
  path exists (SIGHUP, or the DBus `ExportMgr` interface — DBus is not wired up
  in the gateway image today), but neither has been tested. Everything in
  [Shared instances](#shared-instances-and-shared-volumes) assumes this works
  without dropping live mounts.
- **Project instances beyond storage.** The storage half is designed above; the
  rest is not. Whose `sudo` policy applies, who may connect to the session at
  all and how that is enforced (the SSH/console path, not the gateway), and
  what happens to a running project instance when membership changes.

**Resolved and folded in:** the access matrix replaces taint and security
levels (2026-08-19); no migration path for existing homes is needed (nothing is
in production); revoking access flags instances for reboot rather than applying
live; read-only enforcement is a
server-side `Access_Type = RO` export and needs no separate mechanism, since
the published export set already encodes it; multi-user and project-wide
instances are no longer deferred wholesale — their storage model is
[above](#shared-instances-and-shared-volumes).
