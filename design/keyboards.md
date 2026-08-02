# Keyboard input: how a keystroke gets from a Mac to a guest app

This is a reference, not a changelog: it documents the full path a keystroke
takes, what's authoritative at each stage, and the failure modes discovered
at each one — so that the next time a character comes out wrong, there's a
map of where to look instead of starting from zero. See
[Current deployment status](#current-deployment-status) for what's actually
shipped vs. still pending.

## The pipeline

```
Mac physical key
  → macOS active input layout           (System Settings → Keyboard, e.g. "Swedish")
  → browser KeyboardEvent                (key/code/keyCode + modifier flags)
  → Selkies web client JS                (client-side Mac fixups, keysym encoding)
  → WebSocket: kd,<keysym> / ku,<keysym> / co,end,<text>
  → Selkies Python server (input_handler.py, vendored, not our code)
  → one of several injection mechanisms  (picks an X11 keycode + modifiers)
  → guest X11 server, active XKB group   (which "layout" is currently selected)
  → XKB key type for that keycode        (governs modifiers→symbol, per key)
  → delivered X11 KeyPress/KeyRelease → focused app
```

Two independent things determine what character comes out the other end,
and they are **not the same setting**:

- **macOS's active input layout** (host side) — governs what keysym the
  *client* resolves a physical key to and sends over the wire. This is a
  property of the OS's current layout selection, **not of the physical
  keyboard's printed legends**. Confirmed directly: two physically different
  keyboards (one with Swedish-printed keycaps, one an external US-legend
  keyboard), both with macOS's active layout set to `sv`, produce identical
  results — the physical key printing is irrelevant, only the OS layout
  selection matters.
- **The guest's active GNOME input source / XKB group** (guest side) —
  governs how the *server* turns a received keysym into an actual X11
  keypress, and therefore what the guest's X server and apps see.

Everything downstream assumes these two are the same "language" the user
intends to type in. When they're not, see
[Out of scope: host/guest layout mismatch](#out-of-scope-hostguest-layout-mismatch).

### Stage: browser capture and client-side Mac fixups

The web client (Selkies 2.x, pinned commit
`5686f6c4d20ed63a27e253bac00fb89ef99828c8`, see
[creating_desktops.md](creating_desktops.md)) sends `kd,<keysym>` /
`ku,<keysym>` text frames over the same WebSocket as the video, or
`co,end,<text>` for a resolved printable string. **`co,end` — not `kd`/`ku`
— is what a Mac browser sends for many shifted symbols**: macOS resolves the
shifted character (e.g. `<`) before the browser ever sees a distinct Shift
keydown to track, so no modifier chord reaches the server for these — the
server has to treat `co,end` characters as arriving with no genuinely-held
modifiers at all. This asymmetry (same character, two different message
shapes depending on how the OS resolved it) is a recurring source of bugs
because it means the two are easy to only partially handle.

macOS also has no OS-level equivalent for several X11/Linux concepts, which
the client (JS) and a companion X11 agent handle explicitly — this part is
fixed and shipped, in `mac-cmd-chords.patch` (both copies —
[desktops/streamer-selkies2/](../desktops/streamer-selkies2/mac-cmd-chords.patch),
[desktops/vm-gnome-selkies/bake/](../desktops/vm-gnome-selkies/bake/mac-cmd-chords.patch))
and [whistler-copy-agent](../desktops/streamer-selkies2/whistler-copy-agent):

- **Cmd → Ctrl swap.** Upstream only re-fires the swap while `Meta` is still
  in the client's `_keyDownList`; the first chord removes it, and macOS
  reports `ctrlKey=false` on Cmd-chord key events, so every chord after the
  first typed the bare letter instead of firing the shortcut. Fixed to keep
  re-firing while Cmd stays physically held.
- **"Copy" has no single answer.** Ctrl+C is SIGINT in a terminal; VTE's
  Shift+Insert pastes PRIMARY, not CLIPBOARD (measured, not assumed). Plain
  Cmd-C/V now send dedicated XF86Copy/XF86Paste taps; `whistler-copy-agent`
  grabs those and re-injects the chord the focused window's `WM_CLASS`
  actually expects (Ctrl+C/V vs Ctrl+Shift+C/V).
- **Mode_switch vs ISO_Level3_Shift.** Mac's Option-key remap targets the
  legacy `Mode_switch` keysym, which no modern XKB layout binds (layouts use
  `ISO_Level3_Shift` instead) — so it was unbound on every guest regardless
  of layout, and the server's injector fell through pynput→xdotool→a
  hardcoded 1-second timeout trying to type it. `whistler-copy-agent`
  pre-binds both keysyms to spare keycodes at startup.

Full narrative: auto-memory `mac-cmd-chord-bug-and-fix.md` (not in git).

### Stage: server-side injection (the crux of nearly every bug found so far)

`input_handler.py` inside the pip-installed `selkies` package — **vendored,
not our code**, pulled at `pip install` time in both
[streamer-selkies2/Dockerfile](../desktops/streamer-selkies2/Dockerfile) and
[vm-gnome-selkies/bake/Dockerfile.builder](../desktops/vm-gnome-selkies/bake/Dockerfile.builder)
— has three call sites that each convert an incoming keysym or text string
into actual X11 keypresses, selected by which message shape the client sent
and whether a modifier chord is currently tracked:

1. **`send_x11_keypress`'s `command` branch** — printable non-alpha
   characters when a modifier chord *is* tracked server-side.
2. **The `co,end` message handler** — printable non-alpha characters with
   *no* modifier tracked (the common Mac-shifted-symbol case above).
3. **`send_x11_keypress`'s pynput branch** — alphabetic characters
   (`char.isalpha()`, Unicode-aware — covers å/ä/ö too).

Upstream, all three delegate this to `xdotool`/`pynput`'s own
keycode/modifier resolution. That resolution has **no concept of which XKB
group is currently active**, and the guest here routinely has more than
one: any session with multiple GNOME input sources configured (`whistler-
desktop`'s Settings → Keyboard → Input Sources, persisted via dconf on the
home volume) gets an XKB keymap with multiple *groups* on the same physical
keycodes — e.g. keys mean one thing under `se`, another under `us`. This is
the root of the whole bug family; two independent, both now fixed,
mechanisms of failure fall out of it:

**1. Group-blind keycode/modifier selection.** `xdotool`/`pynput` can pick
a keycode+modifier combination that only produces the intended character
*under one specific group*. Confirmed directly: `xdotool key -- less`
resolved via the *physical Z key* + Shift+AltGr — valid only because that
combination happens to mean `<` under `se` specifically; under `en` the
receiving app just saw Shift+Z. Two secondary effects compound this:

- *Stuck modifiers.* `xdotool`'s `--clearmodifiers` cleanup doesn't always
  release what it pressed — after a `>`, `XQueryKeymap` showed AltGr
  (`ISO_Level3_Shift`) genuinely still held, corrupting the *next* keystroke
  (`<` became `|`, exactly AltGr+less on that keymap).
- *Dynamic-remap races.* If a keysym isn't already reachable, `xdotool`/
  `pynput` temporarily remap a spare keycode, which races against GNOME
  Shell's own async keymap reconciliation — first use after any keymap
  change can be slow or wrong; repeats are fast and correct once "warmed
  up." (Same underlying mechanism as the Mode_switch bug above.)

**2. `GetKeyboardMapping`'s flat index is not "the level."** Even once you
bypass `xdotool`/`pynput` and query the live keymap directly
(`get_keyboard_mapping()`), the returned list flattens *all* XKB groups into
one wide per-keycode array. The classic core-protocol convention treats
`index % 4` as a "level" (`0`=base, `1`=shift, `2`=AltGr, `3`=shift+AltGr) —
but that convention only actually holds for `index < 4` (group 0); `index
>= 4` belongs to an entirely different group (confirmed: keycode 53 had
`greater` sitting at index 5 — group 1 — and naively treating `5 % 4 == 1`
as "just needs Shift" delivered group 0's real level-1 symbol there
instead). **And even within group 0, index 2/3 is not reliably
"AltGr"/"Shift+AltGr."** That mapping is actually governed by each key's XKB
**"type"** definition (`TWO_LEVEL`, `FOUR_LEVEL`,
`FOUR_LEVEL_SEMIALPHABETIC`, etc.), which core-protocol
`GetKeyboardMapping` does not expose at all — confirmed directly: injecting
a keysym with *exactly* the modifier state (`Shift`+`Mod5`/AltGr, verified
genuinely held via `XQueryKeymap`) that the naive index arithmetic said was
correct still delivered the wrong character (`^` → `√`, `|` → `×`) on this
particular Swedish group's AltGr-level bindings. There is no way to recover
the real type table from `GetKeyboardMapping` — it lives in the XKB
extension, not core protocol.

**3. A long-lived Xlib connection's convenience methods cache the keymap.**
The server holds one persistent `Display` connection; its
`keycode_to_keysym`/`keysym_to_keycode` methods use an internally cached
mapping that does **not** track GNOME's live keymap reconciliation.
Confirmed: a *fresh* connection found colon at keycode 60/level 1, while the
long-lived connection's cache didn't have it there and fell back to a
level-3 binding elsewhere. Any fix that queries the keymap must use an
explicit `get_keyboard_mapping()` call (a real protocol round-trip) every
time, never the cached convenience methods.

### The fix now in place

Two shared helpers, used by all three call sites instead of `xdotool`/
`pynput`'s own resolution (full source: `keymap-injection.patch`, see
[Current deployment status](#current-deployment-status)):

- **`_whistler_resolve_keycode(keysym)`** — live `get_keyboard_mapping()`
  query (never the cached convenience methods), strictly preferring a match
  at `index < 4` (group 0, the group-independent quad) over any `index >=
  4` match, falling back to the latter only if nothing in group 0 matches.
- **`_whistler_xtest_inject(keysym, down)`** — for level ≤ 1 (no AltGr
  needed), injects directly via `Xlib.ext.xtest.fake_input`, explicitly
  synthesizing Shift only if the target level needs it and it isn't already
  genuinely held (checked against `self.active_modifiers`, not `xdotool`'s
  own bookkeeping), tracking exactly what each keydown synthesized so the
  matching keyup releases precisely that. **For level ≥ 2 (AltGr-involving),
  delegates to `_whistler_remap_inject`** instead of guessing a Shift/AltGr
  combination: it sidesteps the whole XKB-type ambiguity by temporarily
  remapping an unused ("scratch") keycode directly to the target keysym via
  `XChangeKeyboardMapping`, pressing that keycode bare with **no modifiers
  at all**, then restoring it to `NoSymbol` on release. With no modifiers in
  play, only "index 0" is ever read — unambiguous under every XKB type,
  regardless of group or type definition. Scoped to level ≥ 2 only (level ≤
  1 was already reliable, and remapping broadcasts an X `MappingNotify` to
  every client on the display — worth avoiding for the common case).

## Out of scope: host/guest layout mismatch

Testing GNOME's `en` input source while macOS's active layout stayed `sv`
(deliberately, to probe the boundary) produced further discrepancies: `]`/
`}` came out as `¨`/`^`, and Shift+7/8/9 didn't match a real US keyboard's
`*()`. This is **not a bug** — it's the architecture working as described
above: the client only ever sees what macOS's *active layout* resolved a
physical key to, with no visibility into the physical key position or the
guest's intended language. Pointing a `sv`-configured Mac at a guest
`en` group will never produce "what a real US keyboard would send," because
the browser never had access to that information — macOS resolved the
character (or dead-key/AltGr binding) as Swedish before the browser's
`KeyboardEvent` even fired. There is no fix for this short of a materially
bigger feature — bidirectional layout translation, mapping the *guest's*
intended layout back through the *host's* physical key positions — which is
explicitly out of scope. The takeaway to preserve: **match the guest's GNOME
input source to the host OS's active layout** for correct results; anything
else is an unsupported combination by design, not a regression to chase.

## Debugging toolkit

- **`WHISTLER-DEBUG` logging**, present throughout the patched
  `input_handler.py` (grep that string) at `DEBUG` level (not `INFO` — it
  fires on every keystroke through these paths, so raise the `webrtc_input`
  logger to `DEBUG` to see it): entry/outcome/timing at every branch of
  `send_x11_keypress` and `_xdotool_fallback`, a raw log line at the very
  top of the `kd` dispatch (keysym + `active_modifiers` before any routing
  decision — the single most useful line for figuring out *which* of the
  three paths a given keystroke took), and one at the `co,end` handler.
  `sudo journalctl -u whistler-streamer -f | grep WHISTLER-DEBUG`.
- **Raw X11 event observation** is the only ground truth. A small
  python-Xlib receiver window (`create_window` + `KeyPressMask|
  KeyReleaseMask`, focus via `set_input_focus` in a retry loop) logs what
  the X *server* actually delivered `(keycode, keysym, state)` — `xdotool`
  reporting `returncode=0` does **not** mean the right character landed.
- **`XQueryKeymap`** is how to catch stuck modifiers (a synthesized AltGr or
  Shift that didn't get released, corrupting the next keystroke) — check it
  after any modifier-synthesis change.
- **Known pitfalls that cost real debugging time, avoid repeating them:**
  - A hand-rolled WebSocket client that skips the real client's
    SETTINGS/`cr` handshake gets silently ignored by the server — drive
    repro tests through the actual client bundle (Playwright/CDP) or
    replicate the full handshake.
  - CDP `dispatchKeyEvent`'s `modifiers` bitmask alone does **not** make the
    client's own JS treat a keystroke as a real chord — send an actual
    preceding `Shift` keydown/keyup as separate events.
  - After any display-stack restart, `gnome-terminal` needs
    `DBUS_SESSION_BUS_ADDRESS` re-derived from the **new** `gnome-shell`
    PID's `/proc/<pid>/environ` — a stale value silently times out
    `StartServiceByName`.
  - A receiver window can lose the focus race against a
    freshly-restarted GNOME Shell; add a retry loop and a startup delay.
  - A receiver logging `keycode_to_keysym(keycode, 0)` **always** shows the
    unshifted symbol regardless of the event's real `state` bits — cross-
    reference `state` against the actual keymap, or correctly-delivered
    shifted characters will look like phantom bugs.
  - To confirm what actually lands in an app (not just what was injected),
    focus a real window (e.g. `cat > /tmp/out.txt` in a terminal) and read
    back its content — a receiver window only proves what the X server
    delivered, not what a specific app's own input handling did with it.

## Current deployment status

**Formalized as `keymap-injection.patch`, applied at build time — not yet
committed to git (working tree only).** Present in both
[desktops/streamer-selkies2/keymap-injection.patch](../desktops/streamer-selkies2/keymap-injection.patch)
and
[desktops/vm-gnome-selkies/bake/keymap-injection.patch](../desktops/vm-gnome-selkies/bake/keymap-injection.patch)
(kept in sync, same convention as `mac-cmd-chords.patch`), applied via
`patch -p1` to the extracted selkies source **before** `pip install` in both
Dockerfiles (each stage's apt-get list gained `patch`). Generated as a real
`diff` against the pip-installed `.orig`, so the patch is exactly what
real-keyboard testing exercised, not a hand-rewritten approximation.

Verified: real Mac keyboard testing (`sv`, matched host/guest layout) for
every character in the failure catalog above, `XQueryKeymap` confirming no
stuck modifiers; two standalone Docker builds (one per Dockerfile, both
cleaned up after) confirming the patch applies and the resulting venv
installs and imports cleanly on both target Python/Ubuntu ABIs. **Not yet
verified:** through the real skaffold/image pipeline, or on pod sessions
(`streamer-selkies2` sidecar, shared by `vm-xfce-selkies`) — only the GNOME
VM has been exercised so far, despite the bug family needing nothing
GNOME- or VM-specific to trigger (just a multi-group guest keymap).

### Next steps

1. Commit `keymap-injection.patch` (both copies) and the two Dockerfile
   changes.
2. Run through the real build (skaffold dev / a full image build) and
   re-verify on the actual `streamer-selkies2` pod path and
   `vm-xfce-selkies`.
3. Worth reporting upstream: all root causes are genuine Selkies bugs
   (group-blind character resolution, `--clearmodifiers` modifier leaks, a
   `GetKeyboardMapping`-index vs. XKB-type mismatch for AltGr levels), not
   Whistler-specific — though the trigger (multiple switchable guest input
   sources) may be unusual enough that upstream hasn't hit it.
