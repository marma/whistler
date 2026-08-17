# FSAL_PROXY_V4 returns the wrong bytes, and overflows the read buffer doing it

**Status: root-caused 2026-08-16, and present in current upstream.**
`FSAL_PROXY_V4`'s read path copies the backing server's reply into the caller's
buffer using **the length off the wire, never clamped to the buffer it was
given** — `proxyv4_read2`, [`src/FSAL/FSAL_PROXY_V4/handle.c`][hc], the
`memcpy` under the comment *"Copy the read buffer - unfortunately we can't
avoid a data copy here..."*. That line is **byte-identical in V6.5, V9.14 and
V14.1**, the last released 2026-08-05.

[hc]: https://github.com/nfs-ganesha/nfs-ganesha/blob/V14.1/src/FSAL/FSAL_PROXY_V4/handle.c

It has two faces, and the quiet one is worse:

- On **6.5 and 9.14** it corrupts the heap and the process dies. The glibc
  assertion originally reported is the delayed *detection*, several
  allocations later and in an unrelated thread.
- On **14.1** it usually does not die. It just **hands the client bytes that
  are not the file** — RPC framing from the proxy's own receive buffer — while
  the data on the backing store is perfectly intact. A crash loop is loud;
  this is not.

A runnable rig is in [proxy-v4-repro/](proxy-v4-repro/). `./repro.sh` shows
both failures in about ninety seconds and needs no SQLite; `./repro.sh
valgrind` names the offending memmove with a source line.

## Why it matters

`csi-driver-nfs` is the only production storage class available for this
deployment, and the Whistler storage gateway cannot run on it:

- **`FSAL_VFS` cannot export an NFS-backed PVC at all** — a separate problem,
  not this one. Ganesha refuses to build the export with
  `vfs_create_export :FSAL :CRIT :resolve_posix_filesystem(/shares/home) returned No such file or directory (2)`,
  even though the mount is present and healthy (`nfs4`, `vers=4.1`,
  `local_lock=none`, readable). An NFS filesystem never enters ganesha's POSIX
  filesystem table.
- **`FSAL_PROXY_V4` does export it** — and then corrupts its own heap, which is
  this bug.

So PROXY_V4 was the only working shape, and this bug is what closed it off.
See [design/security.md](../../design/security.md), "When the only storage
class is NFS", for the surrounding decisions — including the home-as-virtual-disk
plan that removes ganesha from the VM path entirely and makes this bug stop
mattering to Whistler.

## Root cause

Valgrind against a V14.1 build with symbols (the Debian packages ship no
dbgsym for the proxy FSAL, so this needed a source build):

```
Invalid write of size 8
   at 0x485268B: memmove (vg_replace_strmem.c:1414)
   by 0x59DF86A: proxyv4_read2 (handle.c:2597)
   by 0x4A0276E: mdcache_read2 (mdcache_file.c:617)
   by 0x49C0C6F: nfs4_read.isra.0 (nfs4_op_read.c:1002)
   by 0x49C197F: nfs4_op_read (nfs4_op_read.c:1079)
 Address 0xede2020 is 0 bytes after a block of size 8,192 alloc'd
   at 0x4844818: malloc (vg_replace_malloc.c:446)
   by 0x48A5BEC: gsh_malloc (abstract_mem.h:100)
   by 0x48A5BEC: get_buffer_for_io_response (fsal_helper.c:1967)
   by 0x48A5CA7: fsal_read2 (fsal_helper.c:2017)
```

The source says the rest. In `proxyv4_read2`:

```c
iov_len = read_arg->iov[0].iov_len;         /* the caller's buffer size    */
if (iov_len > maxReadSize)
        iov_len = maxReadSize;              /* the REQUEST is clamped down */
...
resok->data.data_len = read_arg->io_request;   /* io_request, not iov_len! */
...
/* Copy the read buffer - unfortunately we can't avoid a data copy here... */
assert(resok->data.iovcnt == 1);
assert(read_arg->iov_count == 1);

memcpy(read_arg->iov[0].iov_base, resok->data.iov[0].iov_base,
       resok->data.iov[0].iov_len);            /* length from the WIRE     */
read_arg->iov[0].iov_len = resok->data.iov[0].iov_len;
```

Three things line up:

1. `fsal_read2` → `get_buffer_for_io_response` allocates the destination
   **sized to `iov[0].iov_len`**.
2. The proxy clamps the *outgoing request* to `maxReadSize`, but advertises
   the decode capacity as **`io_request`**, a different field. When those two
   diverge, the decoder's idea of how much it may produce stops matching the
   buffer that has to hold it.
3. The `memcpy` back then uses **`resok->data.iov[0].iov_len` — the length the
   backing server returned — with no clamp to the destination at all.** The
   two `assert`s directly above it check `iovcnt`, the one thing that cannot
   hurt you; the length, which can, is unchecked.

So the fix is a bounds check that isn't there. It is not subtle, and it is not
a ganesha-core bug — see "Whose bug this is" below.

The corruption is silent at the moment it happens. The process dies later,
wherever the next allocation trips over the damage — which is why the original
symptom was this, in an unrelated thread encoding a reply:

```
Fatal glibc error: malloc.c:2601 (sysmalloc): assertion failed:
  (old_top == initial_top (av) && old_size == 0) || ...
```

with a stack that goes `complete_request` → ntirpc `xdr_ioq_uv_create` →
`gsh_malloc__` → `malloc`. **`malloc` is the detector, not the culprit.**
Chasing that stack leads nowhere.

### Three more bugs in the same FSAL, same run

Valgrind reported **22 errors in 13 contexts, and every one of them routes
through `libfsalproxy_v4.so`.** Nothing else in the process produced a single
error. Besides the overflow:

- **Heap disclosure onto the wire.** `Syscall param write(buf) points to
  uninitialised byte(s)`, in `proxyv4_compoundv4_execute` under
  `nfs4_op_open`. The bytes come from a **2,098,328-byte buffer `malloc`'d
  once per export by `proxyv4_init_rpc`** and never initialised, so PROXY_V4
  sends uninitialised heap contents to the backing server on OPEN. On a shared
  or untrusted backing NAS that is an information leak, independent of the
  crash.
- **Wild pointer in the read callback.** `Invalid read of size 4` in
  `_mdcache_lru_ref` via `mdc_read_cb`, called from PROXY_V4, at
  `Address 0x20c` — "not stack'd, malloc'd or (recently) free'd". Consistent
  with the overflow above having smashed an adjacent structure.
- **Six `Conditional jump depends on uninitialised value(s)`** — in
  `proxyv4_lookup_path` during export init, and in the write path via
  `xdr_io_data_encode`.

## Every version tested is affected

Debian carries 6.5 in trixie and **9.14 in backports, sid and forky alike**;
upstream is moving much faster than that — V10 through V14 all landed between
2026-06-19 and 2026-08-05 — so anything past 9.14 has to be built
([Dockerfile.ganesha-src](proxy-v4-repro/Dockerfile.ganesha-src) does it).

| Ganesha | Source | Result |
| --- | --- | --- |
| **6.5** | trixie | Crashes. `exit 139`, crash-loops (4 restarts in one run) |
| **9.14** | trixie-backports / sid / forky | Crashes, identically |
| **14.1** | built from `V14.1`, released 2026-08-05 | **Usually survives and silently returns wrong data**; crashed in one run of several |

The `memcpy` is byte-identical at all three tags, so the version difference is
only in what the corruption happens to land on — not in whether it happens.
Valgrind finds the same `Invalid write of size 8` from `proxyv4_read2` on
every one.

**V14.1 is the dangerous one to deploy.** A crash-looping gateway announces
itself; a gateway that serves bytes which are not the file does not.

## The trigger — no SQLite required

SQLite was how this was found, and it is still a good smoke test:

```python
import sqlite3
c = sqlite3.connect("/data/r.db")
c.execute("CREATE TABLE t(i INTEGER)")
c.execute("INSERT INTO t VALUES (1)")   # fails here
c.commit()
```

But the underlying failure needs nothing clever. Write a known pattern, push
it out of the client's page cache, read it back:

```python
data = bytes((i * 7 + 11) & 0xFF for i in range(1024 * 1024))
open(p, "wb").write(data)                       # ... plus flush + fsync
fd = os.open(p, os.O_RDONLY)
os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)   # force it back to the wire
got = os.read(fd, ...)                          # != data
```

On V14.1 this fails **from byte 0**, every run:

```
expected 0b121920272e353c434a51585f666d74
got      3dde816a000000010000000000000000
```

Those bytes are not file data — they are the proxy's own RPC receive buffer.
The leading word changes run to run (`c6dd816a`, `3dde816a`) the way an RPC
XID does. So the client is handed **server-side protocol memory** in place of
its file, which makes this an information-disclosure bug on the *client* side
as well as a correctness one.

**Two controls, both necessary, both run:**

- **The file is fine on the backing store.** Mounting the *backing* ganesha
  directly (`FSAL_VFS`, no proxy) and reading the same file gives a byte-exact
  match, same sha256. So this is the read path, not a corrupted write.
- **Buffered reads can hide it.** Reading straight back after writing returned
  correct data 10/10 — from the client's page cache, never touching the
  server. `POSIX_FADV_DONTNEED` (or `O_DIRECT`) is what makes it visible, and
  is why an earlier round of testing — sequential 1 MiB write and read-back,
  50 × random 4K read-modify-write with `fsync`, `mmap` read and write,
  byte-range locks including a past-EOF lock at SQLite's `PENDING_BYTE` — all
  passed. **A gateway can pass a storage smoke test and still be unusable.**

## It is not memory pressure — measured, not argued

The glibc assertion looks like it could be an out-of-memory symptom, and the
node had been having memory trouble around the time it was first seen. It
isn't, and the three failure modes are distinguishable from evidence
Kubernetes records for free:

| Failure | Signature |
| --- | --- |
| cgroup limit exceeded | SIGKILL, `reason: OOMKilled`, **exit 137**, no message |
| `malloc` returns NULL | ganesha's own allocation wrappers abort with their own out-of-memory message |
| **This bug** | glibc's `malloc_printerr` → abort, **exit 134/139**, `reason: Error` |

The confirming run: gwproxy with **`memory.max=max`** (no limit at all), the
process at ~117 MB RSS, **33 GB available on the node**, node
`MemoryPressure=False`, no `SystemOOM` and no eviction events. It crashed
anyway, on the first try, with `exitCode: 139` and `reason: Error` — never
`OOMKilled`. Reproduced again under gdb, and again under valgrind.

The one real interaction, worth keeping straight: corruption is only *detected*
when an allocation trips over it, so heavier memory traffic changes **when you
notice**, not whether the heap is broken. Memory pressure can plausibly shape
whether a given day's workload reaches the bad path. It cannot write past the
end of an 8 KiB buffer.

`MALLOC_CHECK_=3` did **not** catch it earlier than the top-chunk assert, which
is itself informative: those checks validate the chunk being freed or
reallocated, and this write lands in the space beyond the last allocation
rather than in a neighbouring chunk's header.

## Whose bug this is

"nfs-ganesha is mature software" is true and does not cover this code.
The FSALs with real deployment hours are `CEPH`, `GLUSTER`, `VFS` and `RGW`.
`FSAL_PROXY_V4` is a from-scratch NFSv4 **client** — its own compound builder,
its own RPC buffers, its own reply parsing — living inside the server process
and sharing its heap, which is why a bad copy in the client path kills a server
worker thread encoding an unrelated reply. Re-exporting NFS over NFS is a niche
shape almost nobody runs.

Debian's packaging says the same thing quietly: `nfs-ganesha-proxy-v4` is a
separate package that is **not** installed with the server, which is why the
repro has to `apt-get install` it at container start and why the gateway image
does not carry it.

## Environment

- Image: `whistler-storage-gateway` (see [Dockerfile](Dockerfile)), base
  `debian:trixie-slim`
- `nfs-ganesha 6.5-5`, `nfs-ganesha-vfs 6.5-5`, plus `nfs-ganesha-proxy-v4`
  installed at runtime
- Ganesha banner: `ganesha.nfsd Starting: Ganesha Version 6.5`
- Node: Ubuntu 24.04.4, kernel `6.8.0-134-generic`, k3s `v1.36.2+k3s1`, single node
- Client: kernel NFS client, `nfsvers=4.1`, `local_lock=none`

Gateway config used (the only non-default part is the FSAL block):

```
NFS_CORE_PARAM { NFS_Port = 2049; NFS_Protocols = 4; Enable_NLM = false;
                 Enable_RQUOTA = false; Enable_UDP = false; }
NFSV4 { Minor_Versions = 1, 2; Only_Numeric_Owners = true; Delegations = false; }
NFS_KRB5 { Active_krb5 = false; }
EXPORT {
    Export_Id = 1; Path = /home; Pseudo = /home; Access_Type = RW;
    Protocols = 4; Transports = TCP; SecType = sys;
    Squash = All_Squash; Anonymous_Uid = 1000; Anonymous_Gid = 1000;
    FSAL { Name = PROXY_V4; Srv_Addr = <backing ClusterIP>; }
}
LOG { Default_Log_Level = EVENT; }
```

## Reproducing it

[proxy-v4-repro/repro.sh](proxy-v4-repro/) stands up all three layers, all
namespaced, **no privileged pods** — an in-tree `nfs:` PersistentVolume makes
the *kubelet* perform every mount:

1. **backing** — the gateway image unchanged (`FSAL_VFS`) over a `local-path`
   PVC, mounted at `/shares/home`, `Service` on 2049. Stands in for the NAS.
2. **gwproxy** — the same image with `nfs-ganesha-proxy-v4` installed at
   startup and the config above, `Srv_Addr` = backing's ClusterIP. Toggled by
   env: `INSTALL_DEBUG=1` adds gdb + dbgsym + valgrind, `USE_GDB=1` runs under
   `gdb -batch` for a backtrace, `USE_VALGRIND=1` runs memcheck.
   `GANESHA_SUITE` picks the version — `trixie` (6.5), `trixie-backports`
   (9.14), or `preinstalled` with `GWPROXY_IMAGE` pointing at a source build.
   **Only this pod changes version**; the backing server stays put, so it is a
   constant across the matrix.
3. **guest** — `python:3-slim` with an in-tree `nfs:` PV pointed at gwproxy,
   running the read-back integrity check and then the SQLite snippet.

Four things the script encodes because each cost real time to rediscover:

- **Verify the export via DBus, never the log.** Ganesha prints
  `NFS SERVER INITIALIZED` even when every export failed to build, so a broken
  rig otherwise masquerades as a fixed bug. A *source* build additionally needs
  `org.ganesha.nfsd.conf` copied into `/etc/dbus-1/system.d/` — `cmake
  --install` does not install it, the Debian package does, and without it
  ganesha logs `DBUS not initialized` and the check silently never passes.
- **A fresh database name every run.** The backing PVC outlives a re-run, so a
  fixed name makes the next run open the *previous* run's wreckage and fail
  with `file is not a database` without ever exercising the server. That looks
  like a repro, is not one, and would hide a real fix. This one bit once.
- **Drop the client's page cache before reading back.** Otherwise the read is
  served locally and everything looks healthy.
- **`soft` mounts, and delete guests before servers.** With `hard`, the
  crash-looping server leaves the guest pod unkillable and strands an NFS mount
  on the node that needs `sudo umount -f -l` to clear.

Under valgrind the client needs `timeo=600`; memcheck slows ganesha enough that
the default `timeo=50` gives the client EIO before the server misbehaves, which
looks exactly like the bug not reproducing. Note that valgrind itself dies
shortly after the fault (`valgrind: the 'impossible' happened: main(): signal
was supposed to be fatal`) — it still prints the full error summary first,
which is all that is needed.

## Still open

- **Not yet reported upstream.** The nfs-ganesha tracker has not been searched
  for an existing PROXY_V4 read-path report, and nothing has been filed. The
  material for a report is now complete: source line, three affected releases,
  and a repro with no SQLite in it.
- **What makes `io_request` and `iov[0].iov_len` diverge in practice.** The
  unclamped `memcpy` is the bug regardless, but naming the caller that produces
  the mismatch would make the report sharper. `FULL_DEBUG` on the `FSAL`
  component logging both per read should show it.
- **The uninitialised-send bug is separately fixable** and does not need the
  read bug fixed first.
- **Whether writes are affected too.** Every check so far says no — the
  backing store's copy is always byte-exact — but the write path has its own
  uninitialised-memory finding, so "reads only" is an observation, not a proof.

## If it is fixed

PROXY_V4 becoming reliable would reopen NFS-backed storage classes for the
gateway. Note two things it still would **not** fix, both measured:

- Ganesha answers byte-range locks itself and never forwards them to the
  backing server, so two gateways over one tree remain two lock domains.
  (This also means locks were never a plausible cause of *this* crash: that
  path is ganesha's own state layer, the same code `FSAL_VFS` runs while
  passing the identical test.)
- `FSAL_VFS` still cannot export an NFS-backed PVC (the `resolve_posix_filesystem`
  refusal above), so PROXY_V4 would remain the only option on that substrate.
