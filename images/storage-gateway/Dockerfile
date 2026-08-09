# Per-user storage gateway: a small NFS server that mounts the user's home
# PVC via normal CSI and exports it as NFSv4.2. KubeVirt VM guests mount it
# from cloud-init (see whistler/cloudinit.py).
#
# Why this exists: KubeVirt (<= 1.8) runs virtiofsd unprivileged (hardcoded
# uid 107, --sandbox=none, caps dropped — upstream kubevirt#13028), so a
# virtiofs-shared home is effectively read-only for the guest user.
#
# Why NFS and not SMB: byte-range locks. cifs forwards SQLite's past-EOF
# F_WRLCK to the server, which answers EACCES, and SQLite reads that as
# contention — a flat 100s stall then "database is locked" for anything
# keeping a database in $HOME (nautilus, dconf, Chrome's profile). The SMB
# gateway needed `nobrl` to dodge that, which throws away locking entirely.
# NFSv4 carries locks natively in the protocol, on the same connection.
#
# Why NFS-Ganesha and not the kernel server: nfsd is a kernel-global resource
# needing a privileged container; ganesha is a userspace server, so one
# ordinary pod per user works. It needs exactly two capabilities (see
# _build_gateway_manifests): DAC_READ_SEARCH, because the VFS FSAL addresses
# files by handle (open_by_handle_at), and SYS_RESOURCE for its fd limit.
#
# Identity is server-side, as it was under SMB: the guest is root and can
# assert any uid it likes over AUTH_SYS, so the export squashes every request
# (`Squash = All_Squash`) to the user's REAL uid/gid — the direct analog of
# Samba's `force user`. Writes land on the PVC owned by that uid, so the PVC
# stays consistent for pod sessions that mount it directly. What NFS does NOT
# give is a per-share credential; the boundary is which export exists and who
# can reach it (NetworkPolicy) — see design/security.md.
#
# One container per user, provisioned at startup from env:
#   SHARE_USER          — the Whistler user's name (logging/labelling only)
#   SHARE_UID/SHARE_GID — the user's real uid/gid; the export's anon identity
# The user's PVC is mounted at /shares/home.
#
# Debian trixie: nfs-ganesha 6.5 with the VFS FSAL packaged separately.
FROM debian:trixie-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      nfs-ganesha \
      nfs-ganesha-vfs \
 && rm -rf /var/lib/apt/lists/*

# Ganesha builds its filesystem table with setmntent(/etc/mtab) and refuses to
# create ANY export if that fails ("populate_posix_file_systems returned No
# such file or directory"). debian-slim ships no /etc/mtab. Docker papers over
# this by creating the symlink at container start; **containerd does not**, so
# the image works under `docker run` and exports nothing under Kubernetes —
# and ganesha still binds 2049, so a TCP readiness probe calls it healthy while
# every guest mount gets ENOENT. Own the symlink here rather than depend on the
# runtime.
RUN ln -sf /proc/mounts /etc/mtab

COPY ganesha.conf.template /etc/ganesha/ganesha.conf.template
COPY entrypoint.sh /entrypoint.sh
COPY gateway-ready.sh /usr/local/bin/gateway-ready
RUN chmod +x /entrypoint.sh /usr/local/bin/gateway-ready

EXPOSE 2049
ENTRYPOINT ["/entrypoint.sh"]
