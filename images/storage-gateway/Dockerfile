# Per-user storage gateway: a small Samba server that mounts the user's home
# PVC via normal CSI and exports it as an SMB3 share. KubeVirt VM guests mount
# it over cifs from cloud-init (see whistler/cloudinit.py).
#
# Why this exists: KubeVirt (<= 1.8) runs virtiofsd unprivileged (hardcoded
# uid 107, --sandbox=none, caps dropped — upstream kubevirt#13028), so a
# virtiofs-shared home is effectively read-only for the guest user. SMB gives
# server-side identity instead: the client's uids are never trusted (inherent
# root squash), and `force user` makes every write land on the PVC as the
# user's REAL uid — so the PVC stays consistent for pod sessions sharing it.
#
# One container per user, provisioned at startup from env + a mounted Secret:
#   SMB_USER / SMB_UID       — the Whistler user's name and real uid
#   /etc/whistler-smb/password — random per-user password (Secret whistler-smb-<user>)
# The user's PVC is mounted at /shares/home.
#
# Debian (not Alpine) for the battle-tested Samba build; bookworm ships 4.17.
# The SMB3.1.1 POSIX-extensions profile planned for future *shared* volumes is
# deliberately NOT enabled here — the home share is single-user and `force
# user` covers it (see smb.conf.template).
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      samba \
 && rm -rf /var/lib/apt/lists/*

COPY smb.conf.template /etc/samba/smb.conf.template
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 445
ENTRYPOINT ["/entrypoint.sh"]
