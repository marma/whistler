#!/bin/sh
# Render the per-user export and run ganesha.nfsd in the foreground.
#
# No local account is created: the export squashes every request to
# SHARE_UID/SHARE_GID numerically (Only_Numeric_Owners), so ganesha never
# looks a name up. The ids only have to match the PVC's on-disk ownership,
# which is the same uid the guest and pod sessions run as.
set -eu

: "${SHARE_USER:?SHARE_USER is required}"
: "${SHARE_UID:?SHARE_UID is required}"
SHARE_GID="${SHARE_GID:-$SHARE_UID}"

# A fresh PVC's root is owned by root (or the provisioner) — writes squashed
# to the user's uid would be denied. Claim the share root (non-recursive: on a
# populated home this is the ordinary "home dir owned by its user" no-op).
chown "$SHARE_UID:$SHARE_GID" /shares/home

# Provisioners hand out the volume root world-writable (k3s local-path gives
# 0777), and NFS reports the real mode — so $HOME showed up as 0777 and sshd's
# StrictModes refused to read ~/.ssh/authorized_keys out of it ("bad ownership
# or modes for directory"). Drop the group/other write bits rather than
# forcing 0755: that turns the provisioner's 0777 into the intended 0755 while
# leaving a user who deliberately tightened their own home (0700) alone.
chmod go-w /shares/home

sed -e "s/@USER@/$SHARE_USER/g" \
    -e "s/@UID@/$SHARE_UID/g" \
    -e "s/@GID@/$SHARE_GID/g" \
    /etc/ganesha/ganesha.conf.template > /etc/ganesha/ganesha.conf

# Fail loudly rather than serve nothing. Ganesha reads the mount table through
# /etc/mtab (the Dockerfile symlinks it to /proc/mounts, because containerd —
# unlike Docker — does not create it); without it every export fails to build
# and ganesha carries on listening on 2049 with an empty export list. The pod
# then passes its TCP readiness probe while every guest mount gets ENOENT, so
# the only honest thing to do is refuse to start.
if [ ! -r /etc/mtab ]; then
    echo "whistler: /etc/mtab is missing or unreadable; ganesha cannot build" \
         "its filesystem table and would export nothing" >&2
    exit 1
fi

# NFSv4 client recovery state. Deliberately on the container filesystem and
# not on the home PVC: a restarted gateway then knows of no clients to
# reclaim and lifts the grace period immediately, and the user's home stays
# free of server bookkeeping.
mkdir -p /var/lib/nfs/ganesha /var/run/ganesha /run/dbus

# A system bus, started BEFORE ganesha because ganesha only registers its DBus
# objects at startup and never retries. It exists for exactly one reason: it is
# how gateway-ready asks ganesha which exports it actually has, which is the
# only way to tell a working gateway from one that came up with an empty export
# list. Treat a missing bus as fatal — without it the readiness probe can never
# pass, and failing here names the cause instead of leaving a mystery.
dbus-uuidgen --ensure >/dev/null 2>&1 || true
dbus-daemon --system --fork
for _ in 1 2 3 4 5 6 7 8 9 10; do
    [ -S /run/dbus/system_bus_socket ] && break
    sleep 0.2
done
if [ ! -S /run/dbus/system_bus_socket ]; then
    echo "whistler: dbus-daemon did not come up; ganesha would run unprobeable" >&2
    exit 1
fi

# -F: foreground (PID 1). -L STDOUT: ganesha wants that literal token for the
# pod log — a /dev/stdout path is rejected and it exits.
exec /usr/bin/ganesha.nfsd -F -f /etc/ganesha/ganesha.conf -L STDOUT -N NIV_EVENT
