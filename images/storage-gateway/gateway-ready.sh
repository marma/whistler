#!/bin/bash
# Readiness / startup gate for the per-user storage gateway.
#
# A TCP check on 2049 is NOT sufficient, and believing otherwise cost a long
# debugging session: ganesha binds the port and logs "NFS SERVER INITIALIZED"
# even when every EXPORT failed to build, so a gateway serving nothing reports
# healthy and every guest mount gets ENOENT (see README, /etc/mtab). Assert
# both halves instead — the port answers, AND ganesha itself says it is
# exporting the share.
#
# The export list comes from ganesha's own DBus interface rather than from
# anything we can infer from outside, which is why the entrypoint starts a
# system bus. dbus-send and dbus-daemon ship with the nfs-ganesha package, so
# this costs no extra image weight.
set -u

EXPORT_PATH="${SHARE_PATH:-/shares/home}"
PORT="${NFS_PORT:-2049}"

# bash's /dev/tcp — no nc/socat in the image, and none needed.
if ! exec 3<>"/dev/tcp/127.0.0.1/$PORT"; then
    echo "gateway-ready: nothing listening on $PORT" >&2
    exit 1
fi
exec 3<&-

if ! exports=$(dbus-send --system --print-reply --dest=org.ganesha.nfsd \
                 /org/ganesha/nfsd/ExportMgr \
                 org.ganesha.nfsd.exportmgr.ShowExports 2>&1); then
    echo "gateway-ready: ganesha is not answering on dbus: $exports" >&2
    exit 1
fi

if ! printf '%s' "$exports" | grep -qF "\"$EXPORT_PATH\""; then
    echo "gateway-ready: listening on $PORT but NOT exporting $EXPORT_PATH;" \
         "a mount would fail with ENOENT" >&2
    exit 1
fi
