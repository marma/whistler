#!/usr/bin/env bash
# Reproduce the FSAL_PROXY_V4 heap overflow. See ../proxy-v4-heap-bug.md.
#
#   ./repro.sh            # plain run: crashes in ~90s
#   ./repro.sh valgrind   # under memcheck: names the overflowing memmove
#   ./repro.sh clean      # tear the rig down
#
# Set IMAGE to any build of the storage-gateway image; the FSAL_PROXY_V4
# package is installed into it at container start, so no rebuild is needed.
#
# GANESHA_SUITE picks the ganesha the proxy runs, for version bisection:
#   GANESHA_SUITE=trixie-backports ./repro.sh     # 9.14 instead of 6.5
# The backing server always stays on the image's version, so it is a constant.
set -euo pipefail

NS=proxyv4-repro
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="${IMAGE:-localhost:5000/whistler-storage-gateway:latest}"
MODE="${1:-plain}"

teardown() {
    # Guests before servers, always: a soft mount still needs the client gone
    # before the server, or the PV/PVC delete blocks on an in-use volume.
    kubectl -n "$NS" delete pod guest --ignore-not-found --wait=true >/dev/null 2>&1 || true
    kubectl -n "$NS" delete pvc guest-proxy-pvc --ignore-not-found --wait=true >/dev/null 2>&1 || true
    kubectl delete pv guest-proxy-pv --ignore-not-found --wait=true >/dev/null 2>&1 || true
    kubectl delete ns "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || true
}

if [ "$MODE" = clean ]; then teardown; echo "torn down"; exit 0; fi

trap 'echo; echo "!! failed - run ./repro.sh clean to tear down"; exit 1' ERR

echo "== backing (FSAL_VFS over local-path) =="
sed "s|localhost:5000/whistler-storage-gateway:[^ ]*|$IMAGE|" "$HERE/01-backing.yaml" \
    | kubectl apply -f - >/dev/null
kubectl -n "$NS" rollout status deploy/backing --timeout=180s

echo "== gwproxy (FSAL_PROXY_V4) =="
# GWPROXY_IMAGE differs from IMAGE only when testing a ganesha that is not
# packaged — a source build (Dockerfile.ganesha-src) with GANESHA_SUITE=preinstalled.
# The backing server stays on IMAGE either way, so it remains the constant.
sed "s|localhost:5000/whistler-storage-gateway:[^ ]*|${GWPROXY_IMAGE:-$IMAGE}|" \
    "$HERE/02-gwproxy.yaml" | kubectl apply -f - >/dev/null
kubectl -n "$NS" set env deploy/gwproxy \
    GANESHA_SUITE="${GANESHA_SUITE:-trixie}" >/dev/null
if [ "$MODE" = valgrind ]; then
    kubectl -n "$NS" set env deploy/gwproxy INSTALL_DEBUG=1 USE_VALGRIND=1 >/dev/null
    TIMEO=600
else
    TIMEO=50
fi
kubectl -n "$NS" rollout status deploy/gwproxy --timeout=300s

# Ganesha logs "NFS SERVER INITIALIZED" even when every export failed to build,
# so trusting the log here would let a broken rig masquerade as a fixed bug.
# DBus is the only honest check.
echo "== waiting for the export to actually build =="
for _ in $(seq 1 60); do
    POD=$(kubectl -n "$NS" get pods -l app=gwproxy -o jsonpath='{.items[0].metadata.name}')
    if kubectl -n "$NS" exec "$POD" -- dbus-send --system --print-reply \
         --dest=org.ganesha.nfsd /org/ganesha/nfsd/ExportMgr \
         org.ganesha.nfsd.exportmgr.ShowExports 2>/dev/null | grep -q '"/home"'; then
        echo "   export /home present"
        break
    fi
    sleep 5
done

echo "== guest =="
sed -e "s|@GWPROXY_IP@|$(kubectl -n "$NS" get svc gwproxy -o jsonpath='{.spec.clusterIP}')|" \
    -e "s|@TIMEO@|$TIMEO|" "$HERE/03-guest.yaml.tmpl" | kubectl apply -f - >/dev/null
kubectl -n "$NS" wait --for=condition=Ready pod/guest --timeout=180s

echo "== conditions at trigger time =="
kubectl -n "$NS" exec "$POD" -- sh -c \
    'echo "   gwproxy memory.max=$(cat /sys/fs/cgroup/memory.max)"' || true
echo "   node: $(free -m | awk '/^Mem:/{print $7" MiB available of "$2" MiB"}')"

echo "== read-back integrity (no SQLite) =="
# The primitive failure, with nothing clever involved: write a known pattern,
# push it out of the client's page cache, read it back. On a healthy server
# this is boring. Through FSAL_PROXY_V4 the read returns bytes that are not
# the file — and the file itself is fine on the backing store, so this is the
# read path, not the write path.
kubectl -n "$NS" exec guest -- python3 -c '
import os, hashlib
data = bytes((i * 7 + 11) & 0xFF for i in range(1024 * 1024))
p = "/data/pattern-%d.bin" % os.getpid()
with open(p, "wb") as f:
    f.write(data); f.flush(); os.fsync(f.fileno())
fd = os.open(p, os.O_RDONLY)
os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)   # force it back to the wire
got = b""
while True:
    b = os.read(fd, 65536)
    if not b:
        break
    got += b
os.close(fd)
if got == data:
    print("   RESULT: read-back OK")
else:
    d = next((i for i in range(min(len(got), len(data))) if got[i] != data[i]), None)
    print("   RESULT: read-back CORRUPT (len=%d, first bad byte at %s)" % (len(got), d))
    if d is not None:
        print("     expected %s" % data[d:d+16].hex())
        print("     got      %s" % got[d:d+16].hex())
' || true

echo "== trigger =="
# A fresh database name every run, deliberately. The backing PVC outlives a
# re-run of this script, and a crashed run leaves a corrupt r.db behind — so a
# fixed name makes the NEXT run open that wreckage and fail with "file is not
# a database" without ever exercising the server. That looks like a repro and
# is not one, and it hides an actual fix.
kubectl -n "$NS" exec guest -- python -c '
import sqlite3, os, time
path = "/data/r-%d.db" % time.time()
print("   database:", path, "(fresh)")
try:
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE t(i INTEGER)")
    c.execute("INSERT INTO t VALUES (1)")
    c.commit()
    print("RESULT: survived;", list(c.execute("PRAGMA integrity_check")))
except Exception as e:
    print("RESULT: failed:", type(e).__name__, e)
' || true

echo
echo "== gwproxy state =="
kubectl -n "$NS" get pods -l app=gwproxy \
    -o jsonpath='   restarts={.items[0].status.containerStatuses[0].restartCount} lastExit={.items[0].status.containerStatuses[0].lastState.terminated.exitCode} reason={.items[0].status.containerStatuses[0].lastState.terminated.reason}{"\n"}'
echo
echo "Crash detail:   kubectl -n $NS logs -l app=gwproxy --previous | tail -60"
echo "Tear down:      ./repro.sh clean"
