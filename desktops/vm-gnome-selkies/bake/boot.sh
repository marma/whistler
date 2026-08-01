#!/bin/sh
# Bake boot, run INSIDE the whistler-vm-bake container (see Dockerfile.bake;
# launched by ../build.sh with /dev/kvm passed through). Boots the base cloud
# image once against the NoCloud-over-HTTP seed in /seed, waits for the
# baked guest to power itself off, then flattens the result.
#
# Env (set by build.sh): BASE_NAME (file in /cache), DISK_SIZE, QEMU_MEM,
# QEMU_SMP, HTTP_PORT (must match @HTTP_PORT@ in the rendered user-data).
set -eu

qemu-img create -f qcow2 -b "/cache/$BASE_NAME" -F qcow2 /build/work.qcow2 "$DISK_SIZE" >/dev/null

# Loopback is this container's netns; qemu user-net exposes it to the guest
# as 10.0.2.2, which is where the seed's ds=nocloud-net URL points.
python3 -m http.server --bind 127.0.0.1 --directory /seed "$HTTP_PORT" >/dev/null 2>&1 &

# -no-reboot: the bake's `shutdown -P now` ends the process.
qemu-system-x86_64 \
  -enable-kvm -cpu host -smp "$QEMU_SMP" -m "$QEMU_MEM" \
  -drive file=/build/work.qcow2,if=virtio,format=qcow2 \
  -netdev user,id=n0 -device virtio-net-pci,netdev=n0 \
  -smbios "type=1,serial=ds=nocloud-net;s=http://10.0.2.2:$HTTP_PORT/" \
  -display none -serial file:/build/console.log \
  -no-reboot

grep -q "WHISTLER-BAKE-OK" /build/console.log || {
  echo "ERROR: bake did not report success — tail of console.log:" >&2
  tail -80 /build/console.log >&2
  exit 1
}

# zstd + parallel coroutines: zlib -c pegged one core (the containerDisk layer
# re-gzips on push anyway); zstd is much faster for equal/better ratio.
qemu-img convert -O qcow2 -o compression_type=zstd -c -m 16 /build/work.qcow2 /build/disk.qcow2
rm -f /build/work.qcow2
qemu-img info /build/disk.qcow2
