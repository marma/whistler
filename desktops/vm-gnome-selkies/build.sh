#!/usr/bin/env bash
# Bake the vm-gnome-selkies KubeVirt containerDisk: the real GNOME Shell (46,
# X11 backend) + the Selkies 2.x streaming stack baked into an Ubuntu 24.04
# guest. The GNOME sibling of ../vm-xfce-selkies — read that build.sh first;
# this one differs in exactly two places, both forced by GNOME needing 24.04:
#
#   * Artifacts come from bake/Dockerfile.builder (a 24.04 image), NOT from
#     ../streamer-selkies2 (26.04). GNOME Shell with an X11 backend only exists
#     up to GNOME 46 = Ubuntu 24.04, so the guest is 24.04 and its Python 3.12 /
#     libva ABI can't run the 26.04 streamer's 3.13 venv. The builder rebuilds
#     the venv + web client on 24.04 AND vendors libva 2.22 (24.04 ships 2.20;
#     pixelflux needs >= 2.21 — see the builder header).
#   * BASE_IMAGE_URL is the 24.04 cloud image, and the bake apt-installs the
#     GNOME DE instead of XFCE (bake/user-data.in).
#
# Pipeline (host needs only docker, curl and a /dev/kvm node):
#   1. docker-build bake/Dockerfile.builder and extract /opt/venv,
#      /opt/selkies-web, the vendored libva tree (/opt/libva) and the wtype
#      shim — all 24.04-ABI, single-sourced on one SELKIES_COMMIT/LIBVA_VERSION.
#   2. Boot the Ubuntu 24.04 cloud image once under qemu/KVM in a container
#      (bake/Dockerfile.bake + bake/boot.sh) with a NoCloud seed served over
#      HTTP: bake/user-data.in installs GNOME + the guest payload (incl. the
#      vendored libva → /usr/local + ldconfig), resets cloud-init, powers off.
#   3. Wrap the qcow2 as a containerDisk (Dockerfile.containerdisk).
#
# Knobs (env): IMAGE (default localhost:5000/whistler-vm-gnome-selkies),
# TAG (dev), PUSH=1 to docker-push, DISK_SIZE (14G lean / 24G CUDA — GNOME is
# heavier than XFCE), QEMU_MEM (4096), QEMU_SMP (min(8,nproc)), BASE_IMAGE_URL,
# CACHE_DIR (~/.cache/whistler/vm-images), BAKE_TIMEOUT (2700s),
# CUDA/NVIDIA_DRIVER_PACKAGE/CUDA_TOOLKIT_PACKAGE (see below).
#
# CUDA=1 bakes the NVIDIA open driver + the CUDA toolkit in and tags the result
# :<TAG>-cuda; the default (CUDA=0) is a LEAN image with neither. Same split as
# ../vm-xfce-selkies — only GPU templates pull the -cuda image. NOTE the guest
# here is 24.04, whose open-driver package name differs from 26.04's, so the
# default driver is nvidia-driver-550-open (24.04's open-kernel branch) and the
# toolkit is 24.04's archive nvidia-cuda-toolkit (CUDA 12.x); override
# NVIDIA_DRIVER_PACKAGE / CUDA_TOOLKIT_PACKAGE for different ones (a wrong name
# fails the bake). GNOME renders on llvmpipe in both variants (Xvfb's GLX is
# Mesa swrast), but the driver is not desktop-neutral: it brings
# libnvidia-encode, so a -cuda passthrough session encodes the Selkies stream on
# NVENC rather than software x264 (see guest/usr/local/bin/whistler-streamer).
# The CUDA toolkit on top of that is for GPU-compute workloads in-guest, and
# VirtualGL (VIRTUALGL_VERSION, CUDA variant only) lets individual GL apps
# render on the GPU via `vgl <app>` — see README "Three graphics tiers".
#
# amd64-only: the bake runs the target-arch guest under KVM; producing arm64
# needs an arm64 host (or an emulated ~hour-long TCG bake nobody wants).
set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${IMAGE:-localhost:5000/whistler-vm-gnome-selkies}"
TAG="${TAG:-dev}"
PUSH="${PUSH:-0}"
CUDA="${CUDA:-0}"
QEMU_MEM="${QEMU_MEM:-4096}"
QEMU_SMP="${QEMU_SMP:-$(( $(nproc) < 8 ? $(nproc) : 8 ))}"
BASE_IMAGE_URL="${BASE_IMAGE_URL:-https://cloud-images.ubuntu.com/releases/24.04/release/ubuntu-24.04-server-cloudimg-amd64.img}"
CACHE_DIR="${CACHE_DIR:-$HOME/.cache/whistler/vm-images}"
BAKE_TIMEOUT="${BAKE_TIMEOUT:-2700}"
# CUDA=1 → bake the NVIDIA driver + CUDA toolkit and suffix the tag; else a
# lean, driver-free image. CUDA_TOOLKIT_PACKAGE defaults to 24.04's archive
# `nvidia-cuda-toolkit` (nvcc + cuda runtime libs, CUDA 12.x — enough for the
# RTX 4090 / sm_89); point it at a cuda-toolkit-XX-Y package if you've added
# NVIDIA's CUDA apt repo and need a newer release. Empty it to bake the driver
# only. The toolkit adds several GB, so the CUDA build also gets a bigger disk.
if [ "$CUDA" = "1" ]; then
  NVIDIA_DRIVER_PACKAGE="${NVIDIA_DRIVER_PACKAGE:-nvidia-driver-550-open}"
  CUDA_TOOLKIT_PACKAGE="${CUDA_TOOLKIT_PACKAGE:-nvidia-cuda-toolkit}"
  # VirtualGL (pinned upstream .deb) rides along with the driver so GL apps can
  # opt in to *drawing* on the passthrough GPU via `vgl <app>` (EGL backend) —
  # without it the GPU can only do CUDA + NVENC while all OpenGL stays on
  # llvmpipe (Xvfb's GLX is swrast). Empty it to skip.
  VIRTUALGL_VERSION="${VIRTUALGL_VERSION:-3.1.4}"
  TAG="${TAG}-cuda"
  DISK_SIZE="${DISK_SIZE:-24G}"
else
  NVIDIA_DRIVER_PACKAGE=""
  CUDA_TOOLKIT_PACKAGE=""
  VIRTUALGL_VERSION=""
  DISK_SIZE="${DISK_SIZE:-14G}"
fi
# Fixed port is fine: it only exists inside the bake container's netns.
HTTP_PORT=8099

BUILD_DIR="$PWD/build"
STAGE_DIR="$BUILD_DIR/stage"
SEED_DIR="$BUILD_DIR/seed"

[ -e /dev/kvm ] || { echo "ERROR: no /dev/kvm — the bake needs KVM" >&2; exit 1; }

rm -rf "$BUILD_DIR"
mkdir -p "$STAGE_DIR" "$SEED_DIR" "$CACHE_DIR"

cleanup() {
  [ -n "${BAKE_CTR:-}" ] && docker rm -f "$BAKE_CTR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> [1/4] Selkies + libva artifacts from the 24.04 builder image"
docker build -f bake/Dockerfile.builder -t whistler-vm-gnome-builder bake/
BAKE_CTR=$(docker create whistler-vm-gnome-builder)
mkdir -p "$STAGE_DIR/opt" "$STAGE_DIR/usr/local/bin" "$STAGE_DIR/usr/local/lib"
docker cp "$BAKE_CTR:/opt/venv" "$STAGE_DIR/opt/venv"
docker cp "$BAKE_CTR:/opt/selkies-web" "$STAGE_DIR/opt/selkies-web"
docker cp "$BAKE_CTR:/usr/local/bin/wtype" "$STAGE_DIR/usr/local/bin/wtype"
# Vendored libva 2.22 → guest /usr/local (ahead of /usr/lib in ld.so order;
# the bake runs ldconfig after extraction). The builder staged it under
# /opt/libva/usr/local so this copy lands the lib/ (and pkgconfig/include)
# tree straight at the guest's /usr/local.
docker cp "$BAKE_CTR:/opt/libva/usr/local/." "$STAGE_DIR/usr/local/"
# Copy-agent is a plain script, so unlike the venv it needs no 24.04 rebuild —
# take the canonical copy from the streamer context (shell copy: only the
# docker build contexts can't cross directories, build.sh can).
cp ../streamer-selkies2/whistler-copy-agent "$STAGE_DIR/usr/local/bin/whistler-copy-agent"
chmod +x "$STAGE_DIR/usr/local/bin/whistler-copy-agent"
cp -a guest/. "$STAGE_DIR/"
# --owner/--group 0: the tar is created by an ordinary user but extracted by
# root in the guest, where tar would otherwise faithfully restore this uid.
# --mode go-w: the staging dir inherits the repo's group-writable modes, and
# extracting ./etc ./usr with g+w onto the guest made sshd's StrictModes
# refuse the whole /etc/ssh/authorized_keys.d path ("bad ownership or modes
# for directory /etc") — which surfaces only once the SMB home shadows
# ~/.ssh/authorized_keys, i.e. exactly when the root-disk path must work.
tar --owner=0 --group=0 --mode=go-w -czf "$SEED_DIR/artifacts.tar.gz" -C "$STAGE_DIR" .

echo "==> [2/4] Base cloud image (Ubuntu 24.04)"
BASE_IMG="$CACHE_DIR/$(basename "$BASE_IMAGE_URL")"
[ -s "$BASE_IMG" ] || curl -fL --progress-bar -o "$BASE_IMG" "$BASE_IMAGE_URL"

echo "==> [3/4] Bake boot (qemu/KVM in docker; console: $BUILD_DIR/console.log)"
sed -e "s/@HTTP_PORT@/$HTTP_PORT/g" \
    -e "s/@NVIDIA_DRIVER_PACKAGE@/$NVIDIA_DRIVER_PACKAGE/g" \
    -e "s/@CUDA_TOOLKIT_PACKAGE@/$CUDA_TOOLKIT_PACKAGE/g" \
    -e "s/@VIRTUALGL_VERSION@/$VIRTUALGL_VERSION/g" \
    bake/user-data.in > "$SEED_DIR/user-data"
printf 'instance-id: whistler-bake\nlocal-hostname: bake\n' > "$SEED_DIR/meta-data"
docker build -f bake/Dockerfile.bake -t whistler-vm-bake bake/
# --user + --group-add: bake as the invoking user so /build output isn't
# root-owned, with the kvm gid granted inside the container (the device node
# keeps its host owner/group/mode).
timeout --foreground "$BAKE_TIMEOUT" docker run --rm \
  --device /dev/kvm \
  --user "$(id -u):$(id -g)" --group-add "$(stat -c %g /dev/kvm)" \
  -v "$BUILD_DIR:/build" -v "$CACHE_DIR:/cache:ro" -v "$SEED_DIR:/seed:ro" \
  -e BASE_NAME="$(basename "$BASE_IMG")" \
  -e DISK_SIZE="$DISK_SIZE" -e QEMU_MEM="$QEMU_MEM" -e QEMU_SMP="$QEMU_SMP" \
  -e HTTP_PORT="$HTTP_PORT" \
  whistler-vm-bake \
  || { echo "ERROR: bake failed (see $BUILD_DIR/console.log)"; exit 1; }

echo "==> [4/4] containerDisk $IMAGE:$TAG"
cp Dockerfile.containerdisk "$BUILD_DIR/"
docker build -f "$BUILD_DIR/Dockerfile.containerdisk" -t "$IMAGE:$TAG" "$BUILD_DIR"
if [ "$PUSH" = "1" ]; then
  docker push "$IMAGE:$TAG"
fi

echo "OK: $IMAGE:$TAG (disk: $(du -h "$BUILD_DIR/disk.qcow2" | cut -f1))"
