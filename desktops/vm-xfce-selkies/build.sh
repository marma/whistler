#!/usr/bin/env bash
# Bake the vm-xfce-selkies KubeVirt containerDisk.
#
# Pipeline (host needs only docker, curl and a /dev/kvm node — no libguestfs,
# no ISO tooling, no kvm group membership):
#   1. docker-build ../streamer-selkies2 and extract its /opt/venv +
#      /opt/selkies-web + wtype shim — the Selkies stack stays single-sourced
#      (same SELKIES_COMMIT, client/server lock) and is binary-compatible with
#      the guest (both Ubuntu 26.04, same python3/abi).
#   2. Boot the Ubuntu 26.04 cloud image once under qemu/KVM *in a container*
#      (bake/Dockerfile.bake + bake/boot.sh; `--device /dev/kvm --group-add
#      <kvm gid>` works without host kvm-group membership) with a NoCloud
#      seed served over HTTP (SMBIOS serial `ds=nocloud-net;s=http://10.0.2.2:…/`):
#      bake/user-data.in installs XFCE + the streamer stack + the guest/
#      payload, resets cloud-init, powers off. boot.sh then flattens +
#      compresses the disk.
#   3. Wrap the qcow2 as a containerDisk (Dockerfile.containerdisk).
#
# Knobs (env): IMAGE (default localhost:5000/whistler-vm-xfce-selkies),
# TAG (dev), PUSH=1 to docker-push, DISK_SIZE (12G lean / 22G CUDA),
# QEMU_MEM (4096), QEMU_SMP (min(8,nproc)), BASE_IMAGE_URL, CACHE_DIR
# (~/.cache/whistler/vm-images), BAKE_TIMEOUT (2700s),
# CUDA/NVIDIA_DRIVER_PACKAGE/CUDA_TOOLKIT_PACKAGE (see below).
#
# CUDA=1 bakes the NVIDIA open driver + the CUDA toolkit in and tags the result
# :<TAG>-cuda; the default (CUDA=0) is a LEAN image with neither. Two variants,
# not one: the driver+toolkit are dead weight (and the driver a first-boot
# install over the egress-locked guest net) on the vast majority of sessions
# that have no passthrough GPU, so only GPU templates pull the -cuda image.
# They're baked (not session-time installed) precisely because the default zone
# blocks package mirrors — so a GPU session boots ready. NVIDIA_DRIVER_PACKAGE /
# CUDA_TOOLKIT_PACKAGE override which packages (the guest is 26.04; defaults are
# nvidia-driver-595-open and the archive nvidia-cuda-toolkit). Note the driver
# also changes the DISPLAY path: libnvidia-encode makes pixelflux encode the
# Selkies stream on NVENC instead of software x264 (see
# guest/usr/local/bin/whistler-streamer).
#
# amd64-only: the bake runs the target-arch guest under KVM; producing arm64
# needs an arm64 host (or an emulated ~hour-long TCG bake nobody wants).
set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${IMAGE:-localhost:5000/whistler-vm-xfce-selkies}"
TAG="${TAG:-dev}"
PUSH="${PUSH:-0}"
CUDA="${CUDA:-0}"
QEMU_MEM="${QEMU_MEM:-4096}"
QEMU_SMP="${QEMU_SMP:-$(( $(nproc) < 8 ? $(nproc) : 8 ))}"
BASE_IMAGE_URL="${BASE_IMAGE_URL:-https://cloud-images.ubuntu.com/releases/26.04/release/ubuntu-26.04-server-cloudimg-amd64.img}"
CACHE_DIR="${CACHE_DIR:-$HOME/.cache/whistler/vm-images}"
BAKE_TIMEOUT="${BAKE_TIMEOUT:-2700}"
# CUDA=1 → bake the NVIDIA driver + CUDA toolkit and suffix the tag; else a
# lean image with neither. CUDA_TOOLKIT_PACKAGE defaults to 26.04's archive
# nvidia-cuda-toolkit (nvcc + cuda runtime libs); empty it for driver-only, or
# point it at a cuda-toolkit-XX-Y package from NVIDIA's CUDA apt repo for a
# specific release. The toolkit adds several GB, so CUDA builds get a bigger disk.
if [ "$CUDA" = "1" ]; then
  NVIDIA_DRIVER_PACKAGE="${NVIDIA_DRIVER_PACKAGE:-nvidia-driver-595-open}"
  CUDA_TOOLKIT_PACKAGE="${CUDA_TOOLKIT_PACKAGE:-nvidia-cuda-toolkit}"
  TAG="${TAG}-cuda"
  DISK_SIZE="${DISK_SIZE:-22G}"
else
  NVIDIA_DRIVER_PACKAGE=""
  CUDA_TOOLKIT_PACKAGE=""
  DISK_SIZE="${DISK_SIZE:-12G}"
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

echo "==> [1/4] Selkies artifacts from the streamer image"
docker build -t whistler-vm-bake-streamer ../streamer-selkies2
BAKE_CTR=$(docker create whistler-vm-bake-streamer)
mkdir -p "$STAGE_DIR/opt" "$STAGE_DIR/usr/local/bin"
docker cp "$BAKE_CTR:/opt/venv" "$STAGE_DIR/opt/venv"
docker cp "$BAKE_CTR:/opt/selkies-web" "$STAGE_DIR/opt/selkies-web"
docker cp "$BAKE_CTR:/usr/local/bin/wtype" "$STAGE_DIR/usr/local/bin/wtype"
docker cp "$BAKE_CTR:/usr/local/bin/whistler-copy-agent" "$STAGE_DIR/usr/local/bin/whistler-copy-agent"
cp -a guest/. "$STAGE_DIR/"
# --owner/--group 0: the tar is created by an ordinary user but extracted by
# root in the guest, where tar would otherwise faithfully restore this uid.
# --mode go-w: the staging dir inherits the repo's group-writable modes, and
# extracting ./etc ./usr with g+w onto the guest made sshd's StrictModes
# refuse the whole /etc/ssh/authorized_keys.d path ("bad ownership or modes
# for directory /etc") — which surfaces only once the SMB home shadows
# ~/.ssh/authorized_keys, i.e. exactly when the root-disk path must work.
tar --owner=0 --group=0 --mode=go-w -czf "$SEED_DIR/artifacts.tar.gz" -C "$STAGE_DIR" .

echo "==> [2/4] Base cloud image"
BASE_IMG="$CACHE_DIR/$(basename "$BASE_IMAGE_URL")"
[ -s "$BASE_IMG" ] || curl -fL --progress-bar -o "$BASE_IMG" "$BASE_IMAGE_URL"

echo "==> [3/4] Bake boot (qemu/KVM in docker; console: $BUILD_DIR/console.log)"
sed -e "s/@HTTP_PORT@/$HTTP_PORT/g" \
    -e "s/@NVIDIA_DRIVER_PACKAGE@/$NVIDIA_DRIVER_PACKAGE/g" \
    -e "s/@CUDA_TOOLKIT_PACKAGE@/$CUDA_TOOLKIT_PACKAGE/g" \
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
