#!/usr/bin/env bash
# Bake the devbase KubeVirt containerDisk: a *server* VM guest — no desktop, no
# Selkies, no X — set up for development and reached over SSH (mode: ssh,
# runtime: vm). The toolchain is clang/clang++, pixi and Python 3.14, plus the
# usual build/VCS/editor tools; see README.md for the full list and the why.
#
# Sibling of ../../desktops/vm-{xfce,gnome}-selkies/build.sh and deliberately
# structured the same — read one of those for the shared mechanics. This one is
# simpler in one big way: there is no artifact *builder* stage, because there is
# no Selkies venv to ABI-match. Everything comes from the guest's own apt (the
# base is Ubuntu 26.04, which ships Python 3.14 and clang 21 in the archive —
# no deadsnakes, no LLVM apt repo) plus two pinned upstream downloads (pixi,
# and NVIDIA's CUDA repo for the cuda-dev variant).
#
# Pipeline (host needs only docker, curl and a /dev/kvm node):
#   1. tar up guest/ (profile.d PATH bits + motd) as the NoCloud seed's
#      artifacts.tar.gz.
#   2. Boot the Ubuntu 26.04 cloud image once under qemu/KVM in a container
#      (bake/Dockerfile.bake + bake/boot.sh) with the seed served over HTTP:
#      bake/user-data.in installs the toolchain, resets cloud-init, powers off.
#   3. Wrap the qcow2 as a containerDisk (Dockerfile.containerdisk).
#
# Knobs (env): IMAGE (default localhost:5000/whistler-devbase), TAG (latest),
# PUSH=1 to docker-push, VARIANT (below), DISK_SIZE, QEMU_MEM (4096),
# QEMU_SMP (min(8,nproc)), BASE_IMAGE_URL, CACHE_DIR
# (~/.cache/whistler/vm-images), BAKE_TIMEOUT (2700s), PIXI_VERSION,
# NVIDIA_DRIVER_PACKAGE, CUDA_REPO_DISTRO, CUDA_TOOLKIT_PACKAGES.
#
# THREE VARIANTS, selected by VARIANT and distinguished by the image NAME:
#
#   VARIANT=base      whistler-devbase           no NVIDIA anything. Runs on
#                     any node; the toolchain only.
#   VARIANT=cuda      whistler-devbase-cuda      + the NVIDIA open driver, i.e.
#                     the GPU *runtime*: libcuda, the PTX JIT, OptiX, NVENC,
#                     nvidia-smi. This is what PyTorch/JAX wheels and Blender
#                     actually load (they carry their own cudart/cuBLAS/cuDNN).
#                     No nvcc, no CUDA headers, no docs — the same content
#                     split as the -cuda desktop images.
#   VARIANT=cuda-dev  whistler-devbase-cuda-dev  + the CUDA SDK from NVIDIA's
#                     apt repo: nvcc, headers, the dev libraries and the
#                     command-line tools (~4.6GB). For compiling CUDA C++
#                     in-guest — flash-attn, DeepSpeed JIT ops,
#                     torch.utils.cpp_extension.
#
# The variant rides in the image NAME, never the tag, and the dev tag is
# `latest` — both to keep Kubernetes' tag-based imagePullPolicy defaulting
# working. Kubelet (and KubeVirt for containerDisks) defaults ONLY the exact
# tag `:latest` to Always and everything else to IfNotPresent, so a mutable dev
# tag must literally be `:latest` or nodes silently keep booting a stale cached
# qcow2 after a rebuild. A `:latest-cuda` would NOT match. Production uses
# immutable versioned tags, which correctly default to IfNotPresent.
#
# Everything is baked rather than installed at session time because the default
# zone blocks package mirrors: a session gets what the image has.
#
# amd64-only: the bake runs the target-arch guest under KVM; producing arm64
# needs an arm64 host (or an emulated ~hour-long TCG bake nobody wants).
set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${IMAGE:-localhost:5000/whistler-devbase}"
TAG="${TAG:-latest}"
PUSH="${PUSH:-0}"
VARIANT="${VARIANT:-base}"
QEMU_MEM="${QEMU_MEM:-4096}"
QEMU_SMP="${QEMU_SMP:-$(( $(nproc) < 8 ? $(nproc) : 8 ))}"
BASE_IMAGE_URL="${BASE_IMAGE_URL:-https://cloud-images.ubuntu.com/releases/26.04/release/ubuntu-26.04-server-cloudimg-amd64.img}"
CACHE_DIR="${CACHE_DIR:-$HOME/.cache/whistler/vm-images}"
BAKE_TIMEOUT="${BAKE_TIMEOUT:-2700}"
# Pinned so a rebuild is reproducible; bump deliberately. The static musl build
# is a single binary → /usr/local/bin/pixi, system-wide. (The upstream
# installer script would put it in ~/.pixi, i.e. on the NFS home, where it
# would be per-user, un-updatable by a rebuild, and shadowed across images.)
PIXI_VERSION="${PIXI_VERSION:-0.76.2}"

case "$VARIANT" in
  base)
    NVIDIA_DRIVER_PACKAGE=""
    CUDA_TOOLKIT_PACKAGES=""
    DISK_SIZE="${DISK_SIZE:-16G}"
    ;;
  cuda)
    # 26.04's open-kernel driver branch. A wrong name fails the bake.
    NVIDIA_DRIVER_PACKAGE="${NVIDIA_DRIVER_PACKAGE:-nvidia-driver-595-open}"
    CUDA_TOOLKIT_PACKAGES=""
    IMAGE="${IMAGE}-cuda"
    DISK_SIZE="${DISK_SIZE:-24G}"
    ;;
  cuda-dev)
    NVIDIA_DRIVER_PACKAGE="${NVIDIA_DRIVER_PACKAGE:-nvidia-driver-595-open}"
    # CUDA 13.3 from NVIDIA's own apt repo, NOT 26.04's archive
    # `nvidia-cuda-toolkit`: the archive one is CUDA 12.4, whose nvcc rejects
    # this release's default gcc 15 and clang 21 as host compilers (it would
    # need gcc-13 and -ccbin on every compile), and its runtime is a major
    # version behind the 595 driver. 13.3 matches the driver and takes gcc 15.
    #
    # A curated package list, not the `cuda-toolkit-13-3` umbrella: that
    # metapackage *Depends* (not Recommends, so --no-install-recommends does
    # not help) on cuda-documentation-13-3 and on cuda-visual-tools-13-3, which
    # drags in the Nsight Compute/Systems GUI profilers — ~1.3GB of docs and
    # desktop tooling for an image that has no desktop. What's kept:
    #   cuda-compiler-*           nvcc, ptxas, cicc, nvlink, nvvm  (~0.45GB)
    #   cuda-libraries-dev-*      cuBLAS/cuFFT/cuRAND/cuSOLVER/cuSPARSE/NPP
    #                             headers, static libs, cudart-dev  (~3.85GB)
    #   cuda-command-line-tools-* cuda-gdb, compute-sanitizer, nvdisasm,
    #                             CUPTI, NVTX  (~0.32GB)
    #   cuda-nvml-dev-*           NVML headers
    # Override to widen (e.g. add cuda-visual-tools-13-3) or to move CUDA
    # versions; empty it and this is just the -cuda variant under a new name.
    CUDA_TOOLKIT_PACKAGES="${CUDA_TOOLKIT_PACKAGES:-cuda-compiler-13-3 cuda-libraries-dev-13-3 cuda-command-line-tools-13-3 cuda-nvml-dev-13-3}"
    IMAGE="${IMAGE}-cuda-dev"
    DISK_SIZE="${DISK_SIZE:-40G}"
    ;;
  *)
    echo "ERROR: VARIANT must be one of: base, cuda, cuda-dev (got '$VARIANT')" >&2
    exit 1
    ;;
esac
CUDA_REPO_DISTRO="${CUDA_REPO_DISTRO:-ubuntu2604}"
# Fixed port is fine: it only exists inside the bake container's netns.
HTTP_PORT=8099

BUILD_DIR="$PWD/build"
STAGE_DIR="$BUILD_DIR/stage"
SEED_DIR="$BUILD_DIR/seed"

[ -e /dev/kvm ] || { echo "ERROR: no /dev/kvm — the bake needs KVM" >&2; exit 1; }

rm -rf "$BUILD_DIR"
mkdir -p "$STAGE_DIR" "$SEED_DIR" "$CACHE_DIR"

echo "==> [1/4] Guest payload ($VARIANT)"
cp -a guest/. "$STAGE_DIR/"
# --owner/--group 0: the tar is created by an ordinary user but extracted by
# root in the guest, where tar would otherwise faithfully restore this uid.
# --mode go-w: the staging dir inherits the repo's group-writable modes, and
# extracting ./etc with g+w onto the guest made sshd's StrictModes refuse the
# whole /etc/ssh/authorized_keys.d path ("bad ownership or modes for directory
# /etc") — which surfaces only once the NFS home shadows
# ~/.ssh/authorized_keys, i.e. exactly when the root-disk path must work.
tar --owner=0 --group=0 --mode=go-w -czf "$SEED_DIR/artifacts.tar.gz" -C "$STAGE_DIR" .

echo "==> [2/4] Base cloud image"
BASE_IMG="$CACHE_DIR/$(basename "$BASE_IMAGE_URL")"
[ -s "$BASE_IMG" ] || curl -fL --progress-bar -o "$BASE_IMG" "$BASE_IMAGE_URL"

echo "==> [3/4] Bake boot (qemu/KVM in docker; console: $BUILD_DIR/console.log)"
sed -e "s/@HTTP_PORT@/$HTTP_PORT/g" \
    -e "s/@VARIANT@/$VARIANT/g" \
    -e "s/@PIXI_VERSION@/$PIXI_VERSION/g" \
    -e "s/@NVIDIA_DRIVER_PACKAGE@/$NVIDIA_DRIVER_PACKAGE/g" \
    -e "s/@CUDA_REPO_DISTRO@/$CUDA_REPO_DISTRO/g" \
    -e "s/@CUDA_TOOLKIT_PACKAGES@/$CUDA_TOOLKIT_PACKAGES/g" \
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
