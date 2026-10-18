# devbase — the development server VM image

A **server** guest: no desktop, no X, no Selkies. You reach it the way you
reach a machine — `ssh <session>.w` through the gateway's jump host — and it is
set up for development out of the box.

Two variants, one bake script:

| VARIANT    | image                          | what it adds                                       | disk |
|------------|--------------------------------|----------------------------------------------------|------|
| `base`     | `whistler-devbase`             | the toolchain only; runs on any node                | 16G  |
| `cuda`     | `whistler-devbase-cuda`        | + NVIDIA open driver = the GPU **runtime** (no nvcc), Container Toolkit | 24G  |

```bash
make devbase-image                        # base + cuda
make devbase-image VARIANT=cuda           # just the GPU runtime one
make devbase-image PUSH=1                 # and push to the dev registry
```

A template names `whistler-devbase`, and a session with a GPU boots
`whistler-devbase-cuda` in its place (if it is in `whistler.images.vm`),
which is why the default builds both. Neither bakes the CUDA *SDK*; a user
who wants nvcc installs it themselves, e.g. in a pixi environment — see
[Compiling CUDA](#compiling-cuda-bring-your-own-sdk).

Needs docker, curl and a `/dev/kvm` node — nothing else on the host (the qemu
that boots the guest runs in a container). amd64 only, like its siblings.

## What's in it

`cat /etc/whistler-devbase-release` in a session prints the variant and the
resolved versions; it is also the login MOTD. On Ubuntu 26.04:

- **Python 3.14** — the system `python3`. 26.04 ships it, which is the whole
  reason this image is 26.04 and not the 24.04 the GNOME desktop image is
  pinned to; no deadsnakes.
- **clang / clang++ 21** — likewise from the archive, no LLVM apt repo, with
  `clangd`, `clang-format`, `clang-tidy`, `lld`, `lldb` and `libclang-rt-dev`
  (the last one is what makes `-fsanitize=address` actually link).
- **pixi** — pinned static build at `/usr/local/bin/pixi`, plus bash
  completions.
- gcc/g++ 15, make, cmake, ninja, pkg-config, gdb.
- git, git-lfs, curl, wget, rsync, jq, unzip/zip, xz, zstd.
- python3-dev, python3-venv, python3-pip, pipx.
- vim, nano, tmux, htop, less, man.
- **Docker** — `docker.io` from the archive, with the `compose` and `buildx`
  plugins; see [Docker](#docker).
- `nvtop` and the NVIDIA Container Toolkit in the `cuda` variant.

### pixi and $HOME

`pixi` itself is a system binary so an image rebuild can update it. Everything
pixi *installs* stays in `$HOME/.pixi` — and `$HOME` is the user's PVC, mounted
over NFS from their storage gateway, so a user's `pixi global install`s survive
session teardown, image rebuilds, and moving between the two variants.
`$HOME/.pixi/bin` is put on PATH by `/etc/profile.d/50-whistler-devbase.sh`.

Note that profile.d is only read by **login shells**: `ssh host -- some-command`
does not get it. Use absolute paths in scripts. The CUDA SDK's tools are
therefore *symlinked* into `/usr/local/bin` at bake time rather than left to
profile.d — `ssh <session>.w nvcc …` has to work — and `test.sh` asserts exactly
that with a stripped `env -i` PATH.

### Docker

`docker` works as the session user without `sudo`: the socket's group is
`sudo` (`guest/etc/systemd/system/docker.socket.d`), not `docker`. Socket
access is root on the machine, and the session user already has NOPASSWD sudo,
so this grants nothing new — it just avoids a group the per-session cloud-init
would have to add.

dockerd is **socket-activated**: it starts on the first `docker` command, not
at boot, so sessions that never use it don't pay for it at startup. A container
with a restart policy therefore stays down after a reboot until the socket is
touched; `sudo systemctl enable docker` restores the stock behaviour.

Images and containers live in `/var/lib/docker` on the **root disk**, not the
home. On a containerDisk boot that disk is ephemeral and 16G (24G for `-cuda`),
so pulled images are gone at the next session and large ones fill it. Pulling
also needs a zone with egress to the registry (`registry-1.docker.io`,
`production.cloudflare.docker.com` for Docker Hub). Container logs use the
`local` driver, which rotates.

Docker's networks use `198.18.0.0/15` (the default bridge is `198.18.0.1/24`;
`docker network create` takes /24s from `198.19.0.0/16`), not the stock
`172.17.0.0/16`-and-up. The guest routes every bridge's subnet to that bridge,
and SSH clients reach a session from their real address, so a stock pool that
overlapped a client's network would lock that client out as soon as dockerd
started. 198.18/15 is reserved for benchmarking and is not used on real
networks. If it collides anyway, change `/etc/docker/daemon.json` and
`sudo systemctl restart docker`.

Docker's networks get the **MTU of the VM's uplink**, not Docker's fixed 1500.
A session's uplink is whatever the cluster's pod network is: 1450 under
flannel VXLAN (k3s's default), 1500 under `host-gw`, 8950 on jumbo frames.
Above the uplink's MTU a container's packets are not refused but dropped on the
way out, so small requests work and a TLS handshake or a `docker pull` hangs.
Each time dockerd starts, `/usr/local/lib/whistler/docker-mtu` (an
`ExecStartPre` in `/etc/systemd/system/docker.service.d/10-whistler-mtu.conf`)
writes the uplink's MTU into `daemon.json` as `mtu` (the default bridge) and as
the bridge driver's default `com.docker.network.driver.mtu` (`docker network
create`, compose), leaving the other keys alone. A network created earlier
keeps the MTU it was made with, so after a cluster's MTU changes, recreate it
(`docker compose down && docker compose up`). To set your own MTU, delete the
drop-in, `sudo systemctl daemon-reload`, and edit `daemon.json`.

**GPUs in containers** work in the `cuda` variant, which bakes the NVIDIA
Container Toolkit (from NVIDIA's apt repo, pinned by
`NVIDIA_CONTAINER_TOOLKIT_VERSION` in build.sh; it is not in Ubuntu's archive):

```bash
docker run --rm --gpus all ubuntu nvidia-smi       # the toolkit mounts nvidia-smi in
docker run --rm --device nvidia.com/gpu=all ...   # CDI
docker run --rm --runtime=nvidia ...              # the registered runtime
```

Compose's `deploy.resources.reservations.devices` with `driver: nvidia` works
too. The `nvidia` runtime is in `daemon.json` but is not the default. The CDI
spec (`/var/run/cdi/nvidia.yaml`) is made at each boot by the toolkit's
`nvidia-cdi-refresh`, which udev starts when the driver loads, and Docker
waits for it. The image is pulled like any other, so the zone needs egress to
its registry. The `base` variant has neither the driver nor the toolkit.

### VS Code Remote

The image sets `VSCODE_AGENT_FOLDER=/var/lib/vscode-server`, so the VS Code
server installs to the VM's **local disk** rather than into `$HOME`. The home is
an NFS share, and the server install — ~700MB and ~3000 files, extracted and
then directory-renamed into place — is the worst workload to aim at one:
observed here, it never completed, and while it ran, unrelated `stat()`s
elsewhere in the home intermittently failed.

The tradeoff: that path is on the *ephemeral* root disk, so a new session
re-downloads the server (~227MB) and re-installs extensions — use Settings Sync,
or override it for a session with
`export VSCODE_AGENT_FOLDER="$HOME/.vscode-server"`.

Connect with the `ProxyJump` stanza from the launcher's `?` screen. Make sure
`AddKeysToAgent yes` is in it: a jump is two logins, so an agent-less encrypted
key is a passphrase prompt per hop, per connection — and VS Code opens several.

### Network

The default zone blocks package mirrors, which is why everything here is baked
rather than installed on first boot. It is also why `pixi install` /
`apt install` inside a session will hang unless the session's zone permits
egress to the relevant hosts — pick or define a zone accordingly
(`design/zones` and the portal's Zones section).

## CUDA, and what "CUDA available" means

The split is the same one the `-cuda` desktop images make, and it is worth
being precise about because the names are misleading:

- **`cuda`** installs the NVIDIA *driver*, which carries the entire GPU
  **runtime**: `libcuda.so.1`, the PTX JIT, OptiX, NVENC, `nvidia-smi`. That is
  everything a GPU *workload* loads — PyTorch and JAX wheels bring their own
  cudart/cuBLAS/cuDNN, Blender ships precompiled kernels. No nvcc, no headers,
  no documentation.
- **The SDK is not baked at all.** Nothing in the image or at session start
  installs it. A user who compiles CUDA C++ in-guest (flash-attn, DeepSpeed
  JIT ops, `torch.utils.cpp_extension`) brings nvcc and the dev libraries
  themselves; a pixi environment is the suggested way, below.

### Compiling CUDA: bring your own SDK

This is something a user *may* do, per project; Whistler does none of it.
The driver has to be baked — a kernel module and `libcuda.so.1` are not
userspace — but **everything above it can live in the user's `$HOME`**, and
conda-forge deliberately does not ship `libcuda`, so a pixi environment binds to
the baked driver exactly as intended.

```toml
# pixi.toml — a per-project CUDA toolchain, no root, no new image
[workspace]
channels  = ["conda-forge"]
platforms = ["linux-64"]
# If the solver refuses a CUDA-runtime package because it cannot see a GPU
# (no `__cuda` virtual package — e.g. you are on a driver-less box), declare
# the level instead:  platforms = [{ platform = "linux-64", cuda = "13" }]

[dependencies]
cuda-nvcc       = "13.3.*"
cuda-cudart-dev = "*"
libcublas-dev   = "*"
```

Measured on conda-forge (`cuda-nvcc` 13.3.73): **1.8GB** for nvcc +
cudart-dev + cuBLAS-dev, installed cold in seconds on a fast link. A *second*
environment from the warm cache costs ~0s and ~0 bytes — pixi hardlinks
packages out of the cache when the cache and the environment sit on the same
filesystem, which under `$HOME` they do (verified: link count 2, cache + two
full environments still totalling 1.8G).

A baked SDK image (`cuda-dev`, ~4.6GB of NVIDIA's apt packages) existed until
2026-10-10 and was removed for this: the combinatorial explosion — CUDA version
× cuDNN × Python × framework — belongs in a `pixi.lock` per project, where it
is pinned and reproducible, not in image names. Two things to plan for:

1. **Egress.** pixi needs `conda.anaconda.org`, and the default zone blocks
   package mirrors. Users who compile CUDA need a zone that permits it.
2. **Home size.** 1.8G per CUDA stack (plus ~1GB for cuDNN, ~3GB if PyTorch
   comes along) fills a small home quickly. Size it for users who work this
   way.

## How the bake works

Identical in structure to `desktops/vm-gnome-selkies` — read that for the
shared mechanics —
minus the artifact *builder* stage, because there is no Selkies venv to
ABI-match to the guest. Everything comes from the guest's own apt plus one
pinned upstream download (pixi).

1. `guest/` is tarred as the NoCloud seed's `artifacts.tar.gz`.
2. The Ubuntu 26.04 cloud image boots once under qemu/KVM in a container
   (`bake/Dockerfile.bake` + `bake/boot.sh`) against that seed served over HTTP;
   `bake/user-data.in` installs everything, resets cloud-init, powers off.
   Console output lands in `build/console.log` — the first place to look when a
   bake fails.
3. The qcow2 is wrapped as a KubeVirt containerDisk.

**Which cloud image.** By default the bake uses upstream's newest build of
the release (`releases/26.04/release/`), checked again on every bake. To use a
specific build, pin it with `BASE_IMAGE_SERIAL=<serial>`, for example
`make devbase-image BASE_IMAGE_SERIAL=20260927`; the serials are the
`release-<serial>/` directories under
<https://cloud-images.ubuntu.com/releases/26.04/>. `BASE_IMAGE_URL` overrides
both. [`scripts/fetch_cloud_image.sh`](../../scripts/fetch_cloud_image.sh)
checks the download against upstream's `SHA256SUMS` and caches it by hash in
`~/.cache/whistler/vm-images`, so switching between builds re-downloads only
the ones not already cached. The bake log prints the serial it used, and a
booted guest has it in `/etc/cloud/build.info`.

The variant rides in the image **name**, never the tag, and the dev tag is
literally `:latest`: kubelet and KubeVirt default only that exact tag to
`imagePullPolicy: Always`, so a `:latest-cuda` would leave nodes booting a stale
cached qcow2 after every rebuild. See `desktops/vm-gnome-selkies/build.sh` for
the long version.

## Using it

`charts/whistler/values-dev-vm.yaml` (the skaffold dev overlay) defines
the `devbase` template (GPU optional: a session with one boots
`whistler-devbase-cuda`) and lists both images in `whistler.images.vm`
— a VM template whose boot source is not on that list is refused by policy at
reconcile time.

They are `mode: ssh, runtime: vm` templates, which is the point: cloud-init
creates the user, installs their keys, mounts the NFS home and the session is a
plain SSH host. No `viewer`, no streamer, no screenshots. The KubeVirt serial
console and the portal's web terminal still work as rescue paths.

```
ssh -J whistler-gateway devbase-1.w      # or the ProxyJump stanza from the TUI's `?`
```
