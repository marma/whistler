# devbase — the development server VM image

A **server** guest: no desktop, no X, no Selkies. You reach it the way you
reach a machine — `ssh <session>.w` through the gateway's jump host — and it is
set up for development out of the box.

Two variants, one bake script:

| VARIANT    | image                          | what it adds                                       | disk |
|------------|--------------------------------|----------------------------------------------------|------|
| `base`     | `whistler-devbase`             | the toolchain only; runs on any node                | 16G  |
| `cuda`     | `whistler-devbase-cuda`        | + NVIDIA open driver = the GPU **runtime** (no nvcc)| 24G  |

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
- `nvtop` in the two CUDA variants.

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
