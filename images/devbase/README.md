# devbase — the development server VM image

A **server** guest: no desktop, no X, no Selkies. You reach it the way you
reach a machine — `ssh <session>.w` through the gateway's jump host — and it is
set up for development out of the box.

Three variants, one bake script:

| VARIANT    | image                          | what it adds                                       | disk |
|------------|--------------------------------|----------------------------------------------------|------|
| `base`     | `whistler-devbase`             | the toolchain only; runs on any node                | 16G  |
| `cuda`     | `whistler-devbase-cuda`        | + NVIDIA open driver = the GPU **runtime** (no nvcc)| 24G  |
| `cuda-dev` | `whistler-devbase-cuda-dev`    | + the CUDA **SDK**: nvcc, headers, dev libraries    | 40G  |

```bash
make devbase-image                        # base
make devbase-image VARIANT=cuda           # GPU runtime
make devbase-image VARIANT=cuda-dev       # GPU runtime + nvcc
make devbase-image VARIANT=cuda PUSH=1    # and push to the dev registry
```

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
session teardown, image rebuilds, and moving between the three variants.
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
- **`cuda-dev`** adds the SDK on top, for compiling CUDA C++ in-guest
  (flash-attn, DeepSpeed JIT ops, `torch.utils.cpp_extension`): nvcc and the
  compiler chain, `cuda-libraries-dev` (cuBLAS/cuFFT/cuRAND/cuSOLVER/cuSPARSE/
  NPP headers + static libs) and the command-line tools (`cuda-gdb`,
  `compute-sanitizer`, `nvdisasm`, CUPTI, NVTX). ~4.6GB.

Two deliberate choices there:

**NVIDIA's apt repo, not 26.04's `nvidia-cuda-toolkit`.** The archive package is
CUDA 12.4, a major version behind the 595 driver, and its nvcc refuses this
release's default gcc 15 and clang 21 as host compilers — you would be passing
`-ccbin gcc-13` forever. `cuda-toolkit-13-3` from
`developer.download.nvidia.com/compute/cuda/repos/ubuntu2604` matches the
driver and takes gcc 15.

**A curated package list, not the `cuda-toolkit-13-3` umbrella.** That
metapackage *Depends* — not Recommends, so `--no-install-recommends` does not
help — on `cuda-documentation-13-3` and on `cuda-visual-tools-13-3`, which pulls
the Nsight Compute/Systems **GUI** profilers: ~1.3GB of docs and desktop tooling
in an image that has no desktop. `CUDA_TOOLKIT_PACKAGES` is the knob if you want
them anyway (add `cuda-visual-tools-13-3`), or to move CUDA versions.

The pin in `/etc/apt/preferences.d/nvidia-archive-wins` keeps Ubuntu's driver
authoritative: NVIDIA's repo also ships driver packages that would otherwise
outrank the archive's by version and silently replace the open driver.

### …or skip `cuda-dev` and install the SDK with pixi

The driver has to be baked — a kernel module and `libcuda.so.1` are not
userspace — but **everything above it can live in the user's `$HOME`**, and
conda-forge deliberately does not ship `libcuda`, so a pixi environment binds to
the baked driver exactly as intended. That makes the `cuda` variant the real
workhorse and `cuda-dev` a convenience rather than a necessity.

```toml
# pixi.toml — a per-project CUDA toolchain, no root, no new image
[workspace]
channels  = ["conda-forge"]
platforms = ["linux-64"]
# If the solver refuses a CUDA-runtime package because it cannot see a GPU
# (no `__cuda` virtual package — e.g. you are on a driver-less box), declare
# the level instead:  platforms = [{ platform = "linux-64", cuda = "13" }]

[dependencies]
cuda-nvcc       = "13.3.*"   # same version the cuda-dev image bakes
cuda-cudart-dev = "*"
libcublas-dev   = "*"
```

Measured on conda-forge (`cuda-nvcc` 13.3.73, i.e. version parity with the apt
SDK): **1.8GB** for nvcc + cudart-dev + cuBLAS-dev, installed cold in seconds on
a fast link. A *second* environment from the warm cache costs ~0s and ~0 bytes —
pixi hardlinks packages out of the cache when the cache and the environment sit
on the same filesystem, which under `$HOME` they do (verified: link count 2,
cache + two full environments still totalling 1.8G).

Three things decide whether this beats baking a third image:

1. **Egress.** pixi needs `conda.anaconda.org`, and the default zone blocks
   package mirrors. Without a zone that permits it, userspace CUDA is simply
   impossible and the baked SDK is the only path. This is the gating decision,
   not a detail.
2. **Home PVC size.** `userVolume` defaults to 10Gi; 1.8G per CUDA stack (plus
   ~1GB for cuDNN, ~3GB if PyTorch comes along) eats it quickly. Raise it for
   users who work this way.
3. **NFS.** An environment is ~6,400 files. Extraction and tooling that stats
   the tree (clangd, pip) are slower over NFS than on local disk — how much
   slower against a real storage gateway is unmeasured.

The upside is that the combinatorial explosion — CUDA version × cuDNN × Python ×
framework — moves into a `pixi.lock` per project, where it is pinned and
reproducible, instead of into image names.

## How the bake works

Identical in structure to `desktops/vm-xfce-selkies` and
`desktops/vm-gnome-selkies` — read either of those for the shared mechanics —
minus the artifact *builder* stage, because there is no Selkies venv to
ABI-match to the guest. Everything comes from the guest's own apt plus two
pinned upstream downloads (pixi; NVIDIA's CUDA repo for `cuda-dev`).

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
cached qcow2 after every rebuild. See `desktops/vm-xfce-selkies/build.sh` for
the long version.

## Using it

`charts/whistler/values-dev-vm.yaml` (the skaffold dev overlay) defines
`devbase`, `devbase-cuda` and `devbase-cuda-dev` templates and lists the three
images in `whistler.images.vm` — a VM template whose boot source is not on that
list is refused by policy at reconcile time.

They are `mode: ssh, runtime: vm` templates, which is the point: cloud-init
creates the user, installs their keys, mounts the NFS home and the session is a
plain SSH host. No `viewer`, no streamer, no screenshots. The KubeVirt serial
console and the portal's web terminal still work as rescue paths.

```
ssh -J whistler-gateway devbase-1.w      # or the ProxyJump stanza from the TUI's `?`
```
