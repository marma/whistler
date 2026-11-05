# Preparing Linux nodes for KubeVirt

This covers the host side of every node that will run Whistler VM sessions:
firmware, kernel command line, modules, and the kubelet. The cluster side
(KubeVirt, CDI, the GPU allow-list in the KubeVirt CR) is in the README's
"1. KubeVirt" section and `scripts/install_kubevirt.sh`.

KubeVirt does not check any of these requirements itself. Nearly every
failure below looks the same from the cluster: the VM pends, or its VMI goes
`Scheduled → Failed` and lands in `CrashLoopBackOff`, and no error message
names the cause. Prepare each node completely before scheduling VMs on it.

`scripts/check_node.sh`, run on the node, checks everything below and
reports what is missing (`--help` for options).

| What | Needed for | Symptom when missing |
|---|---|---|
| `/dev/kvm` | every VM (or run emulated) | `Insufficient devices.kubevirt.io/kvm` |
| Hugepages reserved, kubelet restarted | every VM (Whistler default) | VM pends on `Insufficient hugepages-2Mi` |
| kubelet root at `/var/lib/kubelet` | every VM | VMI fails in ~3 s, `CrashLoopBackOff`, nothing logged |
| IOMMU on in firmware and kernel | GPU passthrough | vfio-pci cannot bind the card |
| GPU bound to `vfio-pci` | GPU passthrough | resource never advertised, VM pends |
| `vfio_iommu_type1` loaded | GPU passthrough | VMI fails, `No available IOMMU models` |
| GPU alone in its IOMMU group | GPU passthrough | device not assignable |

No libvirt or qemu is installed on the node. KubeVirt ships both inside its
`virt-launcher` pods; the host provides the kernel side only.

The commands below are written for Ubuntu/Debian. RHEL-family equivalents
are given where they differ.

## 1. Editing the kernel command line

Hugepages and IOMMU settings both go on the kernel command line, so here
is how to change it once. `scripts/check_node.sh` ends with the complete
line for this node (existing arguments kept, missing ones added, harmful
ones removed), or the `grubby` command on RHEL-family systems.
`--pool-gb <GiB>` sets the hugepage count in it.

Ubuntu / Debian: edit `/etc/default/grub` and append to the existing value:

```bash
GRUB_CMDLINE_LINUX_DEFAULT="quiet splash intel_iommu=on iommu=pt hugepagesz=2M hugepages=16384"
```

```bash
sudo update-grub
sudo reboot
```

RHEL / Rocky / Fedora: use `grubby`, which edits every installed kernel's
entry (BLS):

```bash
sudo grubby --update-kernel=ALL --args="intel_iommu=on iommu=pt hugepagesz=2M hugepages=16384"
sudo grubby --info=DEFAULT          # check
sudo reboot
```

After the reboot, check the arguments the running kernel actually got:

```bash
cat /proc/cmdline
```

Cloud images and some vendor installs add files under
`/etc/default/grub.d/*.cfg` that are read after `/etc/default/grub`. If an
argument does not show up in `/proc/cmdline`, look for an override there.

## 2. Hardware virtualization: `/dev/kvm`

- Enable VT-x (Intel) or AMD-V/SVM (AMD) in the firmware. `grep -cE 'vmx|svm'
  /proc/cpuinfo` should be non-zero.
- `kvm_intel` or `kvm_amd` creates `/dev/kvm`. Most distributions load it
  automatically; if not:

  ```bash
  sudo modprobe kvm_intel             # or kvm_amd
  echo kvm_intel | sudo tee /etc/modules-load.d/kvm.conf
  ```

To check what the scheduler sees once virt-handler runs on the node:

```bash
kubectl get node <node> -o jsonpath='{.status.allocatable.devices\.kubevirt\.io/kvm}{"\n"}'
```

If the node is itself a VM, the outer host needs nested virtualization
enabled (`/sys/module/kvm_intel/parameters/nested` = `Y`;
`options kvm_intel nested=1` in `/etc/modprobe.d/`). Without KVM at all,
KubeVirt can emulate (`KUBEVIRT_USE_EMULATION=1` to `install_kubevirt.sh`).
Emulation is slow and applies to the whole cluster.

## 3. Hugepages on the node

Whistler runs VM guest RAM on 2 MiB hugepages by default. VFIO pins the
whole guest when a GPU attaches, and with 4 KiB pages a large guest misses
virt-handler's fixed 20 s deadline. The kernel never allocates hugepages
on demand. They must be reserved in advance, and the kubelet only offers
reserved pages to pods. A node with no pool pends every VM.

### Sizing

The pool size is the total RAM of all guests the node should run at once:

| Page size | Count per GiB | 32 GiB of guests | 512 GiB of guests |
|---|---|---|---|
| 2 MiB | 512 | 16384 | 262144 |
| 1 GiB | 1 | 32 | 512 |

Pages in the pool are removed from ordinary memory, whether or not a
guest is using them. A 512 GiB pool on a 1 TiB node leaves about 512 GiB for
the OS and all other pods. Leave enough room for the kernel, the page
cache, containers and per-VM overhead outside the guest (virt-launcher,
qemu itself: a few hundred MiB per VM).

### Reserving at boot (recommended)

At boot, memory has not fragmented yet, so the reservation always
succeeds. Add to the kernel command line (section 1):

```
hugepagesz=2M hugepages=16384
```

For 1 GiB pages, which in practice can only be reserved at boot. The CPU
must support them: `grep -c pdpe1gb /proc/cpuinfo` must be non-zero.
Every current Intel Xeon and AMD EPYC/Ryzen does.

```
default_hugepagesz=1G hugepagesz=1G hugepages=512
```

Both sizes can be reserved together. Each `hugepages=` applies to the
`hugepagesz=` before it:

```
hugepagesz=1G hugepages=256 hugepagesz=2M hugepages=8192
```

On x86_64 only 2 MiB and 1 GiB exist. Whistler's matching setting is
`whistler.hugePages.pageSize` (`2Mi`, `1Gi`, or `""` for ordinary pages);
a node with only 1 GiB pages needs it set to `1Gi`.

### Reserving at runtime

Use this to change a running node. 2 MiB pages usually still allocate on a
running host. The kernel reserves as many as it can find, which can be
fewer than requested, so read the value back:

```bash
echo 16384 | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages      # what you actually got
```

If the result is short, dropping the page cache first frees contiguous
memory: `sync; echo 3 | sudo tee /proc/sys/vm/drop_caches`, then write the
count again.

To keep a runtime setting across reboots, use one of the following:

```bash
# Default page size only (2M unless default_hugepagesz= says otherwise):
echo 'vm.nr_hugepages = 16384' | sudo tee /etc/sysctl.d/80-hugepages.conf

# Any page size, through sysfs, applied early at boot by systemd-tmpfiles:
echo 'w /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages - - - - 16384' \
  | sudo tee /etc/tmpfiles.d/hugepages.conf
```

Both run early in boot, before memory fragments, but the kernel command
line is still the more reliable way to get large pools.

### NUMA

A boot-time pool is spread evenly across NUMA nodes. To see where the pages
are:

```bash
grep -H . /sys/devices/system/node/node*/hugepages/hugepages-2048kB/nr_hugepages
```

The pool for one NUMA node can be set at runtime through the same file
under `/sys/devices/system/node/nodeN/`. Recent kernels also accept a
per-node split on the command line (`hugepages=0:8192,1:8192`). A GPU
guest runs best on pages from the GPU's NUMA node, which is listed in
`/sys/bus/pci/devices/<address>/numa_node`. The kubelet does not place
pages by NUMA node unless its Memory Manager and Topology Manager are set
up for it, so an even split is the safe default.

### Making the kubelet see the pool

**The kubelet reads the hugepage pools only at startup.** After a runtime
change, restart it (the unit name per distribution is in section 7).
Until then the node advertises `hugepages-2Mi: 0` and every VM pends on
`Insufficient hugepages-2Mi`, even though `/proc/meminfo` shows the pages
free. On restart the kubelet re-admits running pods against the smaller
ordinary-memory figure, so drain the node first if the pool is large.

Check both sides:

```bash
grep -i huge /proc/meminfo                       # kernel: HugePages_Total / _Free
kubectl get node <node> -o jsonpath='{.status.capacity.hugepages-2Mi}{" "}{.status.allocatable.hugepages-2Mi}{"\n"}'
```

The scheduler uses `allocatable`.

Nothing else is needed on the node. The container runtime and KubeVirt
mount hugetlbfs for the virt-launcher pod themselves. Transparent hugepages
(THP, `/sys/kernel/mm/transparent_hugepage`) are a separate mechanism, and
these settings do not depend on them.

`scripts/metal_k3s_create.sh` reports both pool sizes when it runs.

## 4. GPU passthrough on the node

The goal is for the GPU to be bound to `vfio-pci`, not `nvidia` or `nouveau`,
with the IOMMU on and the VFIO type1 backend loaded. KubeVirt then hands
the device to the guest, and the NVIDIA driver runs inside the guest, not
on the host.

### 4.1 Firmware

| Setting | Intel | AMD |
|---|---|---|
| CPU virtualization | "Intel Virtualization Technology" / VT-x | "SVM Mode" |
| IOMMU | "VT-d" / "Intel VT for Directed I/O" | "IOMMU" or "AMD-Vi", often under the NBIO / chipset menu |

- **IOMMU: Enabled, not Auto.** On some AMD boards "Auto" leaves the IOMMU
  off.
- **Above 4G decoding**: enabled. Modern GPUs have BARs that are many GiB
  in size and cannot be mapped below 4 GiB. Resizable BAR can stay at the
  vendor default.
- **SR-IOV support**: enabled on server boards where offered. It is not
  needed for whole-GPU passthrough, but some firmware ties ACS and PCIe
  isolation settings to it.
- **CSM / legacy boot**: disabled (UEFI boot) where possible.

### 4.2 Find the GPU: address and IDs

```bash
lspci -nnk -d 10de:
```

```
bd:00.0 VGA compatible controller [0300]: NVIDIA Corporation AD102 [GeForce RTX 4090] [10de:2684] (rev a1)
        Kernel driver in use: nvidia
bd:00.1 Audio device [0403]: NVIDIA Corporation AD102 High Definition Audio Controller [10de:22ba] (rev a1)
        Kernel driver in use: snd_hda_intel
```

Three things to take from this output:

- **Bus address** `bd:00.0` (in full, `0000:bd:00.0`). This identifies one
  physical card.
- **Vendor:device ID** `10de:2684`. This identifies the *model*, so every
  identical card has the same ID. This is the ID that
  `install_kubevirt.sh --allow-gpu` and the KubeVirt CR's
  `pciVendorSelector` use.
- **The card's other functions** (`.1` audio here; some cards also have USB-C
  or serial functions). They are in the same IOMMU group and must be bound
  to vfio-pci too. Only the display function (class `03xx`) goes on the
  KubeVirt allow-list.

No `lspci`? Read sysfs directly:

```bash
for d in /sys/bus/pci/devices/*; do
  [ "$(cat $d/vendor)" = 0x10de ] && echo "$(basename $d) $(cat $d/class) $(cat $d/device)"
done
```

### 4.3 Turn on the IOMMU (kernel command line)

The IOMMU translates and restricts the DMA a device can perform. VFIO
needs it so that a GPU given to a guest can only reach that guest's
memory. Intel calls its IOMMU VT-d (the kernel driver logs as `DMAR`). AMD
calls its IOMMU AMD-Vi (the driver logs as `AMD-Vi`).

| CPU | Add | Notes |
|---|---|---|
| Intel | `intel_iommu=on iommu=pt` | Many distribution kernels build the Intel IOMMU default-off, so set it explicitly |
| AMD | `iommu=pt` | The AMD IOMMU is on whenever firmware exposes it; there is nothing to switch on |

**Intel parameters:**

- `intel_iommu=on` enables VT-d.
- `intel_iommu=off` disables it.

**AMD parameters (`amd_iommu=`):**

- **`on` does not exist.** The driver logs `AMD-Vi: Unknown option - 'on'`
  and ignores it. It is harmless but misleading.
- **`off`** disables the IOMMU. Make sure nothing on the command line or in
  `/etc/default/grub.d/` sets it.
- **`force_isolation`** does not help passthrough. It only stops host
  devices from being relaxed into identity domains, it has nothing to do
  with VFIO, and it slows DMA on every NIC and NVMe. Do not add it.
- **`force_enable`** is only for firmware that wrongly hides a working IOMMU.
  Leave it out unless dmesg shows that the IOMMU was disabled by firmware.

**Shared parameter:** `iommu=pt` (passthrough mode) lets host-driven
devices skip IOMMU translation, while devices assigned through VFIO still
get a translated domain. It is an optimisation, not a requirement, and it
works the same on both vendors.

**Interrupt remapping.** VFIO refuses to assign a device unless the IOMMU
also remaps interrupts. Without remapping, a device could inject arbitrary
interrupts into the host. Every current server platform supports it, on
Intel and AMD alike. If dmesg says remapping is off, fix the firmware
setting (x2APIC / interrupt remapping). Do not use
`vfio_iommu_type1.allow_unsafe_interrupts=1`, which gives up the
protection.

After rebooting, check:

```bash
# Intel
sudo dmesg | grep -E 'DMAR|DMAR-IR'      # "DMAR: IOMMU enabled", "DMAR-IR: Enabled IRQ remapping in x2apic mode"
# AMD
sudo dmesg | grep AMD-Vi                 # "AMD-Vi: Found IOMMU at ...", "AMD-Vi: Interrupt remapping enabled"
# Both
ls /sys/kernel/iommu_groups/ | wc -l     # must be > 0
ls /sys/class/iommu/                     # one entry per IOMMU unit (dmar0..., ivhd0...)
```

If `/sys/kernel/iommu_groups/` is empty, the IOMMU is not active: either
firmware has it off or a kernel argument disables it.

**AMD EPYC notes:**

- EPYC has several IOMMUs, one per PCIe root complex, so
  `/sys/class/iommu/` lists several `ivhd` entries. That is expected.
- Each root port is normally its own IOMMU group, so GPUs in CPU-attached
  slots are well isolated.
- The NUMA layout depends on the firmware's NPS setting (NPS1/2/4). Check
  the GPU's NUMA node in `/sys/bus/pci/devices/<address>/numa_node` when
  splitting hugepages across NUMA nodes (section 3).

**AMD Ryzen / Threadripper notes:** devices behind the chipset (the lower
x16/x4 slots, onboard USB and SATA) often share a single group. Put
passthrough GPUs in the CPU-attached slots.

**Optional on AMD:** `kvm_amd` AVIC (`options kvm_amd avic=1`) accelerates
guest interrupt delivery on Zen 2 and later. It is not needed for
passthrough to work.

### 4.4 Check the IOMMU groups

The IOMMU group is the smallest unit that can be assigned. Every device in
a group must be bound to vfio-pci before any device in it can be assigned:

```bash
for g in /sys/kernel/iommu_groups/*; do
  echo "group ${g##*/}:"; for d in $g/devices/*; do echo "  $(lspci -nns ${d##*/})"; done
done
```

The GPU should be in a group containing only its own functions (`bd:00.0`
and `bd:00.1`). Data-centre cards and switched PCIe platforms (DGX/HGX) are
normally isolated per slot. On consumer boards a card in a chipset-attached
slot can share a group with unrelated devices (USB, SATA, NIC). If so, move
the card to a CPU-attached slot. The ACS override patch is not an
acceptable fix on a multi-user host.

### 4.5 Bind the card to vfio-pci

There are two ways to do this, and you need **one** of them.

**A. Let the NVIDIA GPU Operator do it.** This is what
`scripts/metal_k3s_create.sh` sets up. Install the operator with
`sandboxWorkloads.enabled=true` and label the node:

```bash
kubectl label node <node> nvidia.com/gpu.workload.config=vm-passthrough
```

The operator's vfio-manager unbinds every NVIDIA GPU on that node and binds
it to vfio-pci, and its sandbox-device-plugin advertises it (for example
`nvidia.com/AD102_GEFORCE_RTX_4090: 1`). The host NVIDIA driver may be
installed but must not keep the card open. Stop and disable
`nvidia-persistenced` for passthrough GPUs, and run nothing on the host
that uses them. No module blacklisting is needed for this route, except
`nouveau` (below). Whole-node granularity: every NVIDIA GPU on a labelled
node goes to VMs.

**B. Bind it statically at boot, with no operator.** Use this when the
node is dedicated to VMs, or when you want the binding fixed in the host
config.

1. Have vfio-pci claim the IDs, and make sure it loads before the GPU and
   audio drivers:

   ```bash
   # /etc/modprobe.d/vfio.conf
   options vfio-pci ids=10de:2684,10de:22ba
   softdep nvidia pre: vfio-pci
   softdep nouveau pre: vfio-pci
   softdep snd_hda_intel pre: vfio-pci
   ```

   (`vfio-pci.ids=10de:2684,10de:22ba` on the kernel command line does the
   same thing, which is useful if vfio-pci is built into the kernel.)

2. Blacklist the drivers that would otherwise claim the card:

   ```bash
   # /etc/modprobe.d/blacklist-gpu.conf
   blacklist nouveau
   options nouveau modeset=0
   # Only if NO GPU on this host should use the host NVIDIA driver:
   blacklist nvidia
   blacklist nvidia_drm
   blacklist nvidia_modeset
   blacklist nvidia_uvm
   ```

   `blacklist` only stops a module from loading automatically by alias.
   Another module, or a service such as `nvidia-persistenced`, can still
   load it explicitly. To block a module completely, add
   `install nvidia /bin/false`. Do not blacklist `snd_hda_intel` on a host
   that needs its own onboard audio. The `softdep` lines already let
   vfio-pci claim the GPU's audio function first.

3. Load the VFIO modules from the initramfs, so vfio-pci claims the card
   before anything else probes it:

   ```bash
   # Ubuntu / Debian
   printf 'vfio\nvfio_iommu_type1\nvfio_pci\n' | sudo tee -a /etc/initramfs-tools/modules
   sudo update-initramfs -u

   # RHEL family
   echo 'force_drivers+=" vfio vfio_iommu_type1 vfio_pci "' | sudo tee /etc/dracut.conf.d/vfio.conf
   sudo dracut -f
   ```

4. Reboot and check:

   ```bash
   lspci -nnk -s bd:00.0      # Kernel driver in use: vfio-pci
   lspci -nnk -s bd:00.1      # Kernel driver in use: vfio-pci
   ls -l /dev/vfio/           # one file per bound IOMMU group, plus /dev/vfio/vfio
   ```

*Identical cards, only some for passthrough.* `ids=` matches the model, so
it takes every card with that ID. To bind one card by bus address
instead, use `driver_override`. Run this from a systemd oneshot unit
ordered before the NVIDIA services and the kubelet:

```bash
for f in 0000:bd:00.0 0000:bd:00.1; do
  echo vfio-pci > /sys/bus/pci/devices/$f/driver_override
  [ -e /sys/bus/pci/devices/$f/driver ] && echo $f > /sys/bus/pci/devices/$f/driver/unbind
  echo $f > /sys/bus/pci/drivers_probe
done
```

*Advertising the card without the operator.* With route B, KubeVirt's own
device plugin advertises vfio-bound devices that the KubeVirt CR permits
with `externalResourceProvider: false`. `install_kubevirt.sh` always writes
`true` (it assumes the operator), so patch the CR by hand. Keep the
operator's `nvidia.com/<NAME>` naming:

```yaml
spec:
  configuration:
    permittedHostDevices:
      pciHostDevices:
        - pciVendorSelector: "10de:2684"
          resourceName: nvidia.com/AD102_GEFORCE_RTX_4090
          externalResourceProvider: false
```

### 4.6 Load `vfio_iommu_type1` (both routes)

Binding a device to vfio-pci does not load the IOMMU backend VFIO uses to
map guest memory. The GPU Operator's vfio-manager (route A) runs only
`modprobe vfio-pci`, and on some kernels (Ubuntu's 6.8, for example) that
loads `vfio` but not `vfio_iommu_type1`. qemu then gets a device, a group
and a container, but no way to map guest memory. The VM dies a few
seconds after the launcher starts and goes into `CrashLoopBackOff`. The only
line that explains it is in a `SyncFailed` event on the **VMI** (not the
VM):

```
vfio 0000:bd:00.0: failed to setup container for group 160: No available IOMMU models
```

That message always means no VFIO IOMMU driver is registered. The IOMMU
itself is fine. Load it on every GPU node, and make it persistent:

```bash
lsmod | grep -E '^vfio'            # vfio_pci, vfio and vfio_iommu_type1
sudo modprobe vfio_iommu_type1     # a VM that is retrying succeeds on its next attempt
printf 'vfio_pci\nvfio_iommu_type1\n' | sudo tee /etc/modules-load.d/vfio.conf
```

If `modprobe` reports "Module not found in directory /lib/modules/...",
the kernel package was upgraded under the running kernel and its modules
directory was removed. Only a reboot fixes that.

### 4.7 Checklist for one GPU node

```bash
cat /proc/cmdline                                  # intel_iommu=on / iommu=pt / hugepages=...
ls /sys/kernel/iommu_groups/ | wc -l               # > 0
lspci -nnk -d 10de:                                # every passthrough function: vfio-pci
lsmod | grep -E '^vfio'                            # vfio, vfio_pci, vfio_iommu_type1
ls /dev/vfio/                                      # group files present
grep -i huge /proc/meminfo                         # pool reserved
kubectl get node <node> -o jsonpath='{.status.allocatable}'   # nvidia.com/<NAME> and hugepages-2Mi
```

Then permit the device in KubeVirt (`install_kubevirt.sh --allow-gpu auto`,
or the manual CR patch above for route B).

## 5. The kubelet root must be `/var/lib/kubelet`

virt-handler hostPath-mounts `/var/lib/kubelet` and `/var/lib/kubelet/pods`
and does not check that they are the real kubelet root. On a distribution
that keeps the kubelet elsewhere, virt-handler mounts an empty directory,
reports itself healthy, and can never finish starting a VM: every VMI goes
`Scheduled → Failed` in about three seconds and nothing logs an error. The
Whistler portal shows the VM as `Error` with `CrashLoopBackOff` in the
status message. k3s and kubeadm are unaffected. k0s is affected; the fix is
in section 7.

## 6. Node disk

containerDisk images are cached on the node (several GiB each, and more
for `-cuda` variants). Each guest's root writes go to a copy-on-write
overlay in the virt-launcher pod, which also lives on the node's disk.
Whistler sets no `ephemeral-storage` limit on VMs yet
([rootdisks.md](rootdisks.md)), so give VM nodes room for the image cache
plus the root writes of every guest running at once.

## 7. Distribution notes: k0s and k3s

### k0s

**Kubelet root.** k0s keeps the kubelet under `/var/lib/k0s/kubelet`, which
causes the silent failure in section 5. Until you run a KubeVirt release
that includes [kubevirt#16346](https://github.com/kubevirt/kubevirt/pull/16346)
(merged 2026-09-03; it adds a `--kubelet-root` flag for virt-handler, set
through `KubeVirt.spec.customizeComponents`), bind-mount the real root at
the path virt-handler expects, on every node that runs VMs:

```bash
sudo mkdir -p /var/lib/kubelet
echo '/var/lib/k0s/kubelet /var/lib/kubelet none bind 0 0' | sudo tee -a /etc/fstab
sudo mount /var/lib/kubelet
sudo reboot
```

- **Reboot after mounting.** virt-handler's device plugins
  (`devices.kubevirt.io/kvm`, `tun`, `vhost-net`) register with the kubelet
  over sockets under that root. Restarting virt-handler alone does not
  recover registrations made against the old directory.
- **Don't repoint virt-handler's two hostPaths with `customizeComponents`.**
  With repointed hostPaths, virt-handler marks the node
  `kubevirt.io/schedulable=false`.

**Restarting the kubelet** (for example after changing hugepages) means
restarting the k0s service: `sudo systemctl restart k0sworker` on a worker,
or `k0scontroller` on a controller that also runs workloads
(`--enable-worker`).

### k3s

**Kubelet root.** k3s uses `/var/lib/kubelet`, so section 5 does not apply.

**Restarting the kubelet** means restarting the k3s service:
`sudo systemctl restart k3s` on a server, or `k3s-agent` on an agent.

**containerd and the NVIDIA runtime** (GPU *containers*, not passthrough).
Do *not* let the GPU Operator manage the container toolkit on k3s
(`toolkit.enabled=false`, `cdi.enabled=false`). The toolkit reloads
containerd with SIGHUP. k3s's embedded containerd exits on SIGHUP, k3s
treats that as fatal and restarts the whole server, which reschedules the
toolkit pod, which sends SIGHUP again. The result is a crash loop. Instead,
add the runtime once through k3s's containerd template and restart k3s
once. k3s regenerates `config.toml` from the template on every start:

```toml
# /var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl
{{ template "base" . }}

[plugins."io.containerd.cri.v1.runtime"]
  default_runtime_name = "nvidia"

[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.nvidia]
  runtime_type = "io.containerd.runc.v2"

[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.nvidia.options]
  BinaryName = "/usr/bin/nvidia-container-runtime"
```

`{{ template "base" . }}` must appear exactly once. Appending this block to
an existing template a second time breaks it. With
`toolkit.enabled=false`, `nvidia-operator-validator` may never become
Ready. That does not affect GPU scheduling.

**Other k3s hooks** that `scripts/metal_k3s_create.sh` uses:

- `/etc/rancher/k3s/registries.yaml` configures registry mirrors and
  plain-HTTP endpoints for containerd. It is read at startup, so restart
  k3s after changing it.
- `/var/lib/rancher/k3s/server/manifests/` is applied automatically, and
  re-applied when files change.
- `/usr/local/bin/k3s-uninstall.sh` (`k3s-agent-uninstall.sh` on agents)
  removes k3s, `/etc/rancher`, `/var/lib/rancher` and its iptables/CNI state.
  It does not touch anything from sections 1–4 (kernel arguments, modprobe
  and modules-load files, initramfs, hugepage reservations, drivers), so
  that preparation survives a cluster rebuild.
