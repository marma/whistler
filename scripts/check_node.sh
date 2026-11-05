#!/usr/bin/env bash
#
# Check whether THIS machine is ready to run KubeVirt VMs for Whistler. Run it
# on the node itself; it reads /proc, /sys and the kernel log and changes
# nothing. design/nodes.md explains every check and how to fix it.
#
# What it checks:
#   - hardware virtualization: vmx/svm, /dev/kvm, nested virtualization when
#     the node is itself a VM
#   - hugepages: every pool size, per NUMA node, how it is reserved (kernel
#     command line or runtime) and whether that survives a reboot; and, with
#     kubectl, whether the kubelet advertises the pool (it only notices a pool
#     changed at runtime after a restart)
#   - the kubelet root: virt-handler only works with /var/lib/kubelet (k0s
#     needs a bind mount)
#   - IOMMU (Intel VT-d / AMD-Vi): groups, kernel arguments, interrupt
#     remapping
#   - every NVIDIA GPU: its driver, i.e. whether it is in passthrough mode
#     (vfio-pci) or used by the host; for passthrough GPUs, that the whole
#     IOMMU group is on vfio-pci, /dev/vfio/<group> exists and vfio_iommu_type1
#     is loaded and loaded again at boot
#   - with kubectl: KubeVirt's schedulable label, devices.kubevirt.io/kvm, the
#     GPU Operator's workload label and the GPU resources the node advertises
#   - free disk for containerDisk images and guest root writes
#
# When the kernel command line needs changing (IOMMU or hugepage arguments
# missing, harmful ones present), it ends with the complete new line to paste
# into /etc/default/grub, or the grubby command on RHEL-family systems.
#
# Root is not required. Without it the kernel log may be unreadable, and the
# interrupt-remapping and BAR checks are skipped (reported as such).
#
# Exit status: 0 when nothing FAILed (WARNs allowed), 1 otherwise, 2 on bad
# arguments.
#
# Options:
#   --page-size <2Mi|1Gi|none>  the hugepage size VMs will request, i.e. the
#                         chart's whistler.hugePages.pageSize (default: 2Mi;
#                         none = guests on ordinary pages, pool not required)
#   --pool-gb <GiB>       hugepage pool to put on the suggested kernel command
#                         line (default: the pool reserved now, if any)
#   --node <name>         this node's Kubernetes name (default: hostname)
#   --no-kubectl          skip the checks that ask the API server
#
# Env knobs:
#   MIN_FREE_GB           free disk wanted under the container image store
#                         (default: 50)
set -uo pipefail   # no -e: a check that fails must report, not abort the run

usage() {
  sed -n '3,/^set -uo pipefail/{/^set -uo pipefail/d;s/^# \{0,1\}//;p;}' "$0"
}

PAGE_SIZE=2Mi
NODE=""
POOL_GB=""
USE_KUBECTL=1
while (( $# )); do
  case "$1" in
    --page-size) [[ $# -ge 2 ]] || { echo "ERROR: --page-size needs a value" >&2; exit 2; }
                 PAGE_SIZE="$2"; shift 2 ;;
    --page-size=*) PAGE_SIZE="${1#*=}"; shift ;;
    --pool-gb) [[ $# -ge 2 ]] || { echo "ERROR: --pool-gb needs a value" >&2; exit 2; }
               POOL_GB="$2"; shift 2 ;;
    --pool-gb=*) POOL_GB="${1#*=}"; shift ;;
    --node) [[ $# -ge 2 ]] || { echo "ERROR: --node needs a value" >&2; exit 2; }
            NODE="$2"; shift 2 ;;
    --node=*) NODE="${1#*=}"; shift ;;
    --no-kubectl) USE_KUBECTL=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERROR: unknown argument '$1' (see --help)" >&2; exit 2 ;;
  esac
done
case "$PAGE_SIZE" in
  2Mi|2M)  PAGE_SIZE=2Mi; WANT_KB=2048 ;;
  1Gi|1G)  PAGE_SIZE=1Gi; WANT_KB=1048576 ;;
  none|"") PAGE_SIZE=none; WANT_KB="" ;;
  *) echo "ERROR: --page-size must be 2Mi, 1Gi or none" >&2; exit 2 ;;
esac
[[ -z "$POOL_GB" || "$POOL_GB" =~ ^[1-9][0-9]*$ ]] \
  || { echo "ERROR: --pool-gb must be a whole number of GiB" >&2; exit 2; }
NODE="${NODE:-$(hostname | tr '[:upper:]' '[:lower:]')}"
MIN_FREE_GB="${MIN_FREE_GB:-50}"

# --- output --------------------------------------------------------------------

if [[ -t 1 ]]; then
  C_OK=$'\e[32m' C_WARN=$'\e[33m' C_FAIL=$'\e[31m' C_BOLD=$'\e[1m' C_OFF=$'\e[0m'
else
  C_OK="" C_WARN="" C_FAIL="" C_BOLD="" C_OFF=""
fi
FAILS=0
WARNS=0
section() { printf '\n%s== %s ==%s\n' "$C_BOLD" "$1" "$C_OFF"; }
pass()    { printf '  %sOK%s    %s\n' "$C_OK" "$C_OFF" "$1"; }
warn()    { printf '  %sWARN%s  %s\n' "$C_WARN" "$C_OFF" "$1"; WARNS=$(( WARNS + 1 )); }
fail()    { printf '  %sFAIL%s  %s\n' "$C_FAIL" "$C_OFF" "$1"; FAILS=$(( FAILS + 1 )); }
info()    { printf '        %s\n' "$1"; }

# --- helpers -------------------------------------------------------------------

KREL="$(uname -r)"
CMDLINE="$(cat /proc/cmdline)"

# A word on the kernel command line, matched whole ("iommu=pt" must not match
# "amd_iommu=pt").
has_arg() { [[ " $CMDLINE " == *" $1 "* ]]; }
arg_value() { # value of key=... on the command line, last one wins
  local w v=""
  for w in $CMDLINE; do [[ "$w" == "$1="* ]] && v="${w#*=}"; done
  printf '%s' "$v"
}

# Loaded, or built into the kernel. /proc/modules spells names with "_".
mod_present() {
  grep -q "^$1 " /proc/modules 2>/dev/null && return 0
  grep -qE "/${1//_/[-_]}\.ko" "/lib/modules/${KREL}/modules.builtin" 2>/dev/null
}

# Listed somewhere that loads it at boot (modules-load.d, /etc/modules, the
# initramfs module lists).
mod_persisted() {
  local pat="^[[:space:]]*${1//_/[-_]}([[:space:]]|$)"
  grep -rqsE "$pat" /etc/modules-load.d /usr/lib/modules-load.d /lib/modules-load.d \
    /etc/modules /etc/initramfs-tools/modules && return 0
  grep -rqsE "${1//_/[-_]}" /etc/dracut.conf.d
}

pci_driver() { # driver bound to a PCI function, or "none"
  local l
  l="$(readlink -f "$1/driver" 2>/dev/null)" && [[ -n "$l" ]] && { basename "$l"; return; }
  echo none
}

pci_name() { # human-readable name when lspci is there, ids otherwise
  local addr="$1" dev="/sys/bus/pci/devices/$1"
  if command -v lspci >/dev/null 2>&1; then
    lspci -nn -s "$addr" 2>/dev/null | sed 's/^[^ ]* //'
  else
    echo "$(cat "$dev/vendor"):$(cat "$dev/device") class $(cat "$dev/class")"
  fi
}

human_kb() { # 2048 -> 2Mi, 1048576 -> 1Gi
  local kb="$1"
  if (( kb >= 1048576 )); then echo "$(( kb / 1048576 ))Gi"
  elif (( kb >= 1024 )); then echo "$(( kb / 1024 ))Mi"
  else echo "${kb}Ki"; fi
}

qty_to_bytes() { # Kubernetes quantity ("32Gi", "0", "1073741824") -> bytes
  awk -v q="$1" 'BEGIN {
    n = q + 0; s = q; sub(/^[0-9.]+/, "", s)
    m["Ki"]=2^10; m["Mi"]=2^20; m["Gi"]=2^30; m["Ti"]=2^40
    m["k"]=1e3; m["M"]=1e6; m["G"]=1e9; m["T"]=1e12
    printf "%.0f\n", (s in m) ? n * m[s] : n }'
}

# The kernel log: dmesg (root, or dmesg_restrict=0), else journald (the adm /
# systemd-journal groups). Empty when neither is readable.
KLOG="$(dmesg 2>/dev/null || journalctl -k -b --no-pager -q 2>/dev/null || true)"

# kubectl, in whichever form this node has. k3s and k0s both ship their own.
KC=()
if (( USE_KUBECTL )); then
  if command -v kubectl >/dev/null 2>&1 && kubectl get node "$NODE" >/dev/null 2>&1; then
    KC=(kubectl)
  elif command -v k3s >/dev/null 2>&1 && k3s kubectl get node "$NODE" >/dev/null 2>&1; then
    KC=(k3s kubectl)
  elif command -v k0s >/dev/null 2>&1 && k0s kubectl get node "$NODE" >/dev/null 2>&1; then
    KC=(k0s kubectl)
  fi
fi
kc_node() { "${KC[@]}" get node "$NODE" -o jsonpath="$1" 2>/dev/null; }

# Which distribution runs the kubelet here, for the restart hint.
DISTRO=unknown
RESTART_CMD="restart the kubelet"
for unit in k3s k3s-agent k0sworker k0scontroller kubelet; do
  if systemctl is-active --quiet "$unit" 2>/dev/null; then
    DISTRO="$unit"; RESTART_CMD="sudo systemctl restart $unit"; break
  fi
done

CPU_VENDOR="$(awk -F': ' '/^vendor_id/ {print $2; exit}' /proc/cpuinfo)"
case "$CPU_VENDOR" in
  GenuineIntel) CPU=intel ;;
  AuthenticAMD) CPU=amd ;;
  *)            CPU=other ;;
esac

printf '%sNode readiness for KubeVirt VMs%s: %s (kernel %s, %s CPU, kubelet: %s, kubectl: %s)\n' \
  "$C_BOLD" "$C_OFF" "$NODE" "$KREL" "$CPU_VENDOR" "$DISTRO" "${KC[*]:-not used}"

# --- kernel --------------------------------------------------------------------

section "Kernel"
info "cmdline: $CMDLINE"
if [[ -d "/lib/modules/${KREL}/kernel" ]]; then
  pass "module directory for the running kernel exists"
else
  fail "/lib/modules/${KREL} is missing: the kernel was upgraded under the running one"
  info "modprobe fails for anything not yet loaded (vfio_iommu_type1 included); reboot"
fi

# --- virtualization ------------------------------------------------------------

section "Hardware virtualization"
IN_VM=""
command -v systemd-detect-virt >/dev/null 2>&1 && IN_VM="$(systemd-detect-virt --vm 2>/dev/null | grep -v '^none$')"
[[ -n "$IN_VM" ]] && info "this node is itself a VM (${IN_VM})"

if grep -qE '^flags.*\b(vmx|svm)\b' /proc/cpuinfo; then
  pass "CPU exposes $(grep -oE '\b(vmx|svm)\b' /proc/cpuinfo | head -1) (VT-x / AMD-V)"
else
  fail "no vmx/svm CPU flag: virtualization is off in the firmware${IN_VM:+, or nested virtualization is off on the outer host}"
fi

if [[ -c /dev/kvm ]]; then
  pass "/dev/kvm present"
else
  kvm_mod=kvm_intel; [[ "$CPU" == amd ]] && kvm_mod=kvm_amd
  fail "/dev/kvm missing (try: sudo modprobe ${kvm_mod}; persist in /etc/modules-load.d/kvm.conf)"
  info "without it VMs only run with KubeVirt useEmulation, which is slow"
fi
for p in /sys/module/kvm_intel/parameters/nested /sys/module/kvm_amd/parameters/nested; do
  [[ -r "$p" ]] && info "nested virtualization for guests of this node: $(cat "$p")"
done

# --- hugepages -----------------------------------------------------------------

section "Hugepages"
MEM_KB="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
declare -A HP_TOTAL=()
reserved_kb=0
for d in /sys/kernel/mm/hugepages/hugepages-*kB; do
  [[ -d "$d" ]] || continue
  kb="${d##*-}"; kb="${kb%kB}"
  total="$(cat "$d/nr_hugepages")"; free="$(cat "$d/free_hugepages")"
  HP_TOTAL[$kb]="$total"
  size="$(human_kb "$kb")"
  if (( total > 0 )); then
    reserved_kb=$(( reserved_kb + total * kb ))
    info "${size} pages: ${total} reserved = $(( total * kb / 1048576 )) GiB, ${free} free, $(( total - free )) in use"
    per_node=""
    for n in /sys/devices/system/node/node*; do
      f="$n/hugepages/hugepages-${kb}kB/nr_hugepages"
      [[ -r "$f" ]] && per_node+=" ${n##*/}=$(cat "$f")"
    done
    [[ -n "$per_node" ]] && info "    per NUMA node:${per_node}"
  else
    info "${size} pages: none reserved"
  fi
done
if (( reserved_kb > 0 && MEM_KB > 0 )); then
  pct=$(( reserved_kb * 100 / MEM_KB ))
  info "pool total: $(( reserved_kb / 1048576 )) GiB of $(( MEM_KB / 1048576 )) GiB RAM (${pct}%) is no longer ordinary memory"
  (( pct > 90 )) && warn "over 90% of RAM is in hugepages: little is left for the OS, pods and per-VM overhead"
fi
if grep -q '\bpdpe1gb\b' /proc/cpuinfo; then
  info "CPU supports 1Gi pages (pdpe1gb)"
else
  info "CPU does not support 1Gi pages (no pdpe1gb)"
fi

HP_AT_BOOT=0
if [[ -z "$WANT_KB" ]]; then
  pass "--page-size none: guests use ordinary pages, no pool required"
else
  want_total="${HP_TOTAL[$WANT_KB]:-0}"
  if (( want_total > 0 )); then
    pass "${PAGE_SIZE} pool reserved (${want_total} pages)"
  else
    fail "no ${PAGE_SIZE} hugepages reserved: every VM requesting them pends on 'Insufficient hugepages-${PAGE_SIZE}'"
    if [[ "$WANT_KB" == 2048 ]]; then
      info "boot:    add 'hugepagesz=2M hugepages=<count>' to the kernel command line (count = GiB x 512)"
      info "runtime: echo <count> | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages; ${RESTART_CMD}"
    else
      info "boot:    add 'default_hugepagesz=1G hugepagesz=1G hugepages=<GiB>' to the kernel command line"
    fi
    for kb in "${!HP_TOTAL[@]}"; do
      (( HP_TOTAL[$kb] > 0 )) && info "a $(human_kb "$kb") pool exists: use --page-size $(human_kb "$kb") / whistler.hugePages.pageSize=$(human_kb "$kb")"
    done
  fi

  # How the pool is reserved, and whether it comes back after a reboot.
  if [[ "$CMDLINE" == *hugepages=* ]]; then
    HP_AT_BOOT=1
    pass "pool reserved on the kernel command line (survives reboots)"
  elif grep -rqsE '^[[:space:]]*vm\.nr_hugepages[[:space:]]*=' /etc/sysctl.conf /etc/sysctl.d /usr/lib/sysctl.d \
       || grep -rqs 'nr_hugepages' /etc/tmpfiles.d; then
    HP_AT_BOOT=1
    pass "pool reserved at boot by sysctl.d / tmpfiles.d"
  elif (( want_total > 0 )); then
    warn "pool was reserved at runtime only: it is gone after a reboot"
    info "persist it on the kernel command line (design/nodes.md, section 3)"
  fi

  # What the scheduler sees. kubelet reads the pools once, at startup.
  if (( ${#KC[@]} )); then
    cap="$(kc_node "{.status.capacity.hugepages-${PAGE_SIZE}}")"
    alloc="$(kc_node "{.status.allocatable.hugepages-${PAGE_SIZE}}")"
    kernel_bytes=$(( want_total * WANT_KB * 1024 ))
    cap_bytes="$(qty_to_bytes "${cap:-0}")"
    info "kubelet advertises hugepages-${PAGE_SIZE}: capacity ${cap:-0}, allocatable ${alloc:-0}"
    if (( kernel_bytes > 0 && cap_bytes == 0 )); then
      fail "kernel has the pool but kubelet advertises none: kubelet has not been restarted since it was reserved"
      info "${RESTART_CMD}"
    elif (( kernel_bytes != cap_bytes )); then
      warn "kernel pool ($(( kernel_bytes / 1073741824 )) GiB) and kubelet capacity (${cap}) differ: ${RESTART_CMD}"
    elif (( kernel_bytes > 0 )); then
      pass "kubelet advertises the whole pool"
    fi
  fi
fi

# --- kubelet root --------------------------------------------------------------

section "Kubelet root"
if [[ -S /var/lib/kubelet/device-plugins/kubelet.sock ]]; then
  pass "kubelet runs at /var/lib/kubelet, where virt-handler expects it"
elif [[ -S /var/lib/k0s/kubelet/device-plugins/kubelet.sock ]]; then
  fail "kubelet root is /var/lib/k0s/kubelet (k0s) and /var/lib/kubelet is not a bind mount of it"
  info "every VMI fails within seconds with nothing logged; bind-mount it and reboot:"
  info "  echo '/var/lib/k0s/kubelet /var/lib/kubelet none bind 0 0' | sudo tee -a /etc/fstab"
  info "  sudo mkdir -p /var/lib/kubelet && sudo mount /var/lib/kubelet && sudo reboot"
else
  warn "no kubelet device-plugin socket found under /var/lib/kubelet (no kubelet on this machine yet, or a non-standard root)"
fi
if [[ -d /var/lib/k0s/kubelet && -d /var/lib/kubelet ]] \
   && [[ "$(stat -c %d:%i /var/lib/k0s/kubelet)" == "$(stat -c %d:%i /var/lib/kubelet)" ]]; then
  info "/var/lib/kubelet is bind-mounted from /var/lib/k0s/kubelet"
fi

# --- GPUs (scanned before the IOMMU section, which depends on them) ------------

# NVIDIA only, like the rest of Whistler's passthrough tooling (the GPU
# Operator, install_kubevirt.sh --allow-gpu). Other display controllers (an
# iGPU, a BMC's VGA) are listed but not checked.
GPUS=()
OTHER_DISPLAY=()
for dev in /sys/bus/pci/devices/*; do
  [[ "$(cat "$dev/class")" == 0x03* ]] || continue        # display controllers
  if [[ "$(cat "$dev/vendor")" == 0x10de ]]; then GPUS+=("${dev##*/}")
  else OTHER_DISPLAY+=("${dev##*/}"); fi
done

# --- IOMMU ---------------------------------------------------------------------

section "IOMMU ($( [[ "$CPU" == amd ]] && echo AMD-Vi || echo 'Intel VT-d' ))"
need_iommu=$(( ${#GPUS[@]} > 0 ))
iommu_bad() { if (( need_iommu )); then fail "$1"; else info "$1 (only matters for GPU passthrough)"; fi; }

n_groups="$(find /sys/kernel/iommu_groups -mindepth 1 -maxdepth 1 2>/dev/null | wc -l)"
if (( n_groups > 0 )); then
  pass "IOMMU active: ${n_groups} groups, units: $(ls /sys/class/iommu 2>/dev/null | tr '\n' ' ')"
else
  iommu_bad "IOMMU not active (no IOMMU groups)"
  if [[ "$CPU" == intel ]]; then
    info "enable VT-d in the firmware and add 'intel_iommu=on iommu=pt' to the kernel command line"
  else
    info "enable IOMMU / AMD-Vi in the firmware (Enabled, not Auto); no kernel argument is needed on AMD"
  fi
fi

[[ "$(arg_value intel_iommu)" == off ]] && iommu_bad "intel_iommu=off on the kernel command line disables the IOMMU"
[[ "$(arg_value amd_iommu)" == off ]] && iommu_bad "amd_iommu=off on the kernel command line disables the IOMMU"
[[ "$(arg_value iommu)" == off ]] && iommu_bad "iommu=off on the kernel command line disables the IOMMU"
if [[ "$CPU" == intel ]] && ! has_arg intel_iommu=on && (( n_groups == 0 )); then
  info "intel_iommu=on is not set; many kernels build VT-d default-off"
fi
has_arg amd_iommu=on && warn "amd_iommu=on is not a valid option and is ignored; remove it"
[[ "$(arg_value amd_iommu)" == *force_isolation* ]] \
  && warn "amd_iommu=force_isolation slows DMA on every device and does nothing for VFIO; remove it"
has_arg iommu=pt || info "iommu=pt not set (optional: host-driven devices skip translation)"

if [[ -z "$KLOG" ]]; then
  info "kernel log not readable: interrupt remapping not checked (run as root)"
elif grep -qiE 'DMAR-IR: Enabled IRQ remapping|AMD-Vi: Interrupt remapping enabled|IRQ remapping (was )?enabled' <<<"$KLOG"; then
  pass "interrupt remapping enabled"
elif (( n_groups > 0 )); then
  iommu_bad "interrupt remapping not reported as enabled: VFIO refuses to assign devices without it"
  info "check the firmware's x2APIC / interrupt remapping setting"
fi

# --- GPUs ----------------------------------------------------------------------

section "GPUs"
PT_OK=0
PT_TOTAL=0
WL_LABEL=""
(( ${#KC[@]} )) && WL_LABEL="$(kc_node '{.metadata.labels.nvidia\.com/gpu\.workload\.config}')"

if (( ${#GPUS[@]} == 0 )); then
  pass "no NVIDIA GPU on this node; passthrough checks skipped"
else
  for addr in "${GPUS[@]}"; do
    dev="/sys/bus/pci/devices/$addr"
    drv="$(pci_driver "$dev")"
    grp=""; [[ -e "$dev/iommu_group" ]] && grp="$(basename "$(readlink -f "$dev/iommu_group")")"
    printf '\n  %s%s%s  %s\n' "$C_BOLD" "$addr" "$C_OFF" "$(pci_name "$addr")"
    numa="$(cat "$dev/numa_node" 2>/dev/null)"; [[ -z "$numa" || "$numa" == -1 ]] && numa=none
    info "id $(sed 's/0x//' "$dev/vendor"):$(sed 's/0x//' "$dev/device"), driver ${drv}, IOMMU group ${grp:-none}, NUMA node ${numa}"

    # Every non-bridge function in the group, and whether it belongs to this card.
    slot="${addr%.*}"
    group_ok=1; foreign=""
    if [[ -n "$grp" ]]; then
      for m in /sys/kernel/iommu_groups/"$grp"/devices/*; do
        maddr="${m##*/}"; [[ "$maddr" == "$addr" ]] && continue
        [[ "$(cat "$m/class")" == 0x0604* ]] && continue      # PCI bridges stay on pcieport
        mdrv="$(pci_driver "$m")"
        info "    group member ${maddr} (driver ${mdrv}): $(pci_name "$maddr")"
        [[ "${maddr%.*}" == "$slot" ]] || foreign+=" ${maddr}"
        [[ "$drv" == vfio-pci && "$mdrv" != vfio-pci ]] && group_ok=0
      done
    fi

    case "$drv" in
      vfio-pci)
        PT_TOTAL=$(( PT_TOTAL + 1 ))
        gpu_ok=1
        pass "passthrough mode (bound to vfio-pci)"
        if (( group_ok )); then
          pass "every function in IOMMU group ${grp} is on vfio-pci"
        else
          fail "IOMMU group ${grp} has members not on vfio-pci: the GPU cannot be assigned until they are"; gpu_ok=0
        fi
        [[ -n "$foreign" ]] && warn "group ${grp} also holds devices of another slot:${foreign} (they go to the VM with it)"
        if [[ -c "/dev/vfio/${grp}" ]]; then
          pass "/dev/vfio/${grp} present"
        else
          fail "/dev/vfio/${grp} missing"; gpu_ok=0
        fi
        (( gpu_ok )) && PT_OK=$(( PT_OK + 1 ))
        ;;
      nvidia)
        info "host mode: used by the host driver (GPU containers, not VMs)"
        if [[ "$WL_LABEL" == vm-passthrough ]]; then
          warn "node is labelled nvidia.com/gpu.workload.config=vm-passthrough but the card is still on ${drv}"
          info "the GPU Operator's vfio-manager has not bound it; something on the host may hold it open"
          systemctl is-active --quiet nvidia-persistenced 2>/dev/null \
            && info "nvidia-persistenced is running and holds the GPUs open: sudo systemctl disable --now nvidia-persistenced"
        fi
        ;;
      nouveau)
        warn "bound to nouveau: blacklist it (blacklist nouveau / options nouveau modeset=0 in /etc/modprobe.d) and rebuild the initramfs"
        ;;
      none)
        warn "no driver bound: neither the host nor VMs can use it"
        info "for passthrough: options vfio-pci ids=... in /etc/modprobe.d, or label the node for the GPU Operator"
        ;;
      *)
        warn "bound to ${drv}"
        ;;
    esac
    if [[ "$drv" != vfio-pci && -n "$foreign" ]]; then
      warn "IOMMU group ${grp} is shared with another slot:${foreign}; this card cannot be passed through alone"
    fi
  done

  # Modules and configuration that matter for any passthrough on this node.
  printf '\n'
  if (( PT_TOTAL > 0 )) || [[ "$WL_LABEL" == vm-passthrough ]]; then
    for m in vfio vfio_pci vfio_iommu_type1; do
      if mod_present "$m"; then
        pass "module ${m} loaded"
      else
        fail "module ${m} not loaded (sudo modprobe ${m})"
        [[ "$m" == vfio_iommu_type1 ]] && info "without it every GPU VM fails with 'No available IOMMU models'"
      fi
    done
    if mod_persisted vfio_iommu_type1; then
      pass "vfio_iommu_type1 is loaded at boot (modules-load.d / initramfs)"
    else
      warn "vfio_iommu_type1 is not configured to load at boot"
      info "printf 'vfio_pci\\nvfio_iommu_type1\\n' | sudo tee /etc/modules-load.d/vfio.conf"
    fi
  fi
  if grep -q '^nouveau ' /proc/modules; then
    warn "nouveau is loaded; blacklist it on GPU nodes"
  fi
  vfio_ids="$(grep -rhsE '^[[:space:]]*options[[:space:]]+vfio[-_]pci' /etc/modprobe.d | sed 's/^[[:space:]]*//')"
  [[ -n "$vfio_ids" ]] && info "static vfio-pci binding: ${vfio_ids}"
  [[ -n "$(arg_value vfio-pci.ids)" ]] && info "static vfio-pci binding on the kernel command line: vfio-pci.ids=$(arg_value vfio-pci.ids)"
  [[ -n "$WL_LABEL" ]] && info "GPU Operator workload label: nvidia.com/gpu.workload.config=${WL_LABEL}"

  if [[ -n "$KLOG" ]] && grep -qE 'BAR [0-9]+: (no space|failed to assign)' <<<"$KLOG"; then
    warn "the kernel could not assign some PCI BARs: enable 'Above 4G decoding' in the firmware"
  fi
fi

for addr in ${OTHER_DISPLAY+"${OTHER_DISPLAY[@]}"}; do
  info "other display controller, not checked: ${addr} $(pci_name "$addr") (driver $(pci_driver "/sys/bus/pci/devices/$addr"))"
done

# --- cluster view --------------------------------------------------------------

if (( ${#KC[@]} )); then
  section "Cluster view of ${NODE}"
  sched="$(kc_node '{.metadata.labels.kubevirt\.io/schedulable}')"
  case "$sched" in
    true)  pass "kubevirt.io/schedulable=true" ;;
    false) fail "kubevirt.io/schedulable=false: virt-handler considers this node unusable (kubectl -n kubevirt logs on its virt-handler pod)" ;;
    *)     warn "no kubevirt.io/schedulable label: KubeVirt is not installed, or virt-handler does not run here" ;;
  esac
  kvm_alloc="$(kc_node '{.status.allocatable.devices\.kubevirt\.io/kvm}')"
  if [[ -n "$kvm_alloc" && "$kvm_alloc" != 0 ]]; then
    pass "devices.kubevirt.io/kvm advertised (${kvm_alloc})"
  elif [[ -n "$sched" ]]; then
    fail "devices.kubevirt.io/kvm not advertised"
  fi
  gpu_res="$(kc_node '{.status.allocatable}' | grep -oE '"(nvidia\.com|amd\.com)/[^"]+":"[^"]+"' | tr -d '"' | tr '\n' ' ')"
  if [[ -n "$gpu_res" ]]; then
    info "GPU resources advertised: ${gpu_res}"
  elif (( PT_TOTAL > 0 )); then
    warn "GPUs are on vfio-pci but the node advertises no GPU resource"
    info "the GPU Operator needs sandboxWorkloads.enabled=true; without it, permit the device in the KubeVirt CR with externalResourceProvider: false"
  fi
fi

# --- disk ----------------------------------------------------------------------

section "Disk"
store=/var/lib/containerd
[[ -d /var/lib/rancher/k3s/agent/containerd ]] && store=/var/lib/rancher/k3s/agent/containerd
[[ -d /var/lib/k0s/containerd ]] && store=/var/lib/k0s/containerd
[[ -d "$store" ]] || store=/var/lib
free_gb="$(df -P -BG "$store" 2>/dev/null | awk 'NR==2 {gsub("G","",$4); print $4}')"
if [[ -n "$free_gb" ]]; then
  if (( free_gb >= MIN_FREE_GB )); then
    pass "${free_gb} GiB free under ${store}"
  else
    warn "only ${free_gb} GiB free under ${store} (want ${MIN_FREE_GB}): containerDisk images and guest root writes live here"
  fi
fi

# --- kernel command line -------------------------------------------------------

# Everything above that belongs on the kernel command line, merged into the
# node's existing line so it can be pasted as is. Existing arguments keep their
# order; new ones go at the end.
CL_ADD=()
CL_DEL=()
CL_NOTE=""
if (( need_iommu )); then
  if [[ "$CPU" == intel ]] && ! has_arg intel_iommu=on; then CL_ADD+=(intel_iommu=on); fi
  has_arg iommu=pt || CL_ADD+=(iommu=pt)
  CL_DEL+=(intel_iommu=off amd_iommu=off iommu=off)
fi
CL_DEL+=(amd_iommu=on amd_iommu=force_isolation)
if [[ -n "$WANT_KB" ]] && (( ! HP_AT_BOOT )); then
  if [[ -n "$POOL_GB" ]]; then
    count=$(( WANT_KB == 2048 ? POOL_GB * 512 : POOL_GB ))
  elif (( ${HP_TOTAL[$WANT_KB]:-0} > 0 )); then
    count="${HP_TOTAL[$WANT_KB]}"
    CL_NOTE="hugepages=${count} is the pool reserved now; rerun with --pool-gb <GiB> to size it differently"
  else
    count="<count>"
    CL_NOTE="replace <count> with the pool size (design/nodes.md, section 3), or rerun with --pool-gb <GiB>"
  fi
  if [[ "$WANT_KB" == 2048 ]]; then
    CL_ADD+=(hugepagesz=2M "hugepages=${count}")
  else
    CL_ADD+=(default_hugepagesz=1G hugepagesz=1G "hugepages=${count}")
  fi
fi

# $1 with the CL_DEL words removed and, when $2 is set, the CL_ADD words it
# lacks appended.
cl_merge() {
  local w d out=() keep
  for w in $1; do
    keep=1
    for d in "${CL_DEL[@]}"; do [[ "$w" == "$d" ]] && keep=0; done
    (( keep )) && out+=("$w")
  done
  if [[ -n "$2" ]]; then
    for w in ${CL_ADD+"${CL_ADD[@]}"}; do
      [[ " ${out[*]-} " == *" $w "* ]] || out+=("$w")
    done
  fi
  printf '%s' "${out[*]-}"
}
grub_var() { ( set +u; . /etc/default/grub >/dev/null 2>&1; printf '%s' "${!1-}" ); }

cl_changes=""
for w in ${CL_ADD+"${CL_ADD[@]}"}; do cl_changes+=" +$w"; done
for w in "${CL_DEL[@]}"; do has_arg "$w" && cl_changes+=" -$w"; done

section "Kernel command line"
if [[ -z "$cl_changes" ]]; then
  pass "nothing to add or remove"
else
  info "changes:${cl_changes}"
  if command -v grubby >/dev/null 2>&1; then
    del=()
    for w in "${CL_DEL[@]}"; do has_arg "$w" && del+=("$w"); done
    cmd="sudo grubby --update-kernel=ALL"
    (( ${#CL_ADD[@]} )) && cmd+=" --args=\"${CL_ADD[*]}\""
    (( ${#del[@]} )) && cmd+=" --remove-args=\"${del[*]}\""
    info "run, then reboot:"
    printf '\n%s\n\n' "$cmd"
  elif [[ -r /etc/default/grub ]]; then
    info "in /etc/default/grub, replace the line(s) with these, then: sudo update-grub && sudo reboot"
    printf '\n'
    cur="$(grub_var GRUB_CMDLINE_LINUX_DEFAULT)"; new="$(cl_merge "$cur" add)"
    printf 'GRUB_CMDLINE_LINUX_DEFAULT="%s"\n' "$new"
    cur="$(grub_var GRUB_CMDLINE_LINUX)"; new="$(cl_merge "$cur" "")"
    [[ "$cur" != "$new" ]] && printf 'GRUB_CMDLINE_LINUX="%s"\n' "$new"
    printf '\n'
    for f in $(grep -lsE '^[[:space:]]*GRUB_CMDLINE_LINUX' /etc/default/grub.d/*.cfg); do
      info "${f} also sets the command line and is read after /etc/default/grub: check it too"
    done
  else
    info "no /etc/default/grub or grubby here: change the arguments in this node's boot loader"
  fi
  [[ -n "$CL_NOTE" ]] && info "$CL_NOTE"
fi

# --- summary -------------------------------------------------------------------

section "Summary"
if (( FAILS == 0 )); then
  printf '  %sREADY%s for VM workloads (%d warning(s))\n' "$C_OK" "$C_OFF" "$WARNS"
else
  printf '  %sNOT READY%s: %d failure(s), %d warning(s)\n' "$C_FAIL" "$C_OFF" "$FAILS" "$WARNS"
fi
if (( ${#GPUS[@]} > 0 )); then
  info "GPUs: ${#GPUS[@]} found, ${PT_TOTAL} in passthrough mode, ${PT_OK} ready for VMs"
fi
info "fixes for each item: design/nodes.md"
(( FAILS == 0 ))
