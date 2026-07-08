#!/usr/bin/env bash
#
# Install k3s + KubeVirt directly on this machine (bare metal) — the path
# for real GPU work: with only one virtualization layer, KubeVirt can take
# the GPU via VFIO passthrough and containers can use the NVIDIA runtime.
#
# KubeVirt is installed via scripts/install_kubevirt.sh. No libvirt is
# needed on the host — it is bundled in KubeVirt's virt-launcher pods; the
# host only needs /dev/kvm (kvm kernel modules). If /dev/kvm is missing,
# KubeVirt is configured with useEmulation (slow, but functional).
#
# The install is clean to undo: k3s ships k3s-uninstall.sh / k3s-killall.sh
# which remove the binary, systemd unit, /etc/rancher, /var/lib/rancher and
# unwind iptables/CNI state — KubeVirt lives in the cluster, so it goes
# with it. scripts/metal_k3s_delete.sh wraps that.
#
# GPU prerequisites (drivers, nvidia-container-toolkit, IOMMU) are
# host-level and survive cluster rebuilds; this script only reports their
# status, it does not install them.
#
# The kubeconfig is written to KUBECONFIG_OUT (default:
# ~/.kube/<CLUSTER_NAME>.yaml) with cluster/context named CLUSTER_NAME.
#
# Env knobs:
#   CLUSTER_NAME      context name in the kubeconfig   (default: k3s-metal)
#   K3S_CHANNEL       k3s release channel              (default: stable)
#   K3S_EXTRA_ARGS    extra args for the k3s server    (default: empty)
#   KUBEVIRT_VERSION  KubeVirt release tag             (default: latest stable)
#   KUBECONFIG_OUT    where to write the kubeconfig
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-k3s-metal}"
K3S_CHANNEL="${K3S_CHANNEL:-stable}"
K3S_EXTRA_ARGS="${K3S_EXTRA_ARGS:-}"
KUBECONFIG_OUT="${KUBECONFIG_OUT:-$HOME/.kube/${CLUSTER_NAME}.yaml}"

command -v curl >/dev/null || { echo "ERROR: curl not found"; exit 1; }

if [[ -x /usr/local/bin/k3s-uninstall.sh ]] || systemctl is-active --quiet k3s 2>/dev/null; then
  echo "ERROR: k3s is already installed on this host."
  echo "Tear it down first: scripts/metal_k3s_delete.sh"
  exit 1
fi

# --- Report GPU / virtualization readiness (informational, never fatal) ------
echo "==> Host GPU / virtualization status"
if [[ ! -e /dev/kvm ]]; then
  sudo modprobe kvm_intel 2>/dev/null || sudo modprobe kvm_amd 2>/dev/null || true
fi
if [[ -e /dev/kvm ]]; then
  echo "    /dev/kvm: present — KubeVirt can use hardware virtualization"
else
  echo "    /dev/kvm: MISSING — KubeVirt will be configured with useEmulation"
fi
if [[ -d /sys/kernel/iommu_groups ]] && [[ -n "$(ls -A /sys/kernel/iommu_groups 2>/dev/null)" ]]; then
  echo "    IOMMU: enabled — VFIO GPU passthrough to KubeVirt guests is possible"
else
  echo "    IOMMU: not enabled — needed only for GPU passthrough to VMs"
  echo "           (kernel args: intel_iommu=on or amd_iommu=on, plus iommu=pt)"
fi
if command -v nvidia-smi >/dev/null 2>&1; then
  echo "    NVIDIA driver: $(nvidia-smi --query-gpu=driver_version,name --format=csv,noheader 2>/dev/null | head -1 || echo present)"
else
  echo "    NVIDIA driver: not found — GPU-in-container workloads need it"
fi
if command -v nvidia-ctk >/dev/null 2>&1 || command -v nvidia-container-runtime >/dev/null 2>&1; then
  echo "    nvidia-container-toolkit: present — k3s containerd will auto-add the 'nvidia' runtime"
else
  echo "    nvidia-container-toolkit: not found — install it before the cluster"
  echo "           needs GPU containers (k3s picks it up on install/restart)"
fi

echo "==> Installing k3s (channel: ${K3S_CHANNEL})"
# shellcheck disable=SC2086  # K3S_EXTRA_ARGS is intentionally word-split
curl -sfL https://get.k3s.io \
  | INSTALL_K3S_CHANNEL="${K3S_CHANNEL}" sh -s - --write-kubeconfig-mode 644 ${K3S_EXTRA_ARGS}

echo "==> Waiting for the node to become Ready"
for _ in $(seq 1 60); do
  if KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get node 2>/dev/null | grep -q ' Ready'; then break; fi
  sleep 2
done
KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get node | grep -q ' Ready' \
  || { echo "ERROR: node never became Ready"; KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get node || true; exit 1; }

echo "==> Writing kubeconfig to $KUBECONFIG_OUT"
mkdir -p "$(dirname "$KUBECONFIG_OUT")"
sed -e "s|: default$|: ${CLUSTER_NAME}|" /etc/rancher/k3s/k3s.yaml > "$KUBECONFIG_OUT"
chmod 600 "$KUBECONFIG_OUT"

export KUBECONFIG="$KUBECONFIG_OUT"
kubectl get nodes 2>/dev/null || true

# --- KubeVirt ------------------------------------------------------------------
if [[ ! -e /dev/kvm ]]; then
  export KUBEVIRT_USE_EMULATION=1
fi
"$SCRIPT_DIR/install_kubevirt.sh"

cat <<EOF

Cluster '$CLUSTER_NAME' is up on this host, with KubeVirt.

  export KUBECONFIG=$KUBECONFIG_OUT
  kubectl get nodes
  kubectl -n kubevirt get pods
  virtctl console <vm>                    # once a VM is running

  scripts/metal_k3s_delete.sh             # tear everything down
EOF
