#!/usr/bin/env bash
#
# Tear down the on-metal k3s installed by scripts/metal_k3s_create.sh.
#
# Runs k3s's own uninstall script, which stops all containers, unwinds
# iptables/CNI state, and removes the binary, systemd unit, /etc/rancher
# and /var/lib/rancher. KubeVirt lives in the cluster, so it is removed
# with it. Host-level prerequisites (GPU drivers, nvidia-container-toolkit,
# IOMMU settings, libvirt-clients, ~/.local/bin/virtctl) are untouched.
#
# Env knobs:
#   CLUSTER_NAME    context name used at create     (default: k3s-metal)
#   KUBECONFIG_OUT  kubeconfig file to remove       (default: ~/.kube/<CLUSTER_NAME>.yaml)
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-k3s-metal}"
KUBECONFIG_OUT="${KUBECONFIG_OUT:-$HOME/.kube/${CLUSTER_NAME}.yaml}"

if [[ -x /usr/local/bin/k3s-uninstall.sh ]]; then
  echo "==> Uninstalling k3s (runs k3s-killall.sh + k3s-uninstall.sh)"
  sudo /usr/local/bin/k3s-uninstall.sh
else
  echo "==> k3s does not appear to be installed (no /usr/local/bin/k3s-uninstall.sh), nothing to uninstall"
fi

if [[ -f "$KUBECONFIG_OUT" ]]; then
  echo "==> Removing kubeconfig $KUBECONFIG_OUT"
  rm -f "$KUBECONFIG_OUT"
fi
