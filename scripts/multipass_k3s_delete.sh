#!/usr/bin/env bash
#
# Tear down the Multipass k3s cluster created by scripts/multipass_k3s_create.sh.
#
# Env knobs:
#   VM_NAME         Multipass VM name            (default: whistler-k3s)
#   KUBECONFIG_OUT  kubeconfig file to remove    (default: ~/.kube/<VM_NAME>.yaml)
set -euo pipefail

VM_NAME="${VM_NAME:-whistler-k3s}"
KUBECONFIG_OUT="${KUBECONFIG_OUT:-$HOME/.kube/${VM_NAME}.yaml}"

command -v multipass >/dev/null || { echo "ERROR: multipass not found"; exit 1; }

if multipass info "$VM_NAME" >/dev/null 2>&1; then
  echo "==> Deleting Multipass VM '$VM_NAME'"
  multipass delete --purge "$VM_NAME"
else
  echo "==> Multipass VM '$VM_NAME' does not exist, nothing to delete"
fi

if [[ -f "$KUBECONFIG_OUT" ]]; then
  echo "==> Removing kubeconfig $KUBECONFIG_OUT"
  rm -f "$KUBECONFIG_OUT"
fi
