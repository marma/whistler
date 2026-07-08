#!/usr/bin/env bash
#
# Install KubeVirt into the cluster of the current KUBECONFIG, and virtctl
# onto this machine (~/.local/bin). Called by multipass_k3s_create.sh and
# metal_k3s_create.sh; usable standalone against any cluster.
#
# No libvirt is needed on the host or nodes: KubeVirt bundles libvirt/qemu
# inside its virt-launcher pods. The nodes just need /dev/kvm — without it,
# set KUBEVIRT_USE_EMULATION=1 (VMs run fully emulated, slow).
#
# Env knobs:
#   KUBEVIRT_VERSION       release tag (default: latest stable)
#   KUBEVIRT_USE_EMULATION set to 1 for nodes without /dev/kvm
set -euo pipefail

KUBEVIRT_VERSION="${KUBEVIRT_VERSION:-}"
if [[ -z "$KUBEVIRT_VERSION" ]]; then
  KUBEVIRT_VERSION="$(curl -sfL https://storage.googleapis.com/kubevirt-prow/release/kubevirt/kubevirt/stable.txt)"
  [[ -n "$KUBEVIRT_VERSION" ]] || { echo "ERROR: could not resolve latest KubeVirt version"; exit 1; }
fi
KUBEVIRT_URL="https://github.com/kubevirt/kubevirt/releases/download/${KUBEVIRT_VERSION}"

echo "==> Installing KubeVirt ${KUBEVIRT_VERSION}"
kubectl apply -f "${KUBEVIRT_URL}/kubevirt-operator.yaml"

# The operator registers the KubeVirt CRD; retry the CR until it exists.
cr_applied=""
for _ in $(seq 1 30); do
  if kubectl apply -f "${KUBEVIRT_URL}/kubevirt-cr.yaml" 2>/dev/null; then cr_applied=1; break; fi
  sleep 2
done
[[ -n "$cr_applied" ]] || { echo "ERROR: could not apply the KubeVirt CR (CRD never registered)"; exit 1; }

if [[ "${KUBEVIRT_USE_EMULATION:-}" == "1" ]]; then
  echo "==> Enabling emulation (no /dev/kvm on the nodes; VMs will be slow)"
  kubectl -n kubevirt patch kubevirt kubevirt --type=merge \
    -p '{"spec":{"configuration":{"developerConfiguration":{"useEmulation":true}}}}'
fi

echo "==> Waiting for KubeVirt to become Available (pulls several images; can take minutes)"
kubectl -n kubevirt wait kubevirt kubevirt --for=condition=Available --timeout=15m

# --- virtctl (client for console/vnc/start/stop) ------------------------------
if ! command -v virtctl >/dev/null 2>&1; then
  case "$(uname -m)" in
    x86_64)  VIRTCTL_ARCH=amd64 ;;
    aarch64) VIRTCTL_ARCH=arm64 ;;
    *)       VIRTCTL_ARCH="" ;;
  esac
  if [[ -n "$VIRTCTL_ARCH" ]]; then
    echo "==> Installing virtctl ${KUBEVIRT_VERSION} to ~/.local/bin"
    mkdir -p "$HOME/.local/bin"
    curl -sfL -o "$HOME/.local/bin/virtctl" \
      "${KUBEVIRT_URL}/virtctl-${KUBEVIRT_VERSION}-linux-${VIRTCTL_ARCH}"
    chmod +x "$HOME/.local/bin/virtctl"
  else
    echo "==> Skipping virtctl (unsupported arch $(uname -m))"
  fi
fi

echo "KubeVirt ${KUBEVIRT_VERSION} is Available."
