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
#   KUBEVIRT_INSTALL_CDI   set to 0 to skip CDI (imageURL boot sources need it)
#   CDI_VERSION            CDI release tag (default: latest)
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

# One combined developerConfiguration patch: two separate merge-patches on
# sibling keys would clobber each other. EnableVirtioFsStorageVolumes allows
# virtiofs-sharing PVCs into VMs (replaces ExperimentalVirtiofsSupport,
# deprecated in v1.5, gone in v1.7). Whistler no longer uses virtiofs for VM
# homes (the NFS storage gateway replaced it — unprivileged virtiofsd made
# the share read-only for the guest user, kubevirt#13028); the gate is kept
# for now pending removal of the whole virtiofs path.
DEV_CONFIG='{"featureGates":["EnableVirtioFsStorageVolumes"]}'
if [[ "${KUBEVIRT_USE_EMULATION:-}" == "1" ]]; then
  echo "==> Enabling emulation (no /dev/kvm on the nodes; VMs will be slow)"
  DEV_CONFIG='{"featureGates":["EnableVirtioFsStorageVolumes"],"useEmulation":true}'
fi
echo "==> Configuring KubeVirt (feature gates: virtiofs for PVCs)"
kubectl -n kubevirt patch kubevirt kubevirt --type=merge \
  -p "{\"spec\":{\"configuration\":{\"developerConfiguration\":${DEV_CONFIG}}}}"

echo "==> Waiting for KubeVirt to become Available (pulls several images; can take minutes)"
kubectl -n kubevirt wait kubevirt kubevirt --for=condition=Available --timeout=15m

# --- CDI (DataVolume imports for imageURL boot sources) -----------------------
if [[ "${KUBEVIRT_INSTALL_CDI:-1}" != "0" ]]; then
  CDI_VERSION="${CDI_VERSION:-}"
  if [[ -z "$CDI_VERSION" ]]; then
    CDI_VERSION="$(curl -sfL https://api.github.com/repos/kubevirt/containerized-data-importer/releases/latest | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p')"
    [[ -n "$CDI_VERSION" ]] || { echo "ERROR: could not resolve latest CDI version"; exit 1; }
  fi
  CDI_URL="https://github.com/kubevirt/containerized-data-importer/releases/download/${CDI_VERSION}"

  echo "==> Installing CDI ${CDI_VERSION}"
  kubectl apply -f "${CDI_URL}/cdi-operator.yaml"

  # Same dance as KubeVirt: the operator registers the CDI CRD; retry the CR.
  cdi_applied=""
  for _ in $(seq 1 30); do
    if kubectl apply -f "${CDI_URL}/cdi-cr.yaml" 2>/dev/null; then cdi_applied=1; break; fi
    sleep 2
  done
  [[ -n "$cdi_applied" ]] || { echo "ERROR: could not apply the CDI CR (CRD never registered)"; exit 1; }

  echo "==> Waiting for CDI to become Available"
  kubectl wait cdi cdi --for=condition=Available --timeout=10m
fi

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
