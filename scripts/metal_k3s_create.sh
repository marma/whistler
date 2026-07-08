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
# A dev image registry is deployed *inside* the cluster (registry:2 with
# hostNetwork, so it answers on localhost:REGISTRY_PORT for both the host's
# Docker daemon pushing and k3s's containerd pulling — same address, no TLS,
# no kube-proxy localhost-NodePort quirks). Its manifest goes through k3s's
# auto-deploy dir and its data lives on a local-path PV, so
# metal_k3s_delete.sh removes every trace with the cluster. Use it from
# skaffold with:
#   skaffold config set --kube-context <CLUSTER_NAME> default-repo localhost:5000
#
# Env knobs:
#   CLUSTER_NAME      context name in the kubeconfig   (default: k3s-metal)
#   K3S_CHANNEL       k3s release channel              (default: stable)
#   K3S_EXTRA_ARGS    extra args for the k3s server    (default: empty)
#   KUBEVIRT_VERSION  KubeVirt release tag             (default: latest stable)
#   KUBECONFIG_OUT    where to write the kubeconfig
#   METAL_REGISTRY    set to 0 to skip the in-cluster dev registry
#   REGISTRY_PORT     host port for the dev registry   (default: 5000)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-k3s-metal}"
K3S_CHANNEL="${K3S_CHANNEL:-stable}"
K3S_EXTRA_ARGS="${K3S_EXTRA_ARGS:-}"
KUBECONFIG_OUT="${KUBECONFIG_OUT:-$HOME/.kube/${CLUSTER_NAME}.yaml}"
METAL_REGISTRY="${METAL_REGISTRY:-1}"
REGISTRY_PORT="${REGISTRY_PORT:-5000}"

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

# --- In-cluster dev registry (staged before install; k3s reads both at start) --
if [[ "$METAL_REGISTRY" == "1" ]]; then
  echo "==> Staging in-cluster dev registry (localhost:${REGISTRY_PORT})"

  # containerd side: plain-HTTP endpoint for localhost:PORT image refs.
  # (The Docker daemon pushing needs nothing — localhost is auto-insecure.)
  sudo mkdir -p /etc/rancher/k3s
  sudo tee /etc/rancher/k3s/registries.yaml >/dev/null <<EOF
mirrors:
  "localhost:${REGISTRY_PORT}":
    endpoint:
      - "http://localhost:${REGISTRY_PORT}"
EOF

  # k3s auto-applies everything in server/manifests/ (and re-applies on
  # change), so the registry needs no kubectl ordering. hostNetwork puts it
  # on localhost:PORT directly; Recreate avoids old/new pods fighting over
  # the port and the RWO local-path PV on redeploys.
  sudo mkdir -p /var/lib/rancher/k3s/server/manifests
  sudo tee /var/lib/rancher/k3s/server/manifests/whistler-dev-registry.yaml >/dev/null <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: dev-registry
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: registry-data
  namespace: dev-registry
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 50Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: registry
  namespace: dev-registry
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: registry
  template:
    metadata:
      labels:
        app: registry
    spec:
      hostNetwork: true
      containers:
        - name: registry
          image: registry:2
          env:
            - name: REGISTRY_HTTP_ADDR
              value: "127.0.0.1:${REGISTRY_PORT}"
            - name: REGISTRY_STORAGE_DELETE_ENABLED
              value: "true"
          ports:
            - containerPort: ${REGISTRY_PORT}
          readinessProbe:
            httpGet:
              path: /v2/
              port: ${REGISTRY_PORT}
              host: 127.0.0.1
          volumeMounts:
            - name: data
              mountPath: /var/lib/registry
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: registry-data
EOF
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

# --- Dev registry readiness ------------------------------------------------------
if [[ "$METAL_REGISTRY" == "1" ]]; then
  echo "==> Waiting for the dev registry"
  kubectl -n dev-registry rollout status deployment/registry --timeout=5m
  curl -sf "http://localhost:${REGISTRY_PORT}/v2/" >/dev/null \
    || { echo "ERROR: dev registry not answering on localhost:${REGISTRY_PORT}"; exit 1; }

  # Point skaffold's default-repo for this context at the in-cluster registry
  # (per-context global config; skaffold.yaml stays cluster-agnostic).
  if command -v skaffold >/dev/null 2>&1; then
    skaffold config set --kube-context "$CLUSTER_NAME" default-repo "localhost:${REGISTRY_PORT}"
    echo "    skaffold default-repo for context '$CLUSTER_NAME' -> localhost:${REGISTRY_PORT}"
  else
    echo "    (skaffold not found — when you use it, run:"
    echo "     skaffold config set --kube-context $CLUSTER_NAME default-repo localhost:${REGISTRY_PORT})"
  fi
fi

cat <<EOF

Cluster '$CLUSTER_NAME' is up on this host, with KubeVirt$( [[ "$METAL_REGISTRY" == "1" ]] && echo " and an in-cluster dev registry (localhost:${REGISTRY_PORT})" ).

  export KUBECONFIG=$KUBECONFIG_OUT
  kubectl get nodes
  kubectl -n kubevirt get pods
  virtctl console <vm>                    # once a VM is running

  skaffold dev                            # builds/pushes via localhost:${REGISTRY_PORT}
  scripts/metal_k3s_delete.sh             # tear everything down (registry + images too)
EOF
