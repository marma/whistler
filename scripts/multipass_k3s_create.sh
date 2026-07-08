#!/usr/bin/env bash
#
# Create a real (non-containerized) Kubernetes cluster for KubeVirt work:
# a Multipass VM with nested virtualization, running k3s + KubeVirt.
#
# Unlike k3d, the nodes here are actual VMs with their own kernel, so
# /dev/kvm is available inside the cluster and KubeVirt can run real VMs.
# Requires nested virtualization on the host (kvm_intel/kvm_amd nested=1);
# the script checks this on the host and verifies /dev/kvm inside the guest.
# KubeVirt itself is installed via scripts/install_kubevirt.sh (no libvirt
# needed anywhere on the host or in the VM — it is bundled in KubeVirt's
# virt-launcher pods).
#
# The kubeconfig is written to KUBECONFIG_OUT (default:
# ~/.kube/<VM_NAME>.yaml) with cluster/context named after the VM.
#
# Tear down with scripts/multipass_k3s_delete.sh.
#
# Env knobs:
#   VM_NAME           Multipass VM name            (default: whistler-k3s)
#   VM_CPUS           vCPUs                        (default: 4)
#   VM_MEMORY         RAM                          (default: 8G)
#   VM_DISK           disk size                    (default: 40G)
#   VM_IMAGE          Ubuntu image alias           (default: 24.04)
#   K3S_CHANNEL       k3s release channel          (default: stable)
#   KUBEVIRT_VERSION  KubeVirt release tag         (default: latest stable)
#   KUBECONFIG_OUT    where to write the kubeconfig
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

VM_NAME="${VM_NAME:-whistler-k3s}"
VM_CPUS="${VM_CPUS:-4}"
VM_MEMORY="${VM_MEMORY:-8G}"
VM_DISK="${VM_DISK:-40G}"
VM_IMAGE="${VM_IMAGE:-24.04}"
K3S_CHANNEL="${K3S_CHANNEL:-stable}"
KUBECONFIG_OUT="${KUBECONFIG_OUT:-$HOME/.kube/${VM_NAME}.yaml}"

command -v multipass >/dev/null || { echo "ERROR: multipass not found (snap install multipass)"; exit 1; }

# --- Host: nested virtualization must be on, or the guest gets no /dev/kvm ---
echo "==> Checking nested virtualization on the host"
nested=""
for mod in kvm_intel kvm_amd; do
  f="/sys/module/${mod}/parameters/nested"
  [[ -r "$f" ]] && nested="$(cat "$f")" && break
done
case "$nested" in
  1|Y|y) echo "    nested virtualization: enabled (${mod})" ;;
  *)
    echo "ERROR: nested virtualization is not enabled on the host."
    echo "Enable it and reload the module (Intel shown; use kvm_amd/kvm-amd on AMD):"
    echo "  echo 'options kvm_intel nested=1' | sudo tee /etc/modprobe.d/kvm-nested.conf"
    echo "  sudo modprobe -r kvm_intel && sudo modprobe kvm_intel"
    exit 1
    ;;
esac

if multipass info "$VM_NAME" >/dev/null 2>&1; then
  echo "ERROR: Multipass VM '$VM_NAME' already exists."
  echo "Tear it down first: scripts/multipass_k3s_delete.sh (or VM_NAME=$VM_NAME scripts/multipass_k3s_delete.sh)"
  exit 1
fi

echo "==> Launching VM '$VM_NAME' (${VM_CPUS} CPUs, ${VM_MEMORY} RAM, ${VM_DISK} disk, Ubuntu ${VM_IMAGE})"
multipass launch "$VM_IMAGE" \
  --name "$VM_NAME" \
  --cpus "$VM_CPUS" \
  --memory "$VM_MEMORY" \
  --disk "$VM_DISK"

mexec() { multipass exec "$VM_NAME" -- "$@"; }

# Grab the VM IP now, before k3s adds CNI interfaces and the VM reports
# multiple addresses.
VM_IP="$(multipass info "$VM_NAME" --format csv | awk -F, 'NR==2 {print $3}')"
[[ -n "$VM_IP" ]] || { echo "ERROR: could not determine VM IP"; exit 1; }

# --- Guest: verify virtualization actually made it through -------------------
echo "==> Verifying nested virtualization inside the guest"
if ! mexec grep -qE 'vmx|svm' /proc/cpuinfo; then
  echo "ERROR: guest CPU has no vmx/svm flags — nested virtualization did not"
  echo "pass through. Check the Multipass driver ('multipass get local.driver';"
  echo "qemu and lxd both use the host CPU model) and the host nested setting."
  exit 1
fi
mexec sh -c 'test -e /dev/kvm || sudo modprobe kvm_intel 2>/dev/null || sudo modprobe kvm_amd 2>/dev/null || true'
if mexec test -e /dev/kvm; then
  echo "    /dev/kvm present in guest — KubeVirt can use hardware virtualization"
else
  echo "ERROR: /dev/kvm missing in the guest despite vmx/svm flags."
  exit 1
fi

echo "==> Installing k3s (channel: ${K3S_CHANNEL})"
mexec sh -c "curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL='${K3S_CHANNEL}' sh -s - --write-kubeconfig-mode 644"

echo "==> Waiting for the node to become Ready"
for _ in $(seq 1 60); do
  if mexec kubectl get node 2>/dev/null | grep -q ' Ready'; then break; fi
  sleep 2
done
mexec kubectl get node | grep -q ' Ready' || { echo "ERROR: node never became Ready"; mexec kubectl get node || true; exit 1; }

# --- Kubeconfig on the host ---------------------------------------------------
echo "==> Writing kubeconfig to $KUBECONFIG_OUT (server: https://${VM_IP}:6443)"
mkdir -p "$(dirname "$KUBECONFIG_OUT")"
mexec cat /etc/rancher/k3s/k3s.yaml \
  | sed -e "s|https://127.0.0.1:6443|https://${VM_IP}:6443|" \
        -e "s|: default$|: ${VM_NAME}|" \
  > "$KUBECONFIG_OUT"
chmod 600 "$KUBECONFIG_OUT"

export KUBECONFIG="$KUBECONFIG_OUT"
kubectl get nodes 2>/dev/null || true

# The guest has /dev/kvm (verified above), so no emulation fallback needed.
"$SCRIPT_DIR/install_kubevirt.sh"

cat <<EOF

Cluster '$VM_NAME' is up, with KubeVirt.

  export KUBECONFIG=$KUBECONFIG_OUT
  kubectl get nodes
  kubectl -n kubevirt get pods
  virtctl console <vm>                    # once a VM is running

  multipass shell $VM_NAME                # shell into the node
  scripts/multipass_k3s_delete.sh         # tear everything down
EOF
