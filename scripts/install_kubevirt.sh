#!/usr/bin/env bash
#
# Install or upgrade KubeVirt in the cluster of the current KUBECONFIG, and
# virtctl onto this machine (~/.local/bin). Called by multipass_k3s_create.sh
# and metal_k3s_create.sh; usable standalone against any cluster.
#
# OPTIONAL on a cluster that already runs KubeVirt: Whistler needs only the
# kubevirt.io/v1 API, CDI for imageURL boot sources, and no non-default
# feature gates — see "Already running KubeVirt?" in README.md. Do NOT run
# this where another operator owns KubeVirt (HCO / OpenShift Virtualization):
# the feature-gate/emulation patch below edits the KubeVirt CR directly, and
# HCO reverts direct edits — such clusters are configured through the
# HyperConverged CR instead.
#
# SAFE TO RE-RUN. The script reads what is already installed and, by default,
# will NOT change its version: a re-run against an existing cluster reconciles
# configuration (feature gates, emulation) and the local virtctl, then exits.
# Changing versions is opt-in via KUBEVIRT_UPGRADE=1, because the default
# version is "whatever stable.txt says today" — without the gate, a re-run six
# months from now would silently move the cluster.
#
# KubeVirt supports upgrading one minor release at a time (N-1 -> N) and does
# not support downgrades; CDI is the same. Illegal jumps are refused even with
# KUBEVIRT_UPGRADE=1 — override with KUBEVIRT_FORCE=1 only if you know why.
#
# No libvirt is needed on the host or nodes: KubeVirt bundles libvirt/qemu
# inside its virt-launcher pods. The nodes just need /dev/kvm — without it,
# set KUBEVIRT_USE_EMULATION=1 (VMs run fully emulated, slow).
#
# Env knobs:
#   KUBEVIRT_VERSION       release tag (default: latest stable)
#   KUBEVIRT_USE_EMULATION 1 for nodes without /dev/kvm, 0 to turn it back off
#                          (unset leaves an existing setting alone)
#   KUBEVIRT_UPGRADE       set to 1 to allow changing the version of an
#                          existing install (default: refuse and report)
#   KUBEVIRT_FORCE         set to 1 to allow an unsupported version jump
#   KUBEVIRT_INSTALL_CDI   set to 0 to skip CDI (imageURL boot sources need it)
#   CDI_VERSION            CDI release tag (default: latest)
set -euo pipefail

KUBEVIRT_UPGRADE="${KUBEVIRT_UPGRADE:-0}"
KUBEVIRT_FORCE="${KUBEVIRT_FORCE:-0}"

# Feature gates this script guarantees are ON. Merged into whatever the cluster
# already has rather than overwriting it — an admin's own gates (and the stock
# CR's empty list) must survive a re-run.
#
# EnableVirtioFsStorageVolumes allows virtiofs-sharing PVCs into VMs (replaces
# ExperimentalVirtiofsSupport, deprecated in v1.5, gone in v1.7). Whistler no
# longer uses virtiofs for VM homes (they are disk images now — design/
# storage.md); the gate is kept pending removal of the whole virtiofs path.
REQUIRED_GATES=(EnableVirtioFsStorageVolumes)

# --- helpers -----------------------------------------------------------------

# "v1.8.4" / "v1.9.0-rc1" -> "1 8 4"
ver_parts() {
  local v="${1#v}"
  v="${v%%-*}"
  local a b c
  IFS='.' read -r a b c <<<"$v"
  echo "${a:-0} ${b:-0} ${c:-0}"
}

# Print a reason and return 1 when installed -> target is not a supported step.
check_upgrade_path() {
  local installed="$1" target="$2"
  local imaj imin ipat tmaj tmin tpat
  read -r imaj imin ipat <<<"$(ver_parts "$installed")"
  read -r tmaj tmin tpat <<<"$(ver_parts "$target")"

  if (( tmaj != imaj )); then
    echo "major version change ($installed -> $target)"; return 1
  fi
  if (( tmin < imin )) || { (( tmin == imin )) && (( tpat < ipat )); }; then
    echo "downgrade ($installed -> $target); downgrades are not supported"; return 1
  fi
  if (( tmin > imin + 1 )); then
    echo "skips $(( tmin - imin - 1 )) minor release(s) ($installed -> $target); upgrade one minor at a time"
    return 1
  fi
  return 0
}

# Decide whether to touch an existing install. Returns 0 to proceed, 1 to stop.
gate_version_change() {
  local what="$1" installed="$2" target="$3" env_hint="$4"
  [[ "$installed" == "$target" ]] && return 0

  local reason=""
  if ! reason="$(check_upgrade_path "$installed" "$target")"; then
    if [[ "$KUBEVIRT_FORCE" == "1" ]]; then
      echo "==> WARNING: ${what} ${reason} — proceeding because KUBEVIRT_FORCE=1"
      return 0
    fi
    cat >&2 <<EOF
ERROR: refusing to change ${what}: ${reason}.
       installed: ${installed}
       requested: ${target}
       Pin the next step with ${env_hint}=<tag>, or set KUBEVIRT_FORCE=1 to
       override (unsupported).
EOF
    return 1
  fi

  if [[ "$KUBEVIRT_UPGRADE" != "1" ]]; then
    cat >&2 <<EOF
ERROR: ${what} is installed at ${installed}; this run targets ${target}.
       Not upgrading by default. Re-run with KUBEVIRT_UPGRADE=1 to upgrade, or
       pin this run to the installed version with ${env_hint}=${installed}.
EOF
    return 1
  fi
  echo "==> Upgrading ${what}: ${installed} -> ${target}"
  return 0
}

json_array() {
  local out="" item
  for item in "$@"; do out+="\"${item}\","; done
  echo "[${out%,}]"
}

# --- resolve versions --------------------------------------------------------

KUBEVIRT_VERSION="${KUBEVIRT_VERSION:-}"
if [[ -z "$KUBEVIRT_VERSION" ]]; then
  KUBEVIRT_VERSION="$(curl -sfL https://storage.googleapis.com/kubevirt-prow/release/kubevirt/kubevirt/stable.txt)"
  [[ -n "$KUBEVIRT_VERSION" ]] || { echo "ERROR: could not resolve latest KubeVirt version" >&2; exit 1; }
fi
KUBEVIRT_URL="https://github.com/kubevirt/kubevirt/releases/download/${KUBEVIRT_VERSION}"

# What is already there? Empty on a fresh cluster.
KV_INSTALLED="$(kubectl -n kubevirt get kubevirt kubevirt \
  -o jsonpath='{.status.observedKubeVirtVersion}' 2>/dev/null || true)"
KV_EXISTS="$(kubectl -n kubevirt get kubevirt kubevirt -o name 2>/dev/null || true)"

# --- KubeVirt ----------------------------------------------------------------

if [[ -n "$KV_INSTALLED" ]]; then
  echo "==> KubeVirt ${KV_INSTALLED} is already installed (target: ${KUBEVIRT_VERSION})"
  gate_version_change "KubeVirt" "$KV_INSTALLED" "$KUBEVIRT_VERSION" KUBEVIRT_VERSION || exit 1
else
  echo "==> Installing KubeVirt ${KUBEVIRT_VERSION}"
fi

# Applying the operator manifest is both the install and the supported upgrade
# trigger: with no spec.imageTag on the CR, virt-operator rolls the components
# to match its own version. Re-applying the SAME version is a harmless no-op
# that also repairs a partially deleted install.
kubectl apply -f "${KUBEVIRT_URL}/kubevirt-operator.yaml"

# The stock kubevirt-cr.yaml ships an explicit `featureGates: []`, so applying
# it over a configured cluster wipes the gate list. Apply it only when the CR
# does not exist yet; an existing CR is configured by patch below, never
# replaced.
if [[ -z "$KV_EXISTS" ]]; then
  cr_applied=""
  for _ in $(seq 1 30); do
    if kubectl apply -f "${KUBEVIRT_URL}/kubevirt-cr.yaml" 2>/dev/null; then cr_applied=1; break; fi
    sleep 2
  done
  [[ -n "$cr_applied" ]] || { echo "ERROR: could not apply the KubeVirt CR (CRD never registered)" >&2; exit 1; }
fi

# Merge REQUIRED_GATES into whatever is live, so a re-run never drops a gate
# somebody else set. Read AFTER the CR exists, BEFORE the patch.
live_gates="$(kubectl -n kubevirt get kubevirt kubevirt \
  -o jsonpath='{.spec.configuration.developerConfiguration.featureGates[*]}' 2>/dev/null || true)"
merged_gates=()
for g in $live_gates "${REQUIRED_GATES[@]}"; do
  seen=""
  for m in ${merged_gates+"${merged_gates[@]}"}; do [[ "$m" == "$g" ]] && seen=1 && break; done
  [[ -n "$seen" ]] || merged_gates+=("$g")
done

# useEmulation is set explicitly to true OR false when the knob is given: a
# merge patch only ever ADDS keys, so `true` could never be taken back by
# re-running without the flag. Unset leaves any existing value alone.
dev_config="{\"featureGates\":$(json_array ${merged_gates+"${merged_gates[@]}"})"
case "${KUBEVIRT_USE_EMULATION:-}" in
  1) echo "==> Enabling emulation (no /dev/kvm on the nodes; VMs will be slow)"
     dev_config+=',"useEmulation":true' ;;
  0) echo "==> Disabling emulation (nodes must have /dev/kvm)"
     dev_config+=',"useEmulation":false' ;;
esac
dev_config+="}"

echo "==> Configuring KubeVirt (feature gates: ${merged_gates[*]})"
kubectl -n kubevirt patch kubevirt kubevirt --type=merge \
  -p "{\"spec\":{\"configuration\":{\"developerConfiguration\":${dev_config}}}}"

# `condition=Available` is NOT an upgrade gate: it is already true on an
# existing install and stays true while virt-operator rolls the components.
# The real signal is the observed version reaching the one we asked for, with
# the CR out of Deploying/Updating.
echo "==> Waiting for KubeVirt ${KUBEVIRT_VERSION} to roll out (pulls several images; can take minutes)"
deadline=$(( SECONDS + 900 ))
last_status=""
while (( SECONDS < deadline )); do
  observed="$(kubectl -n kubevirt get kubevirt kubevirt -o jsonpath='{.status.observedKubeVirtVersion}' 2>/dev/null || true)"
  phase="$(kubectl -n kubevirt get kubevirt kubevirt -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  [[ "$observed" == "$KUBEVIRT_VERSION" && "$phase" == "Deployed" ]] && break
  status="observed=${observed:-<none>} phase=${phase:-<none>}"
  [[ "$status" == "$last_status" ]] || { echo "    ${status}"; last_status="$status"; }
  sleep 5
done
if [[ "$observed" != "$KUBEVIRT_VERSION" || "$phase" != "Deployed" ]]; then
  echo "ERROR: KubeVirt did not reach ${KUBEVIRT_VERSION}/Deployed (observed=${observed:-<none>} phase=${phase:-<none>})" >&2
  kubectl -n kubevirt get kubevirt kubevirt -o jsonpath='{range .status.conditions[*]}{.type}={.status} {.reason}: {.message}{"\n"}{end}' >&2 || true
  exit 1
fi

# After a version change, VMIs keep running on their old virt-launcher until
# they are restarted — say so rather than leaving it to be discovered.
outdated="$(kubectl -n kubevirt get kubevirt kubevirt \
  -o jsonpath='{.status.outdatedVirtualMachineInstanceWorkloads}' 2>/dev/null || true)"
if [[ -n "$outdated" && "$outdated" != "0" ]]; then
  echo "==> NOTE: ${outdated} running VM workload(s) are still on the previous version;"
  echo "    they pick up ${KUBEVIRT_VERSION} on their next restart."
fi

# --- CDI (DataVolume imports for imageURL boot sources) -----------------------
if [[ "${KUBEVIRT_INSTALL_CDI:-1}" != "0" ]]; then
  CDI_VERSION="${CDI_VERSION:-}"
  if [[ -z "$CDI_VERSION" ]]; then
    CDI_VERSION="$(curl -sfL https://api.github.com/repos/kubevirt/containerized-data-importer/releases/latest | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p')"
    [[ -n "$CDI_VERSION" ]] || { echo "ERROR: could not resolve latest CDI version" >&2; exit 1; }
  fi
  CDI_URL="https://github.com/kubevirt/containerized-data-importer/releases/download/${CDI_VERSION}"

  CDI_INSTALLED="$(kubectl get cdi cdi -o jsonpath='{.status.observedVersion}' 2>/dev/null || true)"
  CDI_EXISTS="$(kubectl get cdi cdi -o name 2>/dev/null || true)"

  if [[ -n "$CDI_INSTALLED" ]]; then
    echo "==> CDI ${CDI_INSTALLED} is already installed (target: ${CDI_VERSION})"
    gate_version_change "CDI" "$CDI_INSTALLED" "$CDI_VERSION" CDI_VERSION || exit 1
  else
    echo "==> Installing CDI ${CDI_VERSION}"
  fi

  kubectl apply -f "${CDI_URL}/cdi-operator.yaml"

  if [[ -z "$CDI_EXISTS" ]]; then
    cdi_applied=""
    for _ in $(seq 1 30); do
      if kubectl apply -f "${CDI_URL}/cdi-cr.yaml" 2>/dev/null; then cdi_applied=1; break; fi
      sleep 2
    done
    [[ -n "$cdi_applied" ]] || { echo "ERROR: could not apply the CDI CR (CRD never registered)" >&2; exit 1; }
  fi

  echo "==> Waiting for CDI ${CDI_VERSION} to roll out"
  deadline=$(( SECONDS + 600 ))
  last_status=""
  while (( SECONDS < deadline )); do
    cdi_observed="$(kubectl get cdi cdi -o jsonpath='{.status.observedVersion}' 2>/dev/null || true)"
    cdi_phase="$(kubectl get cdi cdi -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [[ "$cdi_observed" == "$CDI_VERSION" && "$cdi_phase" == "Deployed" ]] && break
    status="observed=${cdi_observed:-<none>} phase=${cdi_phase:-<none>}"
    [[ "$status" == "$last_status" ]] || { echo "    ${status}"; last_status="$status"; }
    sleep 5
  done
  if [[ "$cdi_observed" != "$CDI_VERSION" || "$cdi_phase" != "Deployed" ]]; then
    echo "ERROR: CDI did not reach ${CDI_VERSION}/Deployed (observed=${cdi_observed:-<none>} phase=${cdi_phase:-<none>})" >&2
    exit 1
  fi
fi

# --- virtctl (client for console/vnc/start/stop) ------------------------------
# Version-matched, not merely present: after an upgrade an older virtctl is
# skewed against the cluster, and the old `command -v` check never noticed.
case "$(uname -m)" in
  x86_64)  VIRTCTL_ARCH=amd64 ;;
  aarch64) VIRTCTL_ARCH=arm64 ;;
  *)       VIRTCTL_ARCH="" ;;
esac

if [[ -z "$VIRTCTL_ARCH" ]]; then
  echo "==> Skipping virtctl (unsupported arch $(uname -m))"
else
  virtctl_have=""
  virtctl_path="$(command -v virtctl 2>/dev/null || true)"
  if [[ -n "$virtctl_path" ]]; then
    virtctl_have="$("$virtctl_path" version --client 2>/dev/null \
      | sed -n 's/.*GitVersion:"\([^"]*\)".*/\1/p')"
  fi

  if [[ "$virtctl_have" == "$KUBEVIRT_VERSION" ]]; then
    echo "==> virtctl ${virtctl_have} already matches (${virtctl_path})"
  else
    target="$HOME/.local/bin/virtctl"
    echo "==> Installing virtctl ${KUBEVIRT_VERSION} to ${target}${virtctl_have:+ (replacing ${virtctl_have})}"
    mkdir -p "$HOME/.local/bin"
    curl -sfL -o "${target}.tmp" \
      "${KUBEVIRT_URL}/virtctl-${KUBEVIRT_VERSION}-linux-${VIRTCTL_ARCH}"
    # Download to .tmp and swap, so an interrupted run cannot leave a truncated
    # binary where a working one used to be.
    if [[ ! -s "${target}.tmp" ]]; then
      rm -f "${target}.tmp"
      echo "ERROR: virtctl download was empty (${KUBEVIRT_URL}/virtctl-${KUBEVIRT_VERSION}-linux-${VIRTCTL_ARCH})" >&2
      exit 1
    fi
    chmod +x "${target}.tmp"
    mv "${target}.tmp" "$target"
    if [[ -n "$virtctl_path" && "$virtctl_path" != "$target" ]]; then
      echo "    NOTE: ${virtctl_path} (${virtctl_have:-unknown}) still shadows it on PATH;"
      echo "    put $HOME/.local/bin first, or remove the other copy."
    fi
  fi
fi

echo "KubeVirt ${KUBEVIRT_VERSION} is Deployed."
