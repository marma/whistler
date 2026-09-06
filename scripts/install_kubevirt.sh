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
#   PCI_IDS                path to a pci.ids file, if not where pciutils puts it
#   PCI_PROBE_IMAGE        image for the `--allow-gpu auto` node probe (default
#                          busybox:1.36; anything with a POSIX sh)
#   PCI_PROBE_NODE_SELECTOR  which nodes `auto` probes (default: the GPU
#                          Operator's NFD label feature.node.kubernetes.io/pci-10de.present=true;
#                          every node when no node carries it)
#
# Arguments:
#   --allow-gpu <spec>     permit a GPU for VFIO passthrough (repeatable). Writes
#                          the KubeVirt CR's permittedHostDevices; see the "GPU
#                          passthrough" section below. <spec> is one of
#                            auto                      every NVIDIA display device on the cluster's
#                                                      nodes (probed with a throwaway pod)
#                            AD102_GEFORCE_RTX_4090    a resource name (nvidia.com/ optional)
#                            10de:2684  or  2684       a PCI device id
#                            10de:2684=nvidia.com/X    both, when pci.ids cannot be trusted
#                            none                      clear the list
#                          Not given: the list is left alone.
set -euo pipefail

usage() {
  sed -n '3,/^set -euo pipefail/{/^set -euo pipefail/d;s/^# \{0,1\}//;p}' "$0"
}

ALLOW_GPU_SPECS=()
while (( $# )); do
  case "$1" in
    --allow-gpu)
      [[ $# -ge 2 ]] || { echo "ERROR: --allow-gpu needs a value (see --help)" >&2; exit 2; }
      ALLOW_GPU_SPECS+=("$2"); shift 2 ;;
    --allow-gpu=*) ALLOW_GPU_SPECS+=("${1#*=}"); shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERROR: unknown argument '$1' (see --help)" >&2; exit 2 ;;
  esac
done

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

# --- GPU passthrough helpers ---------------------------------------------------

NVIDIA_VENDOR_ID=10de

# pci.ids as pciutils ships it (the `pci.ids` / `hwdata` package).
find_pci_ids() {
  local f
  for f in "${PCI_IDS:-}" /usr/share/misc/pci.ids /usr/share/hwdata/pci.ids \
           /usr/share/pci.ids /var/lib/pciutils/pci.ids; do
    [[ -n "$f" ]] || continue
    [[ -r "$f" ]] && { echo "$f"; return 0; }
    [[ -r "$f.gz" ]] && { echo "$f.gz"; return 0; }
  done
  cat >&2 <<EOF
ERROR: no pci.ids found (install pci.ids / hwdata, or point PCI_IDS at one).
       Or name the device fully: --allow-gpu 10de:2684=nvidia.com/AD102_GEFORCE_RTX_4090
EOF
  return 1
}

# Every NVIDIA device in pci.ids as "<device-id>\t<resource-name>\t<raw name>".
# The resource name is derived exactly as the GPU Operator's sandbox-device-
# plugin derives it (getDeviceName in pkg/device_plugin/device_plugin.go of
# NVIDIA/kubevirt-gpu-device-plugin): upper-case; "/" and "." become "_"; a run
# of whitespace becomes "_"; every other character outside [A-Za-z0-9_] is
# dropped. So "AD102 [GeForce RTX 4090]" -> AD102_GEFORCE_RTX_4090. The plugin
# reads the pci.ids its image was built with and this reads the host's, so a
# card added to the database recently can differ — the NOTE after the patch
# (a permitted resource no node advertises) is how that shows up.
nvidia_pci_table() {
  local f; f="$(find_pci_ids)" || return 1
  case "$f" in *.gz) zcat "$f" ;; *) cat "$f" ;; esac | awk -v vendor="$NVIDIA_VENDOR_ID" '
    /^#/ { next }
    /^C / { in_vendor = 0; next }
    /^[0-9a-f][0-9a-f][0-9a-f][0-9a-f]/ { in_vendor = (substr($0, 1, 4) == vendor); next }
    in_vendor && /^\t[0-9a-f][0-9a-f][0-9a-f][0-9a-f]/ {
      id = substr($0, 2, 4)
      raw = substr($0, 6); gsub(/^[ \t]+|[ \t]+$/, "", raw)
      name = toupper(raw)
      gsub(/[\/.]/, "_", name)
      gsub(/[ \t]+/, "_", name)
      gsub(/[^A-Za-z0-9_]/, "", name)
      print id "\t" name "\t" raw
    }'
}

# "<class> <vendor> <device>" (0x-prefixed hex) for every PCI function on a
# node, read from sysfs by a throwaway pod. The machine running this script is
# usually not a node, so the probe goes through the cluster: any pod sees the
# host's PCI bus in /sys/bus/pci/devices with no privilege at all — the same
# files the NVIDIA plugin reads. It runs in the kubevirt namespace, which
# KubeVirt labels pod-security `privileged`, so admission does not get in the
# way, and tolerates every taint because GPU nodes usually carry one. Waits for
# Succeeded and reads the logs rather than `run -i`, whose attach races a pod
# that exits in milliseconds.
probe_node_pci() {
  local node="$1" ns=kubevirt pod="pci-probe-${RANDOM}${RANDOM}" ok=""
  kubectl -n "$ns" run "$pod" --restart=Never --image="${PCI_PROBE_IMAGE:-busybox:1.36}" \
    --overrides="{\"spec\":{\"nodeName\":\"${node}\",\"tolerations\":[{\"operator\":\"Exists\"}]}}" \
    --command -- sh -c 'cd /sys/bus/pci/devices && for d in *; do echo "$(cat "$d/class") $(cat "$d/vendor") $(cat "$d/device")"; done' \
    >/dev/null || return 1
  if kubectl -n "$ns" wait pod/"$pod" --for=jsonpath='{.status.phase}'=Succeeded --timeout=120s >/dev/null 2>&1; then
    kubectl -n "$ns" logs "$pod" && ok=1
  else
    echo "ERROR: PCI probe pod on node ${node} did not complete:" >&2
    kubectl -n "$ns" get pod "$pod" -o wide >&2 || true
  fi
  kubectl -n "$ns" delete pod "$pod" --wait=false >/dev/null 2>&1 || true
  [[ -n "$ok" ]]
}

# One --allow-gpu spec -> "vendor:device\tresourceName" lines on stdout.
resolve_gpu_spec() {
  local spec="$1" table
  if [[ "$spec" == auto ]]; then
    # Which nodes: the GPU Operator's NFD label marks every node with an NVIDIA
    # PCI function; without it (no operator yet, or another NFD config) probe
    # them all — one short pod per node.
    local selector="${PCI_PROBE_NODE_SELECTOR:-feature.node.kubernetes.io/pci-10de.present=true}" nodes
    nodes="$(kubectl get nodes -l "$selector" -o jsonpath='{.items[*].metadata.name}')"
    if [[ -z "$nodes" ]]; then
      echo "==> No node carries ${selector}; probing every node" >&2
      nodes="$(kubectl get nodes -o jsonpath='{.items[*].metadata.name}')"
    fi
    # Class 03xxxx is a display controller (0300 VGA, 0302 3D — data-centre
    # cards are the latter). The card's audio function is 0403 and is skipped
    # here, which is the point.
    local node probe class vendor device found=""
    for node in $nodes; do
      echo "==> Probing the PCI bus of node ${node}" >&2
      probe="$(probe_node_pci "$node")" || return 1
      while read -r class vendor device; do
        [[ "${vendor#0x}" == "$NVIDIA_VENDOR_ID" && "${class#0x}" == 03* ]] || continue
        resolve_gpu_spec "${NVIDIA_VENDOR_ID}:${device#0x}" || return 1
        found=1
      done <<<"$probe"
    done
    [[ -n "$found" ]] || { echo "ERROR: --allow-gpu auto: no NVIDIA display device (PCI class 03xx) on any probed node" >&2; return 1; }

  elif [[ "$spec" == *=* ]]; then
    if [[ ! "$spec" =~ ^([0-9a-fA-F]{4}:[0-9a-fA-F]{4})=([a-z0-9][a-z0-9.-]*/)?([A-Za-z0-9_.-]+)$ ]]; then
      echo "ERROR: --allow-gpu '${spec}': expected vendor:device=resourceName, e.g. 10de:2684=nvidia.com/AD102_GEFORCE_RTX_4090" >&2
      return 1
    fi
    printf '%s\t%s%s\n' "${BASH_REMATCH[1],,}" "${BASH_REMATCH[2]:-nvidia.com/}" "${BASH_REMATCH[3]}"

  elif [[ "$spec" =~ ^(([0-9a-fA-F]{4}):)?([0-9a-fA-F]{4})$ ]]; then
    local vendor="${BASH_REMATCH[2]:-$NVIDIA_VENDOR_ID}" id="${BASH_REMATCH[3],,}"
    if [[ "${vendor,,}" != "$NVIDIA_VENDOR_ID" ]]; then
      echo "ERROR: --allow-gpu '${spec}': only NVIDIA (${NVIDIA_VENDOR_ID}:xxxx) devices resolve by id; use vendor:device=resourceName" >&2
      return 1
    fi
    table="$(nvidia_pci_table)" || return 1
    local line name raw
    line="$(awk -F'\t' -v id="$id" '$1 == id { print; exit }' <<<"$table")"
    if [[ -z "$line" ]]; then
      echo "ERROR: --allow-gpu '${spec}': ${NVIDIA_VENDOR_ID}:${id} is not in pci.ids; name it fully: --allow-gpu ${NVIDIA_VENDOR_ID}:${id}=nvidia.com/<NAME>" >&2
      return 1
    fi
    IFS=$'\t' read -r _ name raw <<<"$line"
    case "$name" in
      *AUDIO*) echo "==> WARNING: ${NVIDIA_VENDOR_ID}:${id} is '${raw}' — the card's audio function, not a GPU" >&2 ;;
    esac
    printf '%s:%s\tnvidia.com/%s\n' "$NVIDIA_VENDOR_ID" "$id" "$name"

  else
    local want="${spec#nvidia.com/}" ids
    table="$(nvidia_pci_table)" || return 1
    ids="$(awk -F'\t' -v n="$want" '$2 == n { print $1 }' <<<"$table")"
    if [[ -z "$ids" ]]; then
      cat >&2 <<EOF
ERROR: --allow-gpu '${spec}': no NVIDIA device in pci.ids normalises to ${want}.
       Try the id instead (lspci -nn -d ${NVIDIA_VENDOR_ID}: on the node prints it), or give both:
       --allow-gpu ${NVIDIA_VENDOR_ID}:xxxx=nvidia.com/${want}
EOF
      return 1
    fi
    if (( $(wc -l <<<"$ids") > 1 )); then
      echo "ERROR: --allow-gpu '${spec}': ${want} names several devices (${NVIDIA_VENDOR_ID}:$(paste -sd, <<<"$ids" | sed "s/,/, ${NVIDIA_VENDOR_ID}:/g")); pick one by id" >&2
      return 1
    fi
    printf '%s:%s\tnvidia.com/%s\n' "$NVIDIA_VENDOR_ID" "$ids" "$want"
  fi
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

# --- GPU passthrough: permittedHostDevices -------------------------------------
# KubeVirt hands a host PCI device to a guest only if the KubeVirt CR permits
# it, and nothing fills that list in for you. The NVIDIA GPU Operator's
# sandbox-device-plugin binds the card to vfio-pci and advertises it on the
# node (e.g. nvidia.com/AD102_GEFORCE_RTX_4090), but both NVIDIA and KubeVirt
# document the allow-list itself as the admin's manual step: it is the
# cluster-level decision about what may enter a root-capable guest, and
# KubeVirt has no wildcard, vendor-only selector or discovery mode for it.
# `--allow-gpu` makes the step declarative instead of typed, and resolves the
# half you did not give: the device id comes from the nodes' sysfs (auto,
# through a probe pod — this script rarely runs on a node) or pci.ids (from a
# name), the resource name from pci.ids by the plugin's own rule
# (nvidia_pci_table). Every entry is written with externalResourceProvider=true
# — the sandbox plugin allocates, KubeVirt only permits — so the selector is
# informational there and the resource name is the half that has to be right.
# List the display function only: the card's audio controller is advertised
# right next to it and must not be permitted (Whistler's GPU catalog reads this
# list to tell the two apart); `auto` skips it by PCI class. A JSON merge patch
# replaces the whole pciHostDevices list, so these arguments are the single
# author of it — `--allow-gpu none` clears it, no argument keeps whatever is
# configured (the useEmulation rule). mediatedDevices (vGPU) are not touched.
if (( ${#ALLOW_GPU_SPECS[@]} )); then
  pci_devs=""
  pci_names=()
  if [[ " ${ALLOW_GPU_SPECS[*]} " == *" none "* ]]; then
    (( ${#ALLOW_GPU_SPECS[@]} == 1 )) || { echo "ERROR: --allow-gpu none cannot be combined with other --allow-gpu values" >&2; exit 1; }
    echo "==> Clearing KubeVirt permitted PCI host devices (--allow-gpu none)"
  else
    echo "==> Configuring KubeVirt permitted PCI host devices"
    seen_ids=" "
    for spec in "${ALLOW_GPU_SPECS[@]}"; do
      resolved="$(resolve_gpu_spec "$spec")" || exit 1
      while IFS=$'\t' read -r id rname; do
        [[ -n "$id" ]] || continue
        # Two identical cards resolve to the same id under `auto`; one entry.
        [[ "$seen_ids" == *" $id "* ]] && continue
        seen_ids+="$id "
        echo "    ${id} -> ${rname}"
        pci_devs+="{\"pciVendorSelector\":\"${id}\",\"resourceName\":\"${rname}\",\"externalResourceProvider\":true},"
        pci_names+=("$rname")
      done <<<"$resolved"
    done
  fi
  kubectl -n kubevirt patch kubevirt kubevirt --type=merge \
    -p "{\"spec\":{\"configuration\":{\"permittedHostDevices\":{\"pciHostDevices\":[${pci_devs%,}]}}}}"

  # Informational: a permitted resource nobody advertises is a VM that pends
  # forever on it. Expected right after a fresh install, where the GPU Operator
  # has not bound the card yet — hence a NOTE, not an error.
  # jsonpath: the key holds a dot, which must be escaped; the bracket form
  # silently yields nothing for a key with a slash in it.
  for rname in ${pci_names+"${pci_names[@]}"}; do
    advertised="$(kubectl get nodes \
      -o jsonpath="{range .items[*]}{.status.allocatable.${rname//./\\.}}{' '}{end}" 2>/dev/null \
      | tr ' ' '\n' | grep -vx '0\?' | head -1 || true)"
    if [[ -z "$advertised" ]]; then
      echo "    NOTE: no node advertises ${rname} yet. Fine if the GPU Operator has not"
      echo "    bound the device: it needs sandboxWorkloads.enabled=true and the node"
      echo "    labelled nvidia.com/gpu.workload.config=vm-passthrough. If it stays"
      echo "    that way once bound, the plugin's pci.ids names the card differently:"
      echo "    compare kubectl get node <n> -o jsonpath='{.status.allocatable}'."
    fi
  done
fi

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
