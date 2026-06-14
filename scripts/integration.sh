#!/usr/bin/env bash
#
# C1 integration harness: run the whistler server and operator as host
# processes against a Kubernetes cluster, and run the integration test suite
# (the SSH round trip). Pods run for real; the app code runs on the host so
# there is no image build/import in the loop.
#
# Cluster providers (PROVIDER env, default: auto):
#   auto      use k3d if available, otherwise the current kubectl context
#   k3d       create/delete a throwaway k3d cluster
#   existing  use the current kubectl context (e.g. kind, docker-desktop);
#             the cluster is left running, only the resources we create are
#             cleaned up.
#
# Requirements: kubectl, a Python env with the package installed
# (`pip install -e .[test]`), and k3d only for PROVIDER=k3d.
#
# Env knobs:
#   PROVIDER      auto | k3d | existing      (default: auto)
#   CLUSTER       k3d cluster name           (default: whistler-it)
#   KEEP_CLUSTER  if set (k3d), don't delete the cluster on exit
#   PYTHON        python interpreter         (default: .venv/bin/python or python)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CLUSTER="${CLUSTER:-whistler-it}"
SYS_NS="whistler"
USER_NS="whistler-user-tester"
TEST_USER="tester"
TEST_TEMPLATE="small"

PROVIDER="${PROVIDER:-auto}"
if [[ "$PROVIDER" == "auto" ]]; then
  if command -v k3d >/dev/null 2>&1; then PROVIDER="k3d"; else PROVIDER="existing"; fi
fi

if [[ -z "${PYTHON:-}" ]]; then
  if [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then PYTHON="$REPO_ROOT/.venv/bin/python"; else PYTHON="python"; fi
fi

command -v kubectl >/dev/null || { echo "kubectl not found"; exit 1; }

# Fail fast if :8022 is already taken — otherwise the readiness check below
# would pass against a stale/orphaned server and tests would run against it.
if nc -z 127.0.0.1 8022 2>/dev/null; then
  echo "ERROR: port 8022 is already in use (orphaned whistler server?). Free it first:"
  echo "  pkill -f 'whistler.server'; pkill -f 'kopf run whistler'"
  exit 1
fi

WORK="$(mktemp -d)"
SERVER_PID=""
OPERATOR_PID=""
PORTAL_PID=""
GUACD_PF_PID=""

cleanup() {
  set +e
  [[ -n "$SERVER_PID" ]]    && kill "$SERVER_PID"    2>/dev/null
  [[ -n "$OPERATOR_PID" ]]  && kill "$OPERATOR_PID"  2>/dev/null
  [[ -n "$PORTAL_PID" ]]    && kill "$PORTAL_PID"    2>/dev/null
  [[ -n "$GUACD_PF_PID" ]]  && kill "$GUACD_PF_PID"  2>/dev/null
  if [[ "$PROVIDER" == "k3d" && -z "${KEEP_CLUSTER:-}" ]]; then
    k3d cluster delete "$CLUSTER" >/dev/null 2>&1
  elif [[ "$PROVIDER" == "existing" ]]; then
    # Only remove what we created; leave the cluster itself alone.
    echo "==> Cleaning up test namespaces and template"
    kubectl delete whistlertemplate "$TEST_TEMPLATE" -n "$SYS_NS" --ignore-not-found >/dev/null 2>&1
    kubectl delete deployment,service whistler-guacd -n "$SYS_NS" --ignore-not-found >/dev/null 2>&1
    kubectl delete namespace "$USER_NS" "$SYS_NS" --ignore-not-found --wait=false >/dev/null 2>&1
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

if [[ "$PROVIDER" == "k3d" ]]; then
  echo "==> Using k3d cluster '$CLUSTER'"
  command -v k3d >/dev/null || { echo "k3d not found"; exit 1; }
  if ! k3d cluster list "$CLUSTER" >/dev/null 2>&1; then
    k3d cluster create "$CLUSTER" --wait
  fi
  export KUBECONFIG="$WORK/kubeconfig"
  k3d kubeconfig get "$CLUSTER" > "$KUBECONFIG"
else
  echo "==> Using existing kubectl context: $(kubectl config current-context)"
  export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
fi

echo "==> Installing CRDs, PriorityClass, namespace and test template"
kubectl apply -f charts/whistler/crds/crds.yaml
kubectl apply -f charts/whistler/templates/priorityclass.yaml
kubectl create namespace "$SYS_NS" --dry-run=client -o yaml | kubectl apply -f -

# A lightweight system template so ephemeral sessions schedule fast.
kubectl apply -f - <<EOF
apiVersion: whistler.martinmalmsten.net/v1
kind: WhistlerTemplate
metadata:
  name: ${TEST_TEMPLATE}
  namespace: ${SYS_NS}
spec:
  # Needs a real userland: the server bridges sessions via `bash -c` with an
  # inner `sh -l` + `base64`, so busybox is not enough.
  image: ubuntu:24.04
  description: "Integration test template"
  resources:
    cpu: "100m"
    memory: "128Mi"
EOF

echo "==> Generating test key and config"
ssh-keygen -t ed25519 -N "" -f "$WORK/id" -q
PUBKEY="$(cat "$WORK/id.pub")"
cat > "$WORK/users.yaml" <<EOF
- name: ${TEST_USER}
  publicKeys:
    - "${PUBKEY}"
EOF
echo "[]" > "$WORK/volumes.yaml"
echo "[]" > "$WORK/selectors.yaml"
cat > "$WORK/networkpolicy.yaml" <<EOF
egress:
  allowCIDRs: []
  blockCIDRs: []
EOF

COMMON_ENV=(
  "KUBECONFIG=$KUBECONFIG"
  "POD_NAMESPACE=$SYS_NS"
  "WHISTLER_USERS_FILE=$WORK/users.yaml"
  "WHISTLER_CONFIG_DIR=$WORK"
  "USER_VOLUME_ACCESS_MODE=ReadWriteOnce"
  "USER_VOLUME_SIZE=100Mi"
)

echo "==> Starting operator (kopf)"
env "${COMMON_ENV[@]}" OPERATOR_LOG_LEVEL=INFO \
  "$PYTHON" -m kopf run whistler/operator.py --verbose >"$WORK/operator.log" 2>&1 &
OPERATOR_PID=$!

echo "==> Starting server on :8022"
env "${COMMON_ENV[@]}" WHISTLER_LOG_LEVEL=INFO \
  "$PYTHON" -m whistler.server --kubeconfig "$KUBECONFIG" >"$WORK/server.log" 2>&1 &
SERVER_PID=$!

echo "==> Waiting for server port 8022"
for _ in $(seq 1 30); do
  if nc -z 127.0.0.1 8022 2>/dev/null; then break; fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then echo "server exited early:"; cat "$WORK/server.log"; exit 1; fi
  sleep 1
done

# --- Display path: shared guacd + portal (for tests/integration/test_display.py) ---
echo "==> Deploying guacd into $SYS_NS"
kubectl apply -n "$SYS_NS" -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: whistler-guacd
  labels: { app: whistler-guacd }
spec:
  replicas: 1
  selector: { matchLabels: { app: whistler-guacd } }
  template:
    metadata: { labels: { app: whistler-guacd } }
    spec:
      automountServiceAccountToken: false
      containers:
        - name: guacd
          image: guacamole/guacd:1.5.5
          ports: [{ containerPort: 4822 }]
---
apiVersion: v1
kind: Service
metadata:
  name: whistler-guacd
  labels: { app: whistler-guacd }
spec:
  selector: { app: whistler-guacd }
  ports: [{ name: guacd, port: 4822, targetPort: 4822 }]
EOF
kubectl rollout status deployment/whistler-guacd -n "$SYS_NS" --timeout=120s || {
  echo "guacd did not become ready"; kubectl describe deploy/whistler-guacd -n "$SYS_NS"; exit 1; }

echo "==> Port-forwarding guacd to 127.0.0.1:4822"
kubectl port-forward -n "$SYS_NS" svc/whistler-guacd 4822:4822 >"$WORK/guacd-pf.log" 2>&1 &
GUACD_PF_PID=$!
for _ in $(seq 1 30); do nc -z 127.0.0.1 4822 2>/dev/null && break; sleep 1; done

echo "==> Starting portal on :8080"
env "${COMMON_ENV[@]}" WHISTLER_LOG_LEVEL=INFO \
  WHISTLER_AUTH_ALLOW_ANY=true GUACD_HOST=127.0.0.1 GUACD_PORT=4822 PORTAL_PORT=8080 \
  "$PYTHON" -m whistler.portal >"$WORK/portal.log" 2>&1 &
PORTAL_PID=$!
for _ in $(seq 1 30); do
  if nc -z 127.0.0.1 8080 2>/dev/null; then break; fi
  if ! kill -0 "$PORTAL_PID" 2>/dev/null; then echo "portal exited early:"; cat "$WORK/portal.log"; exit 1; fi
  sleep 1
done

echo "==> Running integration tests"
set +e
env \
  WHISTLER_TEST_SSH_HOST=127.0.0.1 \
  WHISTLER_TEST_SSH_PORT=8022 \
  WHISTLER_TEST_USER="$TEST_USER" \
  WHISTLER_TEST_TEMPLATE="$TEST_TEMPLATE" \
  WHISTLER_TEST_SYS_NS="$SYS_NS" \
  WHISTLER_TEST_KEY="$WORK/id" \
  WHISTLER_TEST_PORTAL="http://127.0.0.1:8080" \
  "$PYTHON" -m pytest tests/integration -m integration -v
RC=$?
set -e

if [[ $RC -ne 0 ]]; then
  echo "==> server.log (tail)";   tail -n 50 "$WORK/server.log"   || true
  echo "==> operator.log (tail)"; tail -n 50 "$WORK/operator.log" || true
  echo "==> portal.log (tail)";   tail -n 50 "$WORK/portal.log"   || true
fi
exit $RC
