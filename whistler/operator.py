import os
import logging

import kopf
from kubernetes import client

from whistler.config import (
    KubeConfigManager,
    PolicyError,
    KUBEVIRT_GROUP,
    KUBEVIRT_VERSION,
    KUBEVIRT_VM_PLURAL,
    KUBEVIRT_VMI_PLURAL,
)

logger = logging.getLogger("whistler.operator")

# Single ConfigManager shared across handlers. It loads in-cluster config and
# the mounted users/volumes/networkpolicy/images configuration once at startup.
_config_manager = None


def _get_config_manager() -> KubeConfigManager:
    global _config_manager
    if _config_manager is None:
        _config_manager = KubeConfigManager()
    return _config_manager


def _session_short_name(name: str, user: str) -> str:
    """CR names are `{user}-{session}`; ensure_session expects the short part."""
    prefix = f"{user}-"
    return name[len(prefix):] if name.startswith(prefix) else name


# Back-compat alias for the unit tests that imported the old name.
_instance_short_name = _session_short_name


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    level = os.environ.get("OPERATOR_LOG_LEVEL", "INFO").upper()
    logging.getLogger("whistler").setLevel(level)
    # Instantiate eagerly so config-loading problems surface at startup.
    _get_config_manager()


# --------------------------------------------------------------------------- #
# Session lifecycle (unified ssh + desktop). The operator owns the pod/VM      #
# (and, for desktop, the per-session Service): reconcile provisions, the timer #
# drives Provisioning -> Booting -> Ready/Failed, and delete tears down.       #
#                                                                              #
# status.mode / status.runtime are denormalized by ensure_session so the timer #
# and delete handler dispatch (pod vs VMI, Service or not) without re-resolving #
# the template. KubeVirt may be absent — every VMI read is guarded so the      #
# operator runs without the KubeVirt CRDs installed.                           #
# --------------------------------------------------------------------------- #


def _session_address(name: str, namespace: str) -> str:
    """The per-session ClusterIP Service DNS name (Service name == CR name)."""
    return f"{name}.{namespace}.svc.cluster.local"


# Back-compat alias (unit-tested under the old name).
_desktop_address = _session_address


def _map_pod_phase(pod_phase: str, all_ready: bool) -> str:
    """Pure mapping from a pod's phase/readiness to a Session phase."""
    if pod_phase == "Running" and all_ready:
        return "Ready"
    if pod_phase == "Failed":
        return "Failed"
    return "Booting"


def _probe_pod(namespace, name, logger):
    """Return (session_phase, podName, address) by reading the session pod."""
    core = client.CoreV1Api()
    try:
        pod = core.read_namespaced_pod(name, namespace)
    except client.rest.ApiException as e:
        if e.status != 404:
            logger.warning(f"Error reading pod {name}: {e}")
        # No pod: reconcile creates it synchronously, so by the time the timer
        # probes, an absent pod means the session was stopped (pod deleted, CR
        # kept), not "still provisioning".
        return ("Stopped", None, None)
    if pod.metadata.deletion_timestamp:
        return ("Terminating", name, None)
    phase = getattr(pod.status, "phase", None)
    statuses = pod.status.container_statuses or []
    all_ready = bool(statuses) and all(cs.ready for cs in statuses)
    session_phase = _map_pod_phase(phase, all_ready)
    address = _session_address(name, namespace) if session_phase == "Ready" else None
    return (session_phase, name, address)


def _probe_vmi(namespace, name, logger):
    """Return (session_phase, vmiName, address) by reading the KubeVirt VMI.

    Guarded: a 404 covers both 'VMI not up yet' and 'KubeVirt CRDs absent';
    both are treated as still-Booting so the operator never crashes without
    KubeVirt installed."""
    api = client.CustomObjectsApi()
    try:
        vmi = api.get_namespaced_custom_object(
            KUBEVIRT_GROUP, KUBEVIRT_VERSION, namespace, KUBEVIRT_VMI_PLURAL, name
        )
    except client.rest.ApiException as e:
        if e.status != 404:
            logger.warning(f"Error reading VMI {name}: {e}")
        return ("Booting", name, None)
    phase = (vmi.get("status") or {}).get("phase")
    if phase == "Running":
        return ("Ready", name, _session_address(name, namespace))
    return ("Booting", name, None)


@kopf.on.create('whistler.martinmalmsten.net', 'v1', 'sessions')
@kopf.on.update('whistler.martinmalmsten.net', 'v1', 'sessions')
def reconcile_session_fn(spec, name, namespace, meta, patch, logger, **kwargs):
    """Own session lifecycle: ensure the pod/VM (and, for desktop, the
    per-session Service) exist. The phase machine itself runs in the timer.

    Consumers create/patch the Session CR rather than creating pods directly,
    making this the single owner of pod creation. Note there is deliberately no
    `@kopf.on.resume` handler: pods are created on-demand (CR creation, or the
    `whistler/last-connect` annotation bump when a user connects), not when the
    operator/cluster (re)starts — a session whose pod was stopped stays stopped
    until someone connects again. The phase timer still resumes on its own."""
    if meta.get('deletionTimestamp'):
        logger.info(f"Skipping reconcile for deleting session {name}")
        return

    user = spec.get('user')
    if not user:
        logger.warning(f"Session {name} has no spec.user; cannot reconcile")
        return

    session_name = _session_short_name(name, user)
    cm = _get_config_manager()

    logger.info(f"Reconciling session {name}: ensuring workload exists")
    try:
        result = cm.ensure_session(user, session_name)
    except PolicyError as e:
        # Hard, non-retryable: the template violates an operator policy.
        logger.error(f"Session {name} rejected by policy: {e}")
        patch.status['phase'] = 'Failed'
        return

    # Denormalize mode/runtime into status (when known) so the timer and delete
    # handler can dispatch without re-resolving the template.
    if result.get('mode'):
        patch.status['mode'] = result['mode']
    if result.get('runtime'):
        patch.status['runtime'] = result['runtime']
    if result.get('displayPort') is not None:
        patch.status['displayPort'] = result['displayPort']

    if result['ok']:
        patch.status['phase'] = 'Provisioning'
    else:
        # Transient (e.g. the old pod is still terminating, or the template
        # isn't present yet). Retry with backoff.
        patch.status['phase'] = 'Pending'
        raise kopf.TemporaryError(f"Workload for {name} could not be created yet", delay=2)


@kopf.on.timer('whistler.martinmalmsten.net', 'v1', 'sessions', interval=10)
def session_phase_timer(spec, name, namespace, meta, status, patch, logger, **kwargs):
    """Drive the Session phase machine by probing the child pod/VMI."""
    if meta.get('deletionTimestamp'):
        return
    status = status or {}
    if not status.get('phase'):
        return  # not reconciled yet

    runtime = status.get('runtime')
    if runtime == 'vm':
        new_phase, child_name, address = _probe_vmi(namespace, name, logger)
        patch.status['vmiName'] = child_name
    else:
        new_phase, child_name, address = _probe_pod(namespace, name, logger)
        patch.status['podName'] = child_name

    patch.status['phase'] = new_phase
    # Only desktop sessions have a Service / reachable address; ssh sessions are
    # bridged via `kubectl exec` and have no Service to point at.
    if status.get('mode') == 'desktop' and address:
        patch.status['address'] = address


@kopf.on.delete('whistler.martinmalmsten.net', 'v1', 'sessions')
def delete_session_fn(spec, name, namespace, status, logger, **kwargs):
    """Tear down the pod/VM (and, for desktop, the per-session Service).
    ownerReferences also GC these; the explicit deletes are for promptness."""
    logger.info(f"Deleting session {name}")
    status = status or {}
    core = client.CoreV1Api()

    if status.get('mode') == 'desktop':
        try:
            core.delete_namespaced_service(name, namespace)
            logger.info(f"Deleted service {name}")
        except client.rest.ApiException as e:
            if e.status != 404:
                logger.warning(f"Failed to delete service {name}: {e}")

    runtime = status.get('runtime')

    def _delete_pod():
        try:
            core.delete_namespaced_pod(name, namespace)  # pod name == CR name
            logger.info(f"Deleted pod {name}")
        except client.rest.ApiException as e:
            if e.status != 404:
                logger.warning(f"Failed to delete pod {name}: {e}")

    def _delete_vm():
        try:
            client.CustomObjectsApi().delete_namespaced_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, namespace, KUBEVIRT_VM_PLURAL, name
            )
            logger.info(f"Deleted VirtualMachine {name}")
        except client.rest.ApiException as e:
            if e.status != 404:
                logger.warning(f"Failed to delete VirtualMachine {name}: {e}")

    if runtime == 'vm':
        _delete_vm()
    elif runtime in ('container', 'kata'):
        _delete_pod()
    else:
        # status.runtime never written (e.g. failed before reconcile set it):
        # try both so nothing is orphaned.
        _delete_pod()
        _delete_vm()
