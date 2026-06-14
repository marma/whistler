import os
import logging

import kopf
from kubernetes import client

from whistler.config import (
    KubeConfigManager,
    KUBEVIRT_GROUP,
    KUBEVIRT_VERSION,
    KUBEVIRT_VM_PLURAL,
    KUBEVIRT_VMI_PLURAL,
)

logger = logging.getLogger("whistler.operator")

# Single ConfigManager shared across handlers. It loads in-cluster config and
# the mounted users/volumes/networkpolicy configuration once at startup.
_config_manager = None


def _get_config_manager() -> KubeConfigManager:
    global _config_manager
    if _config_manager is None:
        _config_manager = KubeConfigManager()
    return _config_manager


def _instance_short_name(name: str, user: str) -> str:
    """CR names are `{user}-{instance}`; ensure_pod expects the short part."""
    prefix = f"{user}-"
    return name[len(prefix):] if name.startswith(prefix) else name


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    level = os.environ.get("OPERATOR_LOG_LEVEL", "INFO").upper()
    logging.getLogger("whistler").setLevel(level)
    # Instantiate eagerly so config-loading problems surface at startup.
    _get_config_manager()


@kopf.on.create('whistler.martinmalmsten.net', 'v1', 'whistlerinstances')
@kopf.on.update('whistler.martinmalmsten.net', 'v1', 'whistlerinstances')
@kopf.on.resume('whistler.martinmalmsten.net', 'v1', 'whistlerinstances')
def reconcile_fn(spec, name, namespace, meta, patch, logger, **kwargs):
    """Own pod lifecycle: ensure the pod for this instance exists.

    The SSH server no longer creates pods directly — it creates/patches the
    WhistlerInstance CR and waits, which makes this the single owner of pod
    creation and lets pods be (re)created on resume.
    """
    if meta.get('deletionTimestamp'):
        logger.info(f"Skipping reconcile for deleting instance {name}")
        return

    user = spec.get('user')
    if not user:
        logger.warning(f"Instance {name} has no spec.user; cannot reconcile")
        return

    instance_name = _instance_short_name(name, user)
    cm = _get_config_manager()

    logger.info(f"Reconciling instance {name}: ensuring pod exists")
    created = cm.ensure_pod(user, instance_name)

    if created:
        patch.status['phase'] = 'Provisioning'
        patch.status['podName'] = name
    else:
        # ensure_pod returns False for a transient state (e.g. the old pod is
        # still terminating) or a hard error (e.g. missing template). Retry with
        # backoff; transient cases resolve quickly, hard errors stay visible in
        # the logs. Failure classification is a follow-up (operator self-healing).
        patch.status['phase'] = 'Pending'
        raise kopf.TemporaryError(f"Pod for {name} could not be created yet", delay=2)


@kopf.on.delete('whistler.martinmalmsten.net', 'v1', 'whistlerinstances')
def delete_fn(spec, name, namespace, logger, **kwargs):
    logger.info(f"Deleting instance {name}")
    # Explicitly delete pod to ensure cleanup, even though GC should handle it
    # via ownerReferences.
    api = client.CoreV1Api()
    try:
        pod_name = name  # pod name == instance (CR) name
        api.delete_namespaced_pod(pod_name, namespace)
        logger.info(f"Explicitly deleted pod {pod_name}")
    except client.rest.ApiException as e:
        if e.status != 404:
            logger.warning(f"Failed to delete pod {name}: {e}")


# --------------------------------------------------------------------------- #
# DesktopSession lifecycle: desktop pod or KubeVirt VM. Because Guacamole is   #
# deferred there is no SSH server in the loop to watch readiness, so the       #
# operator owns the status phase machine: Provisioning -> Booting -> Ready /   #
# Failed. KubeVirt may be absent — every VMI read is guarded so the operator   #
# starts and runs without the KubeVirt CRDs (a timer probes on demand; no VMI  #
# watch is ever registered).                                                   #
# --------------------------------------------------------------------------- #


def _desktop_address(name: str, namespace: str) -> str:
    """The per-session ClusterIP Service DNS name (Service name == CR name)."""
    return f"{name}.{namespace}.svc.cluster.local"


def _map_pod_phase(pod_phase: str, all_ready: bool) -> str:
    """Pure mapping from a pod's phase/readiness to a DesktopSession phase."""
    if pod_phase == "Running" and all_ready:
        return "Ready"
    if pod_phase == "Failed":
        return "Failed"
    return "Booting"


def _session_backend(spec, namespace):
    """Resolve a DesktopSession's backend (pod|vm) by reading its template.
    Defaults to 'pod' when the template can't be resolved."""
    cm = _get_config_manager()
    template = cm._resolve_desktop_template(namespace, spec.get('templateRef'))
    return (template.get('spec', {}) if template else {}).get('backend', 'pod'), template


def _probe_pod(namespace, name, logger):
    """Return (session_phase, podName, address) by reading the desktop pod."""
    core = client.CoreV1Api()
    try:
        pod = core.read_namespaced_pod(name, namespace)
    except client.rest.ApiException as e:
        if e.status != 404:
            logger.warning(f"Error reading desktop pod {name}: {e}")
        return ("Provisioning", None, None)  # not created yet
    if pod.metadata.deletion_timestamp:
        return ("Booting", name, None)
    phase = getattr(pod.status, "phase", None)
    statuses = pod.status.container_statuses or []
    all_ready = bool(statuses) and all(cs.ready for cs in statuses)
    session_phase = _map_pod_phase(phase, all_ready)
    address = _desktop_address(name, namespace) if session_phase == "Ready" else None
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
        return ("Ready", name, _desktop_address(name, namespace))
    return ("Booting", name, None)


@kopf.on.create('whistler.martinmalmsten.net', 'v1', 'desktopsessions')
@kopf.on.update('whistler.martinmalmsten.net', 'v1', 'desktopsessions')
@kopf.on.resume('whistler.martinmalmsten.net', 'v1', 'desktopsessions')
def reconcile_desktop_fn(spec, name, namespace, meta, patch, logger, **kwargs):
    """Own desktop lifecycle: ensure the pod/VM (and per-session Service) exist.
    The phase machine itself is driven by desktop_phase_timer."""
    if meta.get('deletionTimestamp'):
        logger.info(f"Skipping reconcile for deleting desktop session {name}")
        return

    user = spec.get('user')
    if not user:
        logger.warning(f"DesktopSession {name} has no spec.user; cannot reconcile")
        return

    session_name = _instance_short_name(name, user)
    cm = _get_config_manager()

    logger.info(f"Reconciling desktop session {name}: ensuring backend exists")
    created = cm.ensure_desktop(user, session_name)

    if created:
        patch.status['phase'] = 'Provisioning'
    else:
        patch.status['phase'] = 'Failed'
        raise kopf.TemporaryError(f"Desktop for {name} could not be provisioned yet", delay=5)


@kopf.on.timer('whistler.martinmalmsten.net', 'v1', 'desktopsessions', interval=10)
def desktop_phase_timer(spec, name, namespace, meta, status, patch, logger, **kwargs):
    """Drive the DesktopSession phase machine by probing the child pod/VMI."""
    if meta.get('deletionTimestamp'):
        return
    phase = (status or {}).get('phase')
    if not phase:
        return  # not reconciled yet

    backend, template = _session_backend(spec, namespace)
    if backend == 'vm':
        new_phase, child_name, address = _probe_vmi(namespace, name, logger)
        patch.status['vmiName'] = child_name
    else:
        new_phase, child_name, address = _probe_pod(namespace, name, logger)
        patch.status['podName'] = child_name

    patch.status['backend'] = backend
    patch.status['phase'] = new_phase
    if address:
        patch.status['address'] = address
    if template:
        display_port = template.get('spec', {}).get('displayPort')
        if display_port is not None:
            patch.status['displayPort'] = display_port


@kopf.on.delete('whistler.martinmalmsten.net', 'v1', 'desktopsessions')
def delete_desktop_fn(spec, name, namespace, status, logger, **kwargs):
    """Tear down the per-session Service and the pod/VM. ownerReferences also GC
    these; the explicit deletes are for promptness (mirrors delete_fn)."""
    logger.info(f"Deleting desktop session {name}")
    core = client.CoreV1Api()
    try:
        core.delete_namespaced_service(name, namespace)
        logger.info(f"Deleted service {name}")
    except client.rest.ApiException as e:
        if e.status != 404:
            logger.warning(f"Failed to delete service {name}: {e}")

    backend = (status or {}).get('backend', 'pod')
    if backend == 'vm':
        try:
            client.CustomObjectsApi().delete_namespaced_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, namespace, KUBEVIRT_VM_PLURAL, name
            )
            logger.info(f"Deleted VirtualMachine {name}")
        except client.rest.ApiException as e:
            if e.status != 404:
                logger.warning(f"Failed to delete VirtualMachine {name}: {e}")
    else:
        try:
            core.delete_namespaced_pod(name, namespace)
            logger.info(f"Deleted desktop pod {name}")
        except client.rest.ApiException as e:
            if e.status != 404:
                logger.warning(f"Failed to delete desktop pod {name}: {e}")
