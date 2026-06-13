import os
import logging

import kopf
from kubernetes import client

from whistler.config import KubeConfigManager

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
