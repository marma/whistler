import kopf
import kubernetes
import yaml
from kubernetes import client, config

@kopf.on.create('whistler.example.com', 'v1', 'whistlerinstances')
@kopf.on.update('whistler.example.com', 'v1', 'whistlerinstances')
@kopf.on.resume('whistler.example.com', 'v1', 'whistlerinstances')
def reconcile_fn(spec, name, namespace, logger, meta, **kwargs):
    if meta.get('deletionTimestamp'):
        logger.info(f"Skipping reconcile for deleting instance {name}")
        return
        
    # Lazy creation: we do NOT create the pod here anymore.
    # The pod will be created on demand when the user connects.
    logger.info(f"Reconciling instance {name} (lazy creation mode)")

@kopf.on.delete('whistler.example.com', 'v1', 'whistlerinstances')
def delete_fn(spec, name, namespace, logger, **kwargs):
    logger.info(f"Deleting instance {name}")
    # Explicitly delete pod to ensure cleanup, even though GC should handle it
    api = client.CoreV1Api()
    try:
        pod_name = name # We assume pod name == instance name
        api.delete_namespaced_pod(pod_name, namespace)
        logger.info(f"Explicitly deleted pod {pod_name}")
    except client.rest.ApiException as e:
        if e.status != 404:
            logger.warning(f"Failed to delete pod {name}: {e}")
