import kopf
import kubernetes
import yaml
from kubernetes import client, config

def ensure_pod(spec, name, namespace, logger, **kwargs):
    logger.info(f"Ensuring pod for instance {name}")
    
    template_ref = spec.get('templateRef')
    user = spec.get('user')
    
    # TODO: Fetch template details to get image, resources, etc.
    # For now, we'll just create a dummy pod to prove the operator works
    
    pod_name = f"{user}-{name}"
    
    pod_body = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "labels": {
                "app": "whistler-instance",
                "instance": name,
                "user": user
            }
        },
        "spec": {
            "containers": [
                {
                    "name": "main",
                    "image": "ubuntu:latest", # Placeholder
                    "command": ["sleep", "3600"]
                }
            ]
        }
    }
    
    # Adopt the pod so it gets deleted when the WhistlerInstance is deleted
    kopf.adopt(pod_body)
    
    api = client.CoreV1Api()
    try:
        api.create_namespaced_pod(namespace, pod_body)
        logger.info(f"Pod {pod_name} created")
    except client.rest.ApiException as e:
        if e.status == 409:
            # Check if terminating
            try:
                existing_pod = api.read_namespaced_pod(pod_name, namespace)
                if existing_pod.metadata.deletion_timestamp:
                     logger.info(f"Pod {pod_name} is terminating. Waiting...")
                     raise kopf.TemporaryError("Pod is terminating", delay=2)
            except client.rest.ApiException:
                # Pod might have vanished in the meantime
                pass
                
            logger.info(f"Pod {pod_name} already exists")
        else:
            logger.error(f"Failed to create pod: {e}")
            raise kopf.PermanentError(f"Failed to create pod: {e}")

@kopf.on.create('whistler.io', 'v1', 'whistlerinstances')
@kopf.on.update('whistler.io', 'v1', 'whistlerinstances')
@kopf.on.resume('whistler.io', 'v1', 'whistlerinstances')
def reconcile_fn(spec, name, namespace, logger, **kwargs):
    ensure_pod(spec, name, namespace, logger, **kwargs)

@kopf.on.delete('whistler.io', 'v1', 'whistlerinstances')
def delete_fn(spec, name, logger, **kwargs):
    logger.info(f"Deleting instance {name}")
    # Pod deletion is handled by k8s garbage collection (ownerReferences)
