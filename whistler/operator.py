import kopf
import kubernetes
import yaml
from kubernetes import client, config

def ensure_pvc(user, namespace, logger):
    pvc_name = f"whistler-data-{user}"
    api = client.CoreV1Api()
    
    try:
        api.read_namespaced_persistent_volume_claim(pvc_name, namespace)
        # logger.info(f"PVC {pvc_name} already exists")
        return pvc_name
    except client.rest.ApiException as e:
        if e.status != 404:
            raise kopf.PermanentError(f"Failed to check PVC: {e}")

    # Create PVC
    logger.info(f"Creating PVC {pvc_name} for user {user}")
    pvc_body = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": pvc_name,
            "labels": {
                "app": "whistler",
                "user": user
            }
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "10Gi"
                }
            }
        }
    }
    
    try:
        api.create_namespaced_persistent_volume_claim(namespace, pvc_body)
        logger.info(f"PVC {pvc_name} created")
        return pvc_name
    except client.rest.ApiException as e:
        raise kopf.PermanentError(f"Failed to create PVC: {e}")

def ensure_pod(spec, name, namespace, logger, **kwargs):
    logger.info(f"Ensuring pod for instance {name}")
    
    template_ref = spec.get('templateRef')
    user = spec.get('user')
    preemptible = spec.get('preemptible', False)
    
    # Fetch template details
    custom_api = client.CustomObjectsApi()
    try:
        template = custom_api.get_namespaced_custom_object(
            group="whistler.io",
            version="v1",
            namespace=namespace,
            plural="whistlertemplates",
            name=template_ref
        )
        template_spec = template.get('spec', {})
    except client.rest.ApiException as e:
        if e.status == 404:
            raise kopf.TemporaryError(f"Template {template_ref} not found", delay=10)
        raise kopf.PermanentError(f"Failed to fetch template: {e}")

    image = template_spec.get('image', 'ubuntu:latest')
    resources = template_spec.get('resources', {})
    node_selector = template_spec.get('nodeSelector', {})
    
    # Construct resource requirements
    resource_reqs = {}
    if resources:
        requests = {}
        limits = {}
        
        if 'cpu' in resources:
            requests['cpu'] = resources['cpu']
            limits['cpu'] = resources['cpu']
        if 'memory' in resources:
            requests['memory'] = resources['memory']
            limits['memory'] = resources['memory']
        if 'gpu' in resources:
            limits['nvidia.com/gpu'] = resources['gpu']
            
        if requests:
            resource_reqs['requests'] = requests
        if limits:
            resource_reqs['limits'] = limits
    
    # Use CR name as pod name (it should already be unique and prefixed with user)
    pod_name = name
    hostname = name
    if name.startswith(f"{user}-"):
        hostname = name[len(user)+1:]
    
    # Ensure PVC exists
    pvc_name = ensure_pvc(user, namespace, logger)
    
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
                    "image": image,
                    "command": ["sleep", "3600"],
                    "resources": resource_reqs,
                    "volumeMounts": [
                        {
                            "name": "data",
                            "mountPath": "/data"
                        }
                    ]
                }
            ],
            "volumes": [
                {
                    "name": "data",
                    "persistentVolumeClaim": {
                        "claimName": pvc_name
                    }
                }
            ],
            "nodeSelector": node_selector,
            "hostname": hostname,
            "subdomain": "whistler" # Optional: for stable DNS if we had a service
        }
    }
    
    if preemptible:
        pod_body["spec"]["priorityClassName"] = "whistler-preemptible"
    
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
