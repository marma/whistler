"""build_gpu_catalog: the GPU-type catalog derived from live cluster data.

No static gpuTypes config exists anymore — the catalog comes from each node's
GFD product label + allocatable resources, with the KubeVirt CR's
permittedHostDevices deciding which product-specific resources are GPUs at
all. These tests pin the shapes seen on real clusters: a pod-mode node, a
vfio passthrough node (whose GPU also advertises its audio function as a
separate allocatable resource), and mixes of both."""
from whistler.config import build_gpu_catalog


# The wkstn node under NVIDIA GPU Operator vm-passthrough, verbatim shapes.
PASSTHROUGH_NODE = {
    "name": "wkstn",
    "labels": {"nvidia.com/gpu.product": "NVIDIA-GeForce-RTX-4090"},
    "allocatable": {
        "cpu": "32",
        "nvidia.com/gpu": "0",
        "nvidia.com/AD102_GEFORCE_RTX_4090": "1",
        "nvidia.com/AD102_HIGH_DEFINITION_AUDIO_CONTROLLER": "1",
    },
}
PERMITTED = {"nvidia.com/AD102_GEFORCE_RTX_4090"}

POD_MODE_NODE = {
    "name": "gpu1",
    "labels": {"nvidia.com/gpu.product": "NVIDIA-A100-SXM4-40GB"},
    "allocatable": {"nvidia.com/gpu": "4"},
}


def test_passthrough_node_counts_vfio_resource_not_pod_resource():
    catalog = build_gpu_catalog([PASSTHROUGH_NODE], PERMITTED)
    assert catalog == [{
        "name": "NVIDIA-GeForce-RTX-4090",
        "count": 1,
        "vmResource": "nvidia.com/AD102_GEFORCE_RTX_4090",
    }]


def test_audio_function_is_not_a_gpu():
    # The card's audio controller is allocatable right next to the GPU but is
    # not KubeVirt-permitted; counting it would double the GPU total.
    catalog = build_gpu_catalog([PASSTHROUGH_NODE],
                                PERMITTED | {"some.other/permitted-thing"})
    assert catalog[0]["count"] == 1


def test_pod_mode_node_has_count_but_no_vm_resource():
    catalog = build_gpu_catalog([POD_MODE_NODE], set())
    assert catalog == [{"name": "NVIDIA-A100-SXM4-40GB", "count": 4,
                        "vmResource": None}]


def test_mixed_cluster_yields_one_entry_per_type_sorted():
    catalog = build_gpu_catalog([POD_MODE_NODE, PASSTHROUGH_NODE], PERMITTED)
    assert [e["name"] for e in catalog] == [
        "NVIDIA-A100-SXM4-40GB", "NVIDIA-GeForce-RTX-4090"]


def test_same_type_sums_across_nodes():
    second = dict(PASSTHROUGH_NODE, name="wkstn2")
    catalog = build_gpu_catalog([PASSTHROUGH_NODE, second], PERMITTED)
    assert catalog == [{
        "name": "NVIDIA-GeForce-RTX-4090",
        "count": 2,
        "vmResource": "nvidia.com/AD102_GEFORCE_RTX_4090",
    }]


def test_unlabeled_node_is_not_a_selectable_type():
    # No gpu.product label -> nothing a template's nodeSelector could name.
    # (The dashboard still counts such capacity, as "unknown", via
    # get_cluster_resources — that path doesn't go through the catalog.)
    node = {"name": "n1", "labels": {}, "allocatable": {"nvidia.com/gpu": "2"}}
    assert build_gpu_catalog([node], set()) == []


def test_gpuless_labeled_node_is_listed_with_zero_count():
    # Label present, no capacity right now (e.g. device plugin restarting):
    # the type stays selectable, capacity reads 0.
    node = {"name": "n1",
            "labels": {"nvidia.com/gpu.product": "NVIDIA-GeForce-RTX-4090"},
            "allocatable": {}}
    assert build_gpu_catalog([node], PERMITTED) == [
        {"name": "NVIDIA-GeForce-RTX-4090", "count": 0, "vmResource": None}]


def test_no_kubevirt_means_no_vm_resources():
    catalog = build_gpu_catalog([PASSTHROUGH_NODE], set())
    # The vfio resource is still allocatable, but without permittedHostDevices
    # nothing can attach it — so it must not count as capacity either.
    assert catalog == [{"name": "NVIDIA-GeForce-RTX-4090", "count": 0,
                        "vmResource": None}]
