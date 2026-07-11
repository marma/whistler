"""Cluster resource aggregation for the Dashboard tab
(KubeConfigManager._summarize_cluster_resources) — the pure bucketing math
behind get_cluster_resources, tested without a cluster."""
from decimal import Decimal

from whistler.config import KubeConfigManager


def _manager():
    # Bypass __init__ (which loads kube config and files); the method under
    # test reads no instance state.
    return KubeConfigManager.__new__(KubeConfigManager)


def test_no_nodes_no_pods_is_all_zero():
    result = _manager()._summarize_cluster_resources([], [])
    assert result["cpu"] == {"total": Decimal(0), "free": Decimal(0),
                              "whistler": Decimal(0), "whistlerPreemptible": Decimal(0),
                              "other": Decimal(0)}
    assert result["gpus"] == []


def test_cpu_and_memory_bucketed_and_remainder_is_free():
    nodes = [{"cpu": "4", "memory": "8Gi"}, {"cpu": "4", "memory": "8Gi"}]
    pod_requests = [
        {"bucket": "whistler", "cpu": "1", "memory": "1Gi"},
        {"bucket": "whistlerPreemptible", "cpu": "2", "memory": "2Gi"},
        {"bucket": "other", "cpu": "1", "memory": "1Gi"},
    ]
    result = _manager()._summarize_cluster_resources(nodes, pod_requests)
    cpu = result["cpu"]
    assert cpu["total"] == 8
    assert cpu["whistler"] == 1
    assert cpu["whistlerPreemptible"] == 2
    assert cpu["other"] == 1
    assert cpu["free"] == 4
    mem = result["memory"]
    assert mem["total"] == 16 * 2**30
    assert mem["free"] == 12 * 2**30


def test_gpu_totals_grouped_by_type_across_nodes():
    nodes = [
        {"gpuType": "A100", "gpuCount": "2"},
        {"gpuType": "A100", "gpuCount": "1"},
        {"gpuType": "H100", "gpuCount": "4"},
    ]
    pod_requests = [
        {"bucket": "whistler", "gpuType": "A100", "gpuCount": 1},
        {"bucket": "whistlerPreemptible", "gpuType": "H100", "gpuCount": 2},
    ]
    result = _manager()._summarize_cluster_resources(nodes, pod_requests)
    gpus = {g["type"]: g for g in result["gpus"]}
    assert gpus["A100"]["total"] == 3
    assert gpus["A100"]["whistler"] == 1
    assert gpus["A100"]["free"] == 2
    assert gpus["H100"]["total"] == 4
    assert gpus["H100"]["whistlerPreemptible"] == 2
    assert gpus["H100"]["free"] == 2


def test_gpu_used_without_a_matching_node_still_reports_as_unknown_type():
    # A pod's GPU request resolved to no GPU_NODE_LABEL (unscheduled or an
    # unlabeled node) still counts against the total, just under "unknown"
    # rather than silently vanishing.
    pod_requests = [{"bucket": "other", "gpuType": None, "gpuCount": 1}]
    result = _manager()._summarize_cluster_resources([], pod_requests)
    gpus = {g["type"]: g for g in result["gpus"]}
    assert gpus["unknown"]["other"] == 1
    assert gpus["unknown"]["total"] == 0


def test_free_never_goes_negative_when_used_exceeds_total():
    # e.g. burstable requests below limits, or capacity read mid-scale-down.
    nodes = [{"cpu": "2"}]
    pod_requests = [{"bucket": "whistler", "cpu": "3"}]
    result = _manager()._summarize_cluster_resources(nodes, pod_requests)
    assert result["cpu"]["free"] == 0
