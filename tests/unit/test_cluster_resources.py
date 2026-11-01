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


def test_hugepages_are_their_own_meter_per_page_size():
    # A hugepage-backed VM requests its guest RAM as hugepages-<size> and only
    # KubeVirt's overhead as memory, so without this meter it barely registers.
    nodes = [
        {"memory": "64Gi", "hugepages": {"2Mi": "32Gi", "1Gi": "0"}},
        {"memory": "64Gi", "hugepages": {"2Mi": "16Gi", "1Gi": "8Gi"}},
    ]
    pod_requests = [
        {"bucket": "whistler", "memory": "300Mi", "hugepages": {"2Mi": Decimal(16 * 2**30)}},
        {"bucket": "whistlerPreemptible", "hugepages": {"1Gi": Decimal(4 * 2**30)}},
    ]
    result = _manager()._summarize_cluster_resources(nodes, pod_requests)
    hp = {h["size"]: h for h in result["hugepages"]}
    assert [h["size"] for h in result["hugepages"]] == ["2Mi", "1Gi"]
    assert hp["2Mi"]["total"] == 48 * 2**30
    assert hp["2Mi"]["whistler"] == 16 * 2**30
    assert hp["2Mi"]["free"] == 32 * 2**30
    assert hp["1Gi"]["whistlerPreemptible"] == 4 * 2**30
    assert hp["1Gi"]["free"] == 4 * 2**30
    assert result["memory"]["whistler"] == 300 * 2**20


def test_unreserved_hugepage_sizes_are_left_out():
    # Every x86 node advertises hugepages-1Gi: 0 and hugepages-2Mi: 0.
    nodes = [{"hugepages": {"2Mi": "0", "1Gi": "0"}}]
    assert _manager()._summarize_cluster_resources(nodes, [])["hugepages"] == []


def test_hugepages_requested_with_no_pool_still_show():
    # A pod requesting a size no node reserves is the case worth seeing: it pends.
    pod_requests = [{"bucket": "whistler", "hugepages": {"2Mi": "8Gi"}}]
    hp = _manager()._summarize_cluster_resources([], pod_requests)["hugepages"]
    assert hp[0]["size"] == "2Mi" and hp[0]["total"] == 0 and hp[0]["whistler"] == 8 * 2**30


def test_hugepage_quantities_picks_out_the_pools():
    from whistler.config import _hugepage_quantities
    assert _hugepage_quantities({"cpu": "4", "memory": "1Gi", "hugepages-2Mi": "2Gi",
                                 "hugepages-1Gi": "0"}) == {"2Mi": "2Gi", "1Gi": "0"}
    assert _hugepage_quantities(None) == {}


# --- node disk (kubelet /stats/summary) ------------------------------------ #

def _stats(capacity, available, pods=()):
    return {"node": {"fs": {"capacityBytes": capacity,
                            "availableBytes": available}},
            "pods": [{"podRef": {"namespace": ns, "name": name},
                      "ephemeral-storage": {"usedBytes": used}}
                     for ns, name, used in pods]}


def test_node_disk_splits_whistler_pods_from_everything_else():
    G = 2**30
    stats = {"n1": _stats(1000 * G, 300 * G, [
        ("whistler-alice", "virt-launcher-box-abcde", 40 * G),
        ("whistler-bob", "virt-launcher-spot-fghij", 10 * G),
        ("kube-system", "coredns-1", 1 * G),
    ])}
    buckets = {("whistler-alice", "virt-launcher-box-abcde"): "whistler",
               ("whistler-bob", "virt-launcher-spot-fghij"): "whistlerPreemptible",
               ("kube-system", "coredns-1"): "other"}
    (d,) = KubeConfigManager._summarize_node_disks(stats, buckets)
    assert d == {"node": "n1", "total": 1000 * G, "free": 300 * G,
                 "whistler": 40 * G, "whistlerPreemptible": 10 * G,
                 # Used but not Whistler's: OS, images, other pods.
                 "other": 650 * G}


def test_node_disk_is_per_node_and_an_unreadable_node_keeps_its_row():
    stats = {"b": _stats(100, 50), "a": "URLError: timed out"}
    rows = KubeConfigManager._summarize_node_disks(stats, {})
    assert [r["node"] for r in rows] == ["a", "b"]
    assert rows[0] == {"node": "a", "error": "URLError: timed out"}
    assert rows[1]["free"] == 50 and rows[1]["other"] == 50


def test_node_disk_pods_without_a_known_bucket_count_as_other():
    # A pod that started after the pod list was read is not Whistler's yet.
    stats = {"n": _stats(100, 40, [("x", "new-pod", 20)])}
    (d,) = KubeConfigManager._summarize_node_disks(stats, {})
    assert d["whistler"] == 0 and d["other"] == 60


def test_dashboard_disk_view_carries_errors_and_free_space():
    from whistler.portal.management import _dashboard_resource_views
    G = 2**30
    zero = {"total": 0, "free": 0, "whistler": 0,
            "whistlerPreemptible": 0, "other": 0}
    view = _dashboard_resource_views({
        "cpu": zero, "memory": zero, "gpus": [],
        "disks": [{"node": "a", "error": "no"},
                  {"node": "b", "total": 100 * G, "free": 25 * G,
                   "whistler": 5 * G, "whistlerPreemptible": 0,
                   "other": 70 * G}]})
    assert view["disks"][0] == {"node": "a", "error": "no"}
    b = view["disks"][1]
    assert b["freeGiB"] == 25.0 and b["used"] == 75.0 and b["unit"] == "GiB"
