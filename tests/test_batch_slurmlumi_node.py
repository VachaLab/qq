# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from qq_lib.batch.slurmlumi.node import SlurmLumiNode


def make_node_with_info(info: dict[str, str]) -> SlurmLumiNode:
    node = SlurmLumiNode.__new__(SlurmLumiNode)
    node._info = info
    node._name = info.get("NodeName", "node1")
    return node


def test_slurm_node_get_ncpus_returns_correct_value_halved():
    node = make_node_with_info({"CPUTot": "128"})
    assert node.get_n_cpus() == 64


def test_slurm_node_get_nfree_cpus_computes_difference_halved():
    node = make_node_with_info({"CPUTot": "128", "CPUAlloc": "64"})
    assert node.get_n_free_cpus() == 32


def test_slurm_node_get_nfree_cpus_missing_alloc():
    node = make_node_with_info({"CPUTot": "128", "CPUAlloc": "0"})
    assert node.get_n_free_cpus() == 64
