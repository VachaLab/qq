# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import pytest

from qq_lib.properties.resubmit_host import (
    ExplicitHost,
    InputHost,
    ResubmitHost,
    WorkHost,
)


@pytest.mark.parametrize(
    "input_str, expected_type",
    [
        ("input", InputHost),
        ("INPUT", InputHost),
        ("  input  ", InputHost),
        ("working", WorkHost),
        ("work", WorkHost),
        ("WORKING", WorkHost),
        ("  work  ", WorkHost),
    ],
)
def test_resubmit_host_from_str_keyword_mappings(input_str, expected_type):
    result = ResubmitHost.from_str(input_str)
    assert isinstance(result, expected_type)


@pytest.mark.parametrize(
    "input_str, expected_hostname",
    [
        ("node001", "node001"),
        ("node132.random.server.org", "node132.random.server.org"),
        ("  node001  ", "node001"),
        ("my-host", "my-host"),
        ("192.168.1.1", "192.168.1.1"),
    ],
)
def test_resubmit_host_from_str_explicit_hosts(input_str, expected_hostname):
    result = ResubmitHost.from_str(input_str)
    assert isinstance(result, ExplicitHost)
    assert result.hostname == expected_hostname


@pytest.mark.parametrize(
    "raw, expected_types",
    [
        ("input:node132", [InputHost, ExplicitHost]),
        ("work,input", [WorkHost, InputHost]),
        ("node123 node234", [ExplicitHost, ExplicitHost]),
        ("input:working:node001", [InputHost, WorkHost, ExplicitHost]),
        ("input,working,node001", [InputHost, WorkHost, ExplicitHost]),
        ("input working node001", [InputHost, WorkHost, ExplicitHost]),
        ("input, working, node001", [InputHost, WorkHost, ExplicitHost]),
        ("input : working : node001", [InputHost, WorkHost, ExplicitHost]),
    ],
)
def test_multi_from_str_valid_inputs(raw, expected_types):
    hosts = ResubmitHost.multi_from_str(raw)

    assert len(hosts) == len(expected_types)
    for host, expected_type in zip(hosts, expected_types):
        assert isinstance(host, expected_type)


@pytest.mark.parametrize(
    "raw, expected_count",
    [
        ("input:::working", 2),
        ("input,,,working", 2),
        ("  input   working  ", 2),
        ("  input  ", 1),
        ("  input,  ", 1),
    ],
)
def test_multi_from_str_robust_splitting(raw, expected_count):
    hosts = ResubmitHost.multi_from_str(raw)
    assert len(hosts) == expected_count
    assert all(isinstance(h, ResubmitHost) for h in hosts)


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        ",,,",
        "::::",
        "   ,  : ,",
    ],
)
def test_multi_from_str_empty_or_whitespace_input(raw):
    assert ResubmitHost.multi_from_str(raw) == []


def test_input_host_to_str():
    assert InputHost().to_str() == "input"


def test_input_host_resolve():
    host = InputHost()
    assert (
        host.resolve("submit-node.example.com", "compute-node-01")
        == "submit-node.example.com"
    )


def test_work_host_to_str():
    assert WorkHost().to_str() == "working"


def test_work_host_resolve():
    host = WorkHost()
    assert (
        host.resolve("submit-node.example.com", "compute-node-01") == "compute-node-01"
    )


def test_explicit_host_to_str():
    host = ExplicitHost("node132.random.server.org")
    assert host.to_str() == "node132.random.server.org"


def test_explicit_host_resolve_ignores_both_arguments():
    host = ExplicitHost("my-explicit-host")
    assert host.resolve("submit-node", "compute-node") == "my-explicit-host"
