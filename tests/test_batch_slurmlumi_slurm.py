# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import getpass
import os
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from qq_lib.batch.slurm import SlurmNode
from qq_lib.batch.slurmlumi.node import SlurmLumiNode
from qq_lib.batch.slurmlumi.slurm import SlurmLumi
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError


@pytest.mark.parametrize("which_return,expected", [(None, False), ("some/path", True)])
def test_slurmlumi_is_available_depends_on_shutil(which_return, expected):
    with patch("qq_lib.batch.slurmlumi.slurm.shutil.which", return_value=which_return):
        assert SlurmLumi.is_available() is expected


@pytest.mark.parametrize("uses_scratch,expect_env_var", [(True, True), (False, False)])
def test_slurmlumi_job_submit_sets_env_var_conditionally(uses_scratch, expect_env_var):
    res = Mock()
    res.uses_scratch.return_value = uses_scratch
    res.work_dir = "flash"
    env_vars = {}

    with patch(
        "qq_lib.batch.slurmlumi.slurm.SlurmIT4I.job_submit", return_value="JOB123"
    ) as mock_super:
        SlurmLumi.job_submit(
            res, "default", Path("script.sh"), "job", [], env_vars, None
        )

    if expect_env_var:
        assert env_vars[CFG.env_vars.lumi_scratch_type] == "flash"
    else:
        assert not env_vars
    mock_super.assert_called_once()


def test_slurmlumi_create_work_dir_on_scratch_raises_when_no_account(monkeypatch):
    monkeypatch.setattr(os, "environ", {})
    with pytest.raises(QQError, match="No account is defined for job '111'"):
        SlurmLumi.create_work_dir_on_scratch("111")


def test_slurmlumi_create_work_dir_on_scratch_raises_when_no_storage_type(monkeypatch):
    monkeypatch.setattr(os, "environ", {CFG.env_vars.slurm_job_account: "account"})
    with pytest.raises(
        QQError,
        match=f"Environment variable '{CFG.env_vars.lumi_scratch_type}' is not defined",
    ):
        SlurmLumi.create_work_dir_on_scratch("222")


def test_slurmlumi_create_work_dir_on_scratch_creates_directory(monkeypatch):
    os.environ[CFG.env_vars.slurm_job_account] = "account"
    os.environ[CFG.env_vars.lumi_scratch_type] = "scratch"
    monkeypatch.setattr(getpass, "getuser", lambda: "user")

    with patch.object(Path, "mkdir") as mock_mkdir:
        result = SlurmLumi.create_work_dir_on_scratch("333")

    assert isinstance(result, Path)
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    assert Path("/scratch/account/user/qq-jobs/job_333") == result


def test_slurmlumi_create_work_dir_on_scratch_raises_on_creation_error(monkeypatch):
    os.environ[CFG.env_vars.slurm_job_account] = "account"
    os.environ[CFG.env_vars.lumi_scratch_type] = "flash"
    monkeypatch.setattr(getpass, "getuser", lambda: "user")

    with (
        patch.object(Path, "mkdir", side_effect=Exception("fail")) as mock_mkdir,
        pytest.raises(
            QQError, match="Could not create a working directory on flash for job '444'"
        ),
    ):
        SlurmLumi.create_work_dir_on_scratch("444")

    assert mock_mkdir.call_count == CFG.slurm_lumi_options.scratch_dir_attempts


@patch("qq_lib.batch.slurmlumi.slurm.getpass.getuser", return_value="userX")
@patch.dict(
    os.environ,
    {CFG.env_vars.slurm_job_account: "ACCT", CFG.env_vars.lumi_scratch_type: "scratch"},
    clear=True,
)
def test_slurmlumi_create_work_dir_on_scratch_third_attempt_succeeds(mock_user):
    mkdir_mock = MagicMock()
    mkdir_mock.side_effect = [
        OSError("fail 1"),
        OSError("fail 2"),
        None,  # third attempt succeeds
    ]

    with patch("qq_lib.batch.slurmlumi.slurm.Path.mkdir", mkdir_mock):
        result = SlurmLumi.create_work_dir_on_scratch("999")

    expected_path = "/scratch/acct/userX3/qq-jobs/job_999"
    assert str(result).endswith(expected_path)

    mock_user.assert_called_once()
    assert mkdir_mock.call_count == 3


def test_slurmlumi_get_supported_work_dir_types_returns_combined_list():
    expected = ["scratch", "flash", "input_dir", "job_dir"]
    assert SlurmLumi.get_supported_work_dir_types() == expected


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch("qq_lib.batch.slurm.slurm.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.slurm.SlurmNode.from_dict")
def test_slurmlumi_get_nodes_success(mock_from_dict, mock_parser, mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "NodeName=node1 Arch=x86_64\nNodeName=node2 Arch=x86_64"
    mock_run.return_value = mock_result

    mock_parser.side_effect = [
        {"NodeName": "node1", "Arch": "x86_64"},
        {"NodeName": "node2", "Arch": "x86_64"},
    ]

    mock_node1 = MagicMock(spec=SlurmNode)
    mock_node2 = MagicMock(spec=SlurmNode)
    mock_from_dict.side_effect = [mock_node1, mock_node2]

    result = SlurmLumi.get_nodes()

    mock_run.assert_called_once_with(
        ["bash"],
        input="scontrol show node -o",
        text=True,
        check=False,
        capture_output=True,
        errors="replace",
    )

    assert mock_parser.call_count == 2
    mock_from_dict.assert_any_call("node1", {"NodeName": "node1", "Arch": "x86_64"})
    mock_from_dict.assert_any_call("node2", {"NodeName": "node2", "Arch": "x86_64"})

    assert result == [mock_node1, mock_node2]
    assert isinstance(result[0], SlurmLumiNode)
    assert isinstance(result[1], SlurmLumiNode)
