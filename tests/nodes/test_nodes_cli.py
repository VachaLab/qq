# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.nodes.cli import nodes


def test_nodes_command_prints_available_nodes():
    runner = CliRunner()
    mock_node = MagicMock()
    mock_node.is_available_to_user.return_value = True

    with (
        patch("qq_lib.nodes.cli.BatchInterface.from_env_var_or_guess") as mock_meta,
        patch("qq_lib.nodes.cli.NodesPresenter") as mock_presenter_cls,
        patch("qq_lib.nodes.cli.Console"),
        patch("qq_lib.nodes.cli.getpass.getuser", return_value="user"),
    ):
        mock_batch = MagicMock()
        mock_batch.get_nodes.return_value = [mock_node]
        mock_meta.return_value = mock_batch

        mock_presenter = MagicMock()
        mock_presenter_cls.return_value = mock_presenter

        result = runner.invoke(nodes, [])

    assert result.exit_code == 0
    mock_meta.assert_called_once()
    mock_batch.get_nodes.assert_called_once_with(None)
    mock_presenter_cls.assert_called_once_with([mock_node], "user", False, None)
    mock_presenter.create_nodes_info_panel.assert_called_once()


def test_nodes_command_prints_available_nodes_with_server():
    runner = CliRunner()
    mock_node = MagicMock()
    mock_node.is_available_to_user.return_value = True

    with (
        patch("qq_lib.nodes.cli.BatchInterface.from_env_var_or_guess") as mock_meta,
        patch("qq_lib.nodes.cli.NodesPresenter") as mock_presenter_cls,
        patch("qq_lib.nodes.cli.Console"),
        patch("qq_lib.nodes.cli.getpass.getuser", return_value="user"),
    ):
        mock_batch = MagicMock()
        mock_batch.get_nodes.return_value = [mock_node]
        mock_meta.return_value = mock_batch

        mock_presenter = MagicMock()
        mock_presenter_cls.return_value = mock_presenter

        result = runner.invoke(nodes, ["-s", "server"])

    assert result.exit_code == 0
    mock_meta.assert_called_once()
    mock_batch.get_nodes.assert_called_once_with("server")
    mock_presenter_cls.assert_called_once_with([mock_node], "user", False, "server")
    mock_presenter.create_nodes_info_panel.assert_called_once()


def test_nodes_command_prints_all_nodes_with_flag():
    runner = CliRunner()
    mock_node = MagicMock()

    with (
        patch("qq_lib.nodes.cli.BatchInterface.from_env_var_or_guess") as mock_meta,
        patch("qq_lib.nodes.cli.NodesPresenter") as mock_presenter_cls,
        patch("qq_lib.nodes.cli.Console"),
        patch("qq_lib.nodes.cli.getpass.getuser", return_value="testuser"),
    ):
        mock_batch = MagicMock()
        mock_batch.get_nodes.return_value = [mock_node]
        mock_meta.return_value = mock_batch

        mock_presenter = MagicMock()
        mock_presenter_cls.return_value = mock_presenter

        result = runner.invoke(nodes, ["--all"])

    assert result.exit_code == 0
    mock_meta.assert_called_once()
    mock_batch.get_nodes.assert_called_once_with(None)
    mock_presenter_cls.assert_called_once_with([mock_node], "testuser", True, None)
    mock_presenter.create_nodes_info_panel.assert_called_once()


def test_nodes_command_prints_all_nodes_with_flag_with_server():
    runner = CliRunner()
    mock_node = MagicMock()

    with (
        patch("qq_lib.nodes.cli.BatchInterface.from_env_var_or_guess") as mock_meta,
        patch("qq_lib.nodes.cli.NodesPresenter") as mock_presenter_cls,
        patch("qq_lib.nodes.cli.Console"),
        patch("qq_lib.nodes.cli.getpass.getuser", return_value="testuser"),
    ):
        mock_batch = MagicMock()
        mock_batch.get_nodes.return_value = [mock_node]
        mock_meta.return_value = mock_batch

        mock_presenter = MagicMock()
        mock_presenter_cls.return_value = mock_presenter

        result = runner.invoke(nodes, ["--all", "-s", "server"])

    assert result.exit_code == 0
    mock_meta.assert_called_once()
    mock_batch.get_nodes.assert_called_once_with("server")
    mock_presenter_cls.assert_called_once_with([mock_node], "testuser", True, "server")
    mock_presenter.create_nodes_info_panel.assert_called_once()


def test_nodes_command_outputs_yaml_when_flag_set():
    runner = CliRunner()
    mock_node = MagicMock()
    mock_node.is_available_to_user.return_value = True

    with (
        patch("qq_lib.nodes.cli.BatchInterface.from_env_var_or_guess") as mock_meta,
        patch("qq_lib.nodes.cli.NodesPresenter") as mock_presenter_cls,
        patch("qq_lib.nodes.cli.getpass.getuser", return_value="testuser"),
    ):
        mock_batch = MagicMock()
        mock_batch.get_nodes.return_value = [mock_node]
        mock_meta.return_value = mock_batch

        mock_presenter = MagicMock()
        mock_presenter_cls.return_value = mock_presenter

        result = runner.invoke(nodes, ["--yaml"])

    assert result.exit_code == 0
    mock_meta.assert_called_once()
    mock_batch.get_nodes.assert_called_once_with(None)
    mock_presenter_cls.assert_called_once_with([mock_node], "testuser", False, None)
    mock_presenter.dump_yaml.assert_called_once()


def test_nodes_command_handles_qqerror_and_exits_91():
    runner = CliRunner()

    with (
        patch(
            "qq_lib.nodes.cli.BatchInterface.from_env_var_or_guess",
            side_effect=QQError("error"),
        ),
        patch("qq_lib.nodes.cli.logger") as mock_logger,
    ):
        result = runner.invoke(nodes, [])

    assert result.exit_code == CFG.exit_codes.default
    mock_logger.error.assert_called_once()


def test_nodes_command_handles_unexpected_exception_and_exits_99():
    runner = CliRunner()

    with (
        patch(
            "qq_lib.nodes.cli.BatchInterface.from_env_var_or_guess",
            side_effect=RuntimeError("fatal"),
        ),
        patch("qq_lib.nodes.cli.logger") as mock_logger,
    ):
        result = runner.invoke(nodes, [])

    assert result.exit_code == CFG.exit_codes.unexpected_error
    mock_logger.critical.assert_called_once()


def test_nodes_command_without_available_nodes_exits_zero():
    runner = CliRunner()
    mock_node = MagicMock()
    mock_node.is_available_to_user.return_value = False

    with (
        patch("qq_lib.nodes.cli.BatchInterface.from_env_var_or_guess") as mock_meta,
        patch("qq_lib.nodes.cli.getpass.getuser", return_value="user"),
    ):
        mock_meta.return_value.get_nodes.return_value = [mock_node]

        result = runner.invoke(nodes, [])

    assert result.exit_code == 0


def make_fake_spinner(events):
    """Create a replacement for `delayed_spinner` recording when it starts and stops."""

    @contextmanager
    def fake_spinner(message):
        events.append(f"start {message}")
        yield
        events.append(f"stop {message}")

    return fake_spinner


@pytest.mark.parametrize("flags", [[], ["--all"]])
def test_nodes_command_collects_nodes_with_spinner(flags):
    runner = CliRunner()
    events = []
    item = MagicMock()
    item.is_available_to_user.side_effect = lambda _user: (
        events.append("availability"),
        True,
    )[1]

    def fetch(_server):
        events.append("fetch")
        return [item]

    with (
        patch("qq_lib.nodes.cli.BatchInterface.from_env_var_or_guess") as mock_meta,
        patch("qq_lib.nodes.cli.NodesPresenter") as mock_presenter_cls,
        patch("qq_lib.nodes.cli.Console"),
        patch("qq_lib.nodes.cli.getpass.getuser", return_value="user"),
        patch("qq_lib.nodes.cli.delayed_spinner", make_fake_spinner(events)),
    ):
        mock_meta.return_value.get_nodes.side_effect = fetch
        mock_presenter_cls.return_value.create_nodes_info_panel.side_effect = (
            lambda _console: events.append("panel")
        )

        result = runner.invoke(nodes, flags)

    assert result.exit_code == 0
    collected = ["fetch"] if flags else ["fetch", "availability"]
    # the panel may query the availability, so it is created under a spinner as well
    assert events == [
        "start Collecting nodes...",
        *collected,
        "stop Collecting nodes...",
        "start Building the output table...",
        "panel",
        "stop Building the output table...",
    ]
