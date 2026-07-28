# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner

from qq_lib.clear.clearer import CFG, _ClearResult
from qq_lib.clear.cli import Clearer, clear
from qq_lib.core.error import QQError
from qq_lib.properties.states import RealState


def test_clear_result_defaults_to_zero():
    result = _ClearResult(detected=1)
    assert result.detected == 1
    assert result.deleted == 0
    assert result.excluded == 0

    assert result.excluded == 0


def test_clearer_init_sets_directories():
    dirs = [Path("/fake/path1"), Path("/fake/path2")]
    clearer = Clearer(dirs)
    assert clearer._directories == dirs


def test_clearer_delete_files_deletes_all_files():
    mock_file1 = Mock(spec=Path)
    mock_file2 = Mock(spec=Path)

    Clearer._delete_files([mock_file1, mock_file2])

    mock_file1.unlink.assert_called_once()
    mock_file2.unlink.assert_called_once()


def test_clearer_delete_files_empty_list():
    Clearer._delete_files([])


def test_clearer_collect_runtime_files_returns_files_from_helper(tmp_path):
    expected_files = [
        tmp_path / f"a{CFG.suffixes.qq_info}",
        tmp_path / f"b{CFG.suffixes.qq_out}",
    ]

    with patch(
        "qq_lib.clear.clearer.get_runtime_files", return_value=expected_files
    ) as mock_get:
        result = Clearer._collect_runtime_files(tmp_path)

    mock_get.assert_called_once_with(tmp_path)
    assert result == set(expected_files)


@pytest.mark.parametrize("state", list(RealState))
def test_clearer_collect_excluded_files(tmp_path, state):
    dummy_info_file = tmp_path / f"job{CFG.suffixes.qq_info}"
    dummy_info_file.touch()

    dummy_stdout = f"stdout{CFG.suffixes.stdout}"
    dummy_stderr = f"stderr{CFG.suffixes.stderr}"
    dummy_job_name = "job"

    mock_informer = MagicMock()
    mock_informer.get_real_state.return_value = state
    mock_informer.info.stdout_file = dummy_stdout
    mock_informer.info.stderr_file = dummy_stderr
    mock_informer.info.job_name = dummy_job_name

    with (
        patch("qq_lib.clear.clearer.get_info_files", return_value=[dummy_info_file]),
        patch("qq_lib.clear.clearer.Informer.from_file", return_value=mock_informer),
    ):
        result = Clearer._collect_excluded_files(tmp_path)

    if state in {
        RealState.KILLED,
        RealState.FAILED,
        RealState.IN_AN_INCONSISTENT_STATE,
    }:
        assert result == set()
    else:
        assert result == {
            dummy_info_file,
            tmp_path / dummy_stdout,
            tmp_path / dummy_stderr,
            (tmp_path / dummy_job_name).with_suffix(CFG.suffixes.qq_out),
        }


def test_clearer_collect_excluded_files_ignores_files_that_raise_qqerror(tmp_path):
    dummy_info_file = tmp_path / f"bad{CFG.suffixes.qq_info}"
    dummy_info_file.touch()

    with (
        patch("qq_lib.clear.clearer.get_info_files", return_value=[dummy_info_file]),
        patch(
            "qq_lib.clear.clearer.Informer.from_file",
            side_effect=QQError("cannot read file"),
        ),
    ):
        result = Clearer._collect_excluded_files(tmp_path)

    assert result == set()


def test_clearer_clear_directory_returns_empty_result_when_no_files(tmp_path):
    with patch.object(Clearer, "_collect_runtime_files", return_value=set()):
        result = Clearer._clear_directory(tmp_path, force=False)

    assert result.deleted == 0
    assert result.excluded == 0


def test_clearer_clear_directory_deletes_only_safe_files(tmp_path):
    safe_file = tmp_path / f"safe{CFG.suffixes.qq_out}"
    excluded_file = tmp_path / f"excluded{CFG.suffixes.qq_out}"

    with (
        patch.object(
            Clearer, "_collect_runtime_files", return_value={safe_file, excluded_file}
        ),
        patch.object(Clearer, "_collect_excluded_files", return_value={excluded_file}),
        patch.object(Clearer, "_delete_files") as mock_delete,
    ):
        result = Clearer._clear_directory(tmp_path, force=False)

    mock_delete.assert_called_once_with({safe_file})
    assert result.deleted == 1
    assert result.excluded == 1


def test_clearer_clear_directory_force_skips_excluded(tmp_path):
    file1 = tmp_path / f"file1{CFG.suffixes.qq_out}"
    file2 = tmp_path / f"file2{CFG.suffixes.qq_out}"

    with (
        patch.object(Clearer, "_collect_runtime_files", return_value={file1, file2}),
        patch.object(Clearer, "_collect_excluded_files") as mock_excluded,
        patch.object(Clearer, "_delete_files") as mock_delete,
    ):
        result = Clearer._clear_directory(tmp_path, force=True)

    mock_excluded.assert_not_called()
    mock_delete.assert_called_once_with({file1, file2})
    assert result.deleted == 2
    assert result.excluded == 0


def test_clearer_clear_directory_all_excluded_deletes_nothing(tmp_path):
    excluded1 = tmp_path / f"excluded1{CFG.suffixes.qq_out}"
    excluded2 = tmp_path / f"excluded2{CFG.suffixes.qq_out}"

    with (
        patch.object(
            Clearer, "_collect_runtime_files", return_value={excluded1, excluded2}
        ),
        patch.object(
            Clearer, "_collect_excluded_files", return_value={excluded1, excluded2}
        ),
        patch.object(Clearer, "_delete_files") as mock_delete,
    ):
        result = Clearer._clear_directory(tmp_path, force=False)

    mock_delete.assert_not_called()
    assert result.deleted == 0
    assert result.excluded == 2


def test_clearer_clear_logs_nothing_to_clear_when_all_directories_empty(tmp_path):
    clearer = Clearer([tmp_path])

    with (
        patch.object(
            Clearer,
            "_clear_directory",
            return_value=_ClearResult(detected=0, deleted=0, excluded=0),
        ),
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear()

    messages = [c.args[0] for c in mock_info.call_args_list]
    assert any("Nothing to clear" in msg for msg in messages)


def test_clearer_clear_logs_removed_count(tmp_path):
    clearer = Clearer([tmp_path])

    with (
        patch.object(
            Clearer,
            "_clear_directory",
            return_value=_ClearResult(detected=3, deleted=3, excluded=0),
        ),
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear()

    messages = [c.args[0] for c in mock_info.call_args_list]
    assert any("Removed 3 qq files" in msg for msg in messages)


def test_clearer_clear_logs_excluded_count_equal_to_detected(tmp_path):
    clearer = Clearer([tmp_path])

    with (
        patch.object(
            Clearer,
            "_clear_directory",
            return_value=_ClearResult(detected=2, deleted=0, excluded=2),
        ),
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear()

    messages = [c.args[0] for c in mock_info.call_args_list]
    assert any("2 qq files could not be safely cleared" in msg for msg in messages)


def test_clearer_clear_logs_excluded_count_lower_than_detected(tmp_path):
    clearer = Clearer([tmp_path])

    with (
        patch.object(
            Clearer,
            "_clear_directory",
            return_value=_ClearResult(detected=4, deleted=0, excluded=2),
        ),
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear()

    messages = [c.args[0] for c in mock_info.call_args_list]
    assert any("2 qq files could not be safely cleared" in msg for msg in messages)


def test_clearer_clear_logs_excluded_count_higher_than_detected(tmp_path):
    clearer = Clearer([tmp_path])

    with (
        patch.object(
            Clearer,
            "_clear_directory",
            return_value=_ClearResult(detected=2, deleted=0, excluded=4),
        ),
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear()

    messages = [c.args[0] for c in mock_info.call_args_list]
    assert any("2 qq files could not be safely cleared" in msg for msg in messages)


def test_clearer_clear_logs_combined_summary_across_directories(tmp_path):
    dir1 = tmp_path / "a"
    dir2 = tmp_path / "b"
    dir1.mkdir()
    dir2.mkdir()
    clearer = Clearer([dir1, dir2])

    with (
        patch.object(
            Clearer,
            "_clear_directory",
            side_effect=[
                _ClearResult(detected=3, deleted=2, excluded=1),
                _ClearResult(detected=3, deleted=1, excluded=2),
            ],
        ),
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear()

    messages = [c.args[0] for c in mock_info.call_args_list]
    assert any("Removed 3 qq files" in msg for msg in messages)
    assert any("3 qq files could not be safely cleared" in msg for msg in messages)


def test_clearer_clear_logs_combined_summary_across_directories_excluded_larger_than_detected(
    tmp_path,
):
    dir1 = tmp_path / "a"
    dir2 = tmp_path / "b"
    dir1.mkdir()
    dir2.mkdir()
    clearer = Clearer([dir1, dir2])

    with (
        patch.object(
            Clearer,
            "_clear_directory",
            side_effect=[
                _ClearResult(detected=1, deleted=0, excluded=4),
                _ClearResult(detected=2, deleted=0, excluded=4),
            ],
        ),
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear()

    messages = [c.args[0] for c in mock_info.call_args_list]
    assert any("3 qq files could not be safely cleared" in msg for msg in messages)


def test_clearer_clear_calls_clear_directory_for_each_directory(tmp_path):
    dir1 = tmp_path / "a"
    dir2 = tmp_path / "b"
    dir1.mkdir()
    dir2.mkdir()
    clearer = Clearer([dir1, dir2])

    with patch.object(
        Clearer, "_clear_directory", return_value=_ClearResult(detected=4)
    ) as mock_clear:
        clearer.clear()

    assert mock_clear.call_count == 2
    mock_clear.assert_any_call(dir1, False)
    mock_clear.assert_any_call(dir2, False)


def test_clearer_clear_passes_force_to_clear_directory(tmp_path):
    clearer = Clearer([tmp_path])

    with patch.object(
        Clearer, "_clear_directory", return_value=_ClearResult(detected=4)
    ) as mock_clear:
        clearer.clear(force=True)

    mock_clear.assert_called_once_with(tmp_path, True)


def test_clearer_clear_singular_file_grammar(tmp_path):
    clearer = Clearer([tmp_path])

    with (
        patch.object(
            Clearer,
            "_clear_directory",
            return_value=_ClearResult(detected=2, deleted=1, excluded=1),
        ),
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear()

    messages = [c.args[0] for c in mock_info.call_args_list]
    assert any("Removed 1 qq file." in msg for msg in messages)
    assert any("1 qq file could not" in msg for msg in messages)
    assert any("to clear it forcibly" in msg for msg in messages)


def test_clear_runs_successfully():
    runner = CliRunner()

    with patch("qq_lib.clear.cli.Clearer") as mock_cls:
        result = runner.invoke(clear, [])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with([Path()])
    mock_cls.return_value.clear.assert_called_once_with(False)


def test_clear_uses_current_directory_by_default():
    runner = CliRunner()

    with patch("qq_lib.clear.cli.Clearer") as mock_cls:
        runner.invoke(clear, [])

    mock_cls.assert_called_once_with([Path()])


def test_clear_with_single_dir_flag(tmp_path):
    runner = CliRunner()

    with patch("qq_lib.clear.cli.Clearer") as mock_cls:
        result = runner.invoke(clear, ["-d", str(tmp_path)])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with([tmp_path])


def test_clear_with_multiple_dir_flags(tmp_path):
    dir1 = tmp_path / "a"
    dir2 = tmp_path / "b"
    dir1.mkdir()
    dir2.mkdir()
    runner = CliRunner()

    with patch("qq_lib.clear.cli.Clearer") as mock_cls:
        result = runner.invoke(clear, ["-d", str(dir1), "-d", str(dir2)])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with([dir1, dir2])


def test_clear_with_force_flag():
    runner = CliRunner()

    with patch("qq_lib.clear.cli.Clearer") as mock_cls:
        result = runner.invoke(clear, ["--force"])

    assert result.exit_code == 0
    mock_cls.return_value.clear.assert_called_once_with(True)


def test_clear_with_dir_and_force(tmp_path):
    runner = CliRunner()

    with patch("qq_lib.clear.cli.Clearer") as mock_cls:
        result = runner.invoke(clear, ["-d", str(tmp_path), "--force"])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with([tmp_path])
    mock_cls.return_value.clear.assert_called_once_with(True)


def test_clear_qqerror_exits_with_default_code():
    runner = CliRunner()

    with (
        patch("qq_lib.clear.cli.Clearer") as mock_cls,
        patch("qq_lib.clear.cli.logger") as mock_logger,
    ):
        mock_cls.return_value.clear.side_effect = QQError("some error")
        result = runner.invoke(clear, [])

    assert result.exit_code == CFG.exit_codes.default
    mock_logger.error.assert_called_once()


def test_clear_unexpected_exception_exits_with_unexpected_code():
    runner = CliRunner()

    with (
        patch("qq_lib.clear.cli.Clearer") as mock_cls,
        patch("qq_lib.clear.cli.logger") as mock_logger,
    ):
        mock_cls.return_value.clear.side_effect = RuntimeError("unexpected")
        result = runner.invoke(clear, [])

    assert result.exit_code == CFG.exit_codes.unexpected_error
    mock_logger.critical.assert_called_once()
