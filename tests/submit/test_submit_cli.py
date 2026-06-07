# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from unittest.mock import MagicMock, patch

from click.testing import CliRunner, Result

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.submit.cli import submit


def make_submitter_mock(
    job_id: str = "job123",
    continues_loop: bool = False,
    input_dir=None,
) -> MagicMock:
    submitter = MagicMock()
    submitter.submit.return_value = job_id
    submitter.continues_loop.return_value = continues_loop
    submitter.get_input_dir.return_value = input_dir
    return submitter


def make_factory_mock(submitter: MagicMock) -> MagicMock:
    factory = MagicMock()
    factory.make_submitter.return_value = submitter
    return factory


def invoke_submit(scripts: list[str], extra_args: list[str] | None = None) -> Result:
    runner = CliRunner()
    return runner.invoke(submit, scripts + (extra_args or []))


def test_submit_successful(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    submitter = make_submitter_mock(job_id="job123", input_dir=tmp_path)
    factory = make_factory_mock(submitter)

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch("qq_lib.submit.cli.SubmitterFactory", return_value=factory),
        patch("qq_lib.submit.cli.get_runtime_files", return_value=[]),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        result = invoke_submit([str(script)])

    assert result.exit_code == 0
    factory.make_submitter.assert_called_once()
    submitter.submit.assert_called_once()
    info_messages = [call.args[0] for call in mock_logger.info.call_args_list]
    assert any("job123" in msg for msg in info_messages)


def test_submit_detects_runtime_files_and_aborts(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    submitter = make_submitter_mock(continues_loop=False, input_dir=tmp_path)
    factory = make_factory_mock(submitter)

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch("qq_lib.submit.cli.SubmitterFactory", return_value=factory),
        patch(
            "qq_lib.submit.cli.get_runtime_files",
            return_value=[tmp_path / "file.qqout"],
        ),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        result = invoke_submit([str(script)])

    assert result.exit_code == CFG.exit_codes.default
    error_messages = [call.args[0] for call in mock_logger.error.call_args_list]
    assert any("Submission aborted" in str(msg) for msg in error_messages)
    submitter.continues_loop.assert_called_once()
    submitter.submit.assert_not_called()


def test_submit_continues_loop_even_with_runtime_files(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    submitter = make_submitter_mock(
        job_id="job_loop", continues_loop=True, input_dir=tmp_path
    )
    factory = make_factory_mock(submitter)

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch("qq_lib.submit.cli.SubmitterFactory", return_value=factory),
        patch(
            "qq_lib.submit.cli.get_runtime_files",
            return_value=[tmp_path / "file.qqout"],
        ),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        result = invoke_submit([str(script)])

    assert result.exit_code == 0
    info_messages = [call.args[0] for call in mock_logger.info.call_args_list]
    assert any("job_loop" in msg for msg in info_messages)
    submitter.submit.assert_called_once()
    submitter.continues_loop.assert_called_once()


def test_submit_generic_exception_results_in_critical_log(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    factory = MagicMock()
    factory.make_submitter.side_effect = Exception("unexpected error")

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch("qq_lib.submit.cli.SubmitterFactory", return_value=factory),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        result = invoke_submit([str(script)])

    assert result.exit_code == CFG.exit_codes.default
    critical_messages = [call.args[0] for call in mock_logger.critical.call_args_list]
    assert any("unexpected error" in str(msg) for msg in critical_messages)
    factory.make_submitter.assert_called_once()


def test_submit_single_script_does_not_log_summary(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    submitter = make_submitter_mock(job_id="job123", input_dir=tmp_path)
    factory = make_factory_mock(submitter)

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch("qq_lib.submit.cli.SubmitterFactory", return_value=factory),
        patch("qq_lib.submit.cli.get_runtime_files", return_value=[]),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        invoke_submit([str(script)])

    info_messages = [call.args[0] for call in mock_logger.info.call_args_list]
    assert not any("/" in msg for msg in info_messages)


def test_submit_multiple_scripts_all_successful(tmp_path):
    script1 = tmp_path / "script1.sh"
    script2 = tmp_path / "script2.sh"
    script1.write_text("#!/usr/bin/env -S qq run\n")
    script2.write_text("#!/usr/bin/env -S qq run\n")

    submitter1 = make_submitter_mock(job_id="job1", input_dir=tmp_path)
    submitter2 = make_submitter_mock(job_id="job2", input_dir=tmp_path)
    factory = MagicMock()
    factory.make_submitter.side_effect = [submitter1, submitter2]

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch("qq_lib.submit.cli.SubmitterFactory", return_value=factory),
        patch("qq_lib.submit.cli.get_runtime_files", return_value=[]),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        result = invoke_submit([str(script1), str(script2)])

    assert result.exit_code == 0
    info_messages = [call.args[0] for call in mock_logger.info.call_args_list]
    assert any("job1" in msg for msg in info_messages)
    assert any("job2" in msg for msg in info_messages)


def test_submit_multiple_scripts_logs_summary(tmp_path):
    script1 = tmp_path / "script1.sh"
    script2 = tmp_path / "script2.sh"
    script1.write_text("#!/usr/bin/env -S qq run\n")
    script2.write_text("#!/usr/bin/env -S qq run\n")

    submitter1 = make_submitter_mock(job_id="job1", input_dir=tmp_path)
    submitter2 = make_submitter_mock(job_id="job2", input_dir=tmp_path)
    factory = MagicMock()
    factory.make_submitter.side_effect = [submitter1, submitter2]

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch("qq_lib.submit.cli.SubmitterFactory", return_value=factory),
        patch("qq_lib.submit.cli.get_runtime_files", return_value=[]),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        invoke_submit([str(script1), str(script2)])

    info_messages = [call.args[0] for call in mock_logger.info.call_args_list]
    assert any("2/2" in msg for msg in info_messages)


def test_submit_multiple_scripts_some_fail_exits_zero(tmp_path):
    script1 = tmp_path / "script1.sh"
    script2 = tmp_path / "script2.sh"
    script1.write_text("#!/usr/bin/env -S qq run\n")
    script2.write_text("#!/usr/bin/env -S qq run\n")

    submitter1 = make_submitter_mock(job_id="job1", input_dir=tmp_path)
    factory = MagicMock()
    factory.make_submitter.side_effect = [
        submitter1,
        MagicMock(
            get_input_dir=MagicMock(return_value=tmp_path),
            continues_loop=MagicMock(return_value=False),
            make_submitter=MagicMock(side_effect=QQError("fail")),
        ),
    ]

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch(
            "qq_lib.submit.cli.SubmitterFactory",
            side_effect=[
                MagicMock(make_submitter=MagicMock(return_value=submitter1)),
                MagicMock(make_submitter=MagicMock(side_effect=QQError("fail"))),
            ],
        ),
        patch("qq_lib.submit.cli.get_runtime_files", return_value=[]),
        patch("qq_lib.submit.cli.logger"),
    ):
        result = invoke_submit([str(script1), str(script2)])

    assert result.exit_code == 0


def test_submit_multiple_scripts_all_fail_exits_default(tmp_path):
    script1 = tmp_path / "script1.sh"
    script2 = tmp_path / "script2.sh"
    script1.write_text("#!/usr/bin/env -S qq run\n")
    script2.write_text("#!/usr/bin/env -S qq run\n")

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch(
            "qq_lib.submit.cli.SubmitterFactory",
            side_effect=[
                MagicMock(make_submitter=MagicMock(side_effect=QQError("fail1"))),
                MagicMock(make_submitter=MagicMock(side_effect=QQError("fail2"))),
            ],
        ),
        patch("qq_lib.submit.cli.get_runtime_files", return_value=[]),
        patch("qq_lib.submit.cli.logger"),
    ):
        result = invoke_submit([str(script1), str(script2)])

    assert result.exit_code == CFG.exit_codes.default


def test_submit_multiple_scripts_summary_reports_failures(tmp_path):
    script1 = tmp_path / "script1.sh"
    script2 = tmp_path / "script2.sh"
    script3 = tmp_path / "script3.sh"
    script1.write_text("#!/usr/bin/env -S qq run\n")
    script2.write_text("#!/usr/bin/env -S qq run\n")
    script3.write_text("#!/usr/bin/env -S qq run\n")

    submitter1 = make_submitter_mock(job_id="job1", input_dir=tmp_path)

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch(
            "qq_lib.submit.cli.SubmitterFactory",
            side_effect=[
                MagicMock(make_submitter=MagicMock(return_value=submitter1)),
                MagicMock(make_submitter=MagicMock(side_effect=QQError("fail1"))),
                MagicMock(make_submitter=MagicMock(side_effect=QQError("fail2"))),
            ],
        ),
        patch("qq_lib.submit.cli.get_runtime_files", return_value=[]),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        invoke_submit([str(script1), str(script2), str(script3)])

    info_messages = [call.args[0] for call in mock_logger.info.call_args_list]
    assert any("1/3" in msg for msg in info_messages)
    assert any("2" in msg and "failed" in msg for msg in info_messages)
