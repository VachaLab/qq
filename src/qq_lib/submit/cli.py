# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import sys
from pathlib import Path
from typing import NoReturn

import click
from click.shell_completion import CompletionItem
from click_option_group import optgroup

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import (
    available_job_types,
    available_work_dirs,
    get_runtime_files,
    logical_resolve,
)
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.submit.factory import SubmitterFactory

logger = get_logger(__name__)


def complete_script(
    _ctx: click.Context, _param: click.Parameter, incomplete: str
) -> list[CompletionItem]:
    """
    Return completion items for script files and directories matching the incomplete string.

    If incomplete is a directory or resolves to one, lists files and subdirectories
    inside it. Otherwise, lists files and directories in the current directory
    matching the prefix.
    """
    incomplete_path = Path(incomplete)

    if incomplete_path.is_dir():
        search_dir = incomplete_path
        prefix = ""
    else:
        search_dir = incomplete_path.parent
        prefix = incomplete_path.name

    return [
        CompletionItem(str(path), type="file")
        for path in search_dir.iterdir()
        if path.name.startswith(prefix)
    ]


# Note that all options must be part of an optgroup otherwise Parser breaks.
@click.command(
    short_help="Submit a job to the batch system.",
    help=f"""
Submit a qq job to a batch system from the command line.

{click.style("SCRIPT", fg="green")}   Path to the script to submit.

All the options can also be specified inside the submitted script itself
using qq directives of this format: `# qq <option> <value>`.
""",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.argument(
    "script",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, path_type=str
    ),
    metavar=click.style("SCRIPT", fg="green"),
    shell_complete=complete_script,
)
@optgroup.group(f"{click.style('General settings', fg='yellow')}")
@optgroup.option(
    "--queue",
    "-q",
    type=str,
    default=None,
    help="Name of the queue to submit the job to.",
)
@optgroup.option(
    "-s",
    "--server",
    type=str,
    default=None,
    help="Name of the batch server to submit the job to. If not specified, the job is submitted to the current (main) batch server.\n"
    "Only supported on Metacentrum-family clusters.",
)
@optgroup.option(
    "--account",
    type=str,
    default=None,
    help="Account to use for the job. Only needed in environments with accounting (e.g., IT4Innovations).",
)
@optgroup.option(
    "--job-type",
    type=str,
    default=None,
    help=f"Type of the job. Defaults to {click.style('standard', bold=True)}. Available types: {available_job_types()}.",
)
@optgroup.option(
    "--exclude",
    type=str,
    default=None,
    help=(
        f"Colon-, comma-, or space-separated list of files or directories that should {click.style('not', bold=True)} be copied to the working directory.\n"
        "Paths to files and directories to exclude must be relative to the input directory.\n"
    ),
)
@optgroup.option(
    "--include",
    type=str,
    default=None,
    help=(
        f"Colon-, comma-, or space-separated list of files or directories to copy into the working directory "
        f"in addition to the input directory contents.\n"
        f"These files are {click.style('not', bold=True)} copied back after job completion. "
        f"Paths must be absolute or relative to the input directory. "
        f"Ignored if the input directory is used as the working directory.\n"
    ),
)
@optgroup.option(
    "--depend",
    type=str,
    default=None,
    help=(
        f"Comma- or space-separated list of job dependencies in the format '<type>=<job_id>[:<job_id>...]'.\n"
        f"Available types: {click.style('after', bold=True)} (after start), {click.style('afterok', bold=True)} (after success), "
        f"{click.style('afternotok', bold=True)} (after failure/kill), {click.style('afterany', bold=True)} (after completion regardless of outcome).\n"
        f"Multiple job IDs in one expression (colon-separated) require {click.style('all', bold=True)} listed jobs to satisfy the condition. "
        f"Multiple expressions must {click.style('all', bold=True)} be satisfied before the job starts.\n"
        f"Examples: 'afterok=1234', 'after=456:789', 'afterok=123,afternotok=678'."
    ),
)
@optgroup.option(
    "--transfer-mode",
    type=str,
    default=None,
    help=(
        f"Colon-, comma-, or space-separated list of transfer modes controlling when working directory files are transferred to the input directory.\n"
        f"Modes: {click.style('success', bold=True)} (exit code 0), {click.style('failure', bold=True)} (non-zero exit code), "
        f"{click.style('always', bold=True)}, {click.style('never', bold=True)}, "
        f"or a specific {click.style('exit code', bold=True)} number (e.g., 42). Combine modes; files transfer if {click.style('any', bold=True)} apply.\n"
        f"Defaults to {click.style('success', bold=True)}. On transfer, the working directory is deleted; otherwise it is preserved.\n"
        f"Killed jobs are {click.style('never', bold=True)} transferred automatically. Ignored if the input directory is used as the working directory.\n"
        f"Examples: 'success', 'always', 'success:42', '1 2 3'.\n"
    ),
)
@optgroup.option(
    "--interpreter",
    type=str,
    default="bash",
    help=f"Executable name or absolute path of the interpreter used to run the submitted script. Defaults to {click.style('bash', bold=True)}. The interpreter must be available on the computing node.",
)
@optgroup.option(
    "--batch-system",
    type=str,
    default=None,
    help=f"Name of the batch system used to submit the job. If not specified, the value of the environment variable '{CFG.env_vars.batch_system}' is used or the system is auto-detected.",
)
@optgroup.group(
    f"{click.style('Requested resources', fg='yellow')}",
    help="Memory and storage sizes are specified as 'N<unit>' where unit is one of b, kb, mb, gb, tb, pb (e.g., 500mb, 32gb).",
)
@optgroup.option(
    "--nnodes",
    type=int,
    default=None,
    help="Number of nodes to allocate for the job.",
)
@optgroup.option(
    "--ncpus-per-node",
    type=int,
    default=None,
    help="Number of CPU cores to allocate per node.",
)
@optgroup.option(
    "--ncpus",
    type=int,
    default=None,
    help=f"Total number of CPU cores to allocate for the job. Overrides {click.style('--ncpus-per-node', bold=True)}.",
)
@optgroup.option(
    "--mem-per-cpu",
    type=str,
    default=None,
    help="Memory to allocate per CPU core.",
)
@optgroup.option(
    "--mem-per-node",
    type=str,
    default=None,
    help=f"Memory to allocate per node. Overrides {click.style('--mem-per-cpu', bold=True)}.",
)
@optgroup.option(
    "--mem",
    type=str,
    default=None,
    help=f"Total memory to allocate for the job. Overrides {click.style('--mem-per-cpu', bold=True)} and {click.style('--mem-per-node', bold=True)}.",
)
@optgroup.option(
    "--ngpus-per-node",
    type=int,
    default=None,
    help="Number of GPUs to allocate per node.",
)
@optgroup.option(
    "--ngpus",
    type=int,
    default=None,
    help=f"Total number of GPUs to allocate for the job. Overrides {click.style('--ngpus-per-node', bold=True)}.",
)
@optgroup.option(
    "--walltime",
    type=str,
    default=None,
    help="Maximum runtime for the job. Examples: '1d', '12h', '10m', '24:00:00'.",
)
@optgroup.option(
    "--work-dir",
    "--workdir",
    type=str,
    default=None,
    help=f"Type of working directory to use for the job. Available types: {available_work_dirs()}.",
)
@optgroup.option(
    "--work-size-per-cpu",
    "--worksize-per-cpu",
    type=str,
    default=None,
    help="Storage to allocate per CPU core.",
)
@optgroup.option(
    "--work-size-per-node",
    "--worksize-per-node",
    type=str,
    default=None,
    help=f"Storage to allocate per node. Overrides {click.style('--work-size-per-cpu', bold=True)}.",
)
@optgroup.option(
    "--work-size",
    "--worksize",
    type=str,
    default=None,
    help=f"Total storage to allocate for the job. Overrides {click.style('--work-size-per-cpu', bold=True)} and {click.style('--work-size-per-node', bold=True)}.",
)
@optgroup.option(
    "--props",
    type=str,
    default=None,
    help="Colon-, comma-, or space-separated list of node properties required (e.g., cl_two) or prohibited (e.g., ^cl_two) to run the job.",
)
@optgroup.group(
    f"{click.style('Loop options', fg='yellow')}",
    help=f"Only used when job-type is {click.style('loop', bold=True)}.",
)
@optgroup.option(
    "--loop-start",
    type=int,
    default=None,
    help=f"Starting cycle for a loop job. Defaults to {click.style('1', bold=True)}.",
)
@optgroup.option(
    "--loop-end", type=int, default=None, help="Ending cycle for a loop job."
)
@optgroup.option(
    "--archive",
    type=str,
    default=None,
    help=f"Directory name for archiving files from a loop job. Defaults to {click.style('storage', bold=True)}.",
)
@optgroup.option(
    "--archive-format",
    type=str,
    default=None,
    help=f"Filename format for archived files. Defaults to {click.style('job%04d', bold=True)}.",
)
@optgroup.option(
    "--archive-mode",
    type=str,
    default=None,
    help=(
        f"Colon-, comma-, or space-separated list of archive modes controlling when working directory files are archived upon job completion.\n"
        f"Supports the same modes as {click.style('--transfer-mode', bold=True)}. Defaults to {click.style('success', bold=True)}."
    ),
)
def submit(script: str, **kwargs) -> NoReturn:
    """
    Submit a qq job to a batch system from the command line.
    """
    try:
        if not (script_path := Path(script)).is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

        # parse options from the command line and from the script itself
        factory = SubmitterFactory(logical_resolve(script_path), **kwargs)
        submitter = factory.makeSubmitter()

        # guard against multiple submissions from the same directory
        if get_runtime_files(submitter.getInputDir()) and not submitter.continuesLoop():
            raise QQError(
                "Detected qq runtime files in the submission directory. Submission aborted."
            )

        job_id = submitter.submit()
        logger.info(f"Job '{job_id}' submitted successfully.")
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(CFG.exit_codes.unexpected_error)
