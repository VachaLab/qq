#!/usr/bin/env -S uv run --script

# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Get the estimated time of a Gromacs simulation finishing.
Version qq 0.10.0.
Requires `uv`: https://docs.astral.sh/uv
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "qq",
# ]
#
# [tool.uv.sources]
# qq = { git = "https://github.com/Ladme/qq.git", tag = "v0.10.1" }
# ///

import argparse
import getpass
from datetime import datetime
from pathlib import Path

from rich import print

from qq_lib.batch.interface import BatchMeta
from qq_lib.core.common import format_duration_wdhhmmss, get_info_files
from qq_lib.core.navigator import Navigator
from qq_lib.info import Informer


def get_informer(id: str | None) -> Informer:
    """Get informer for the given job id or for the newest job in the current directory."""
    if id:
        return Informer.from_job_id(id)
    return Informer.from_file(get_info_files(Path())[-1])

def get_all_job_ids() -> list[str]:
    batch_system = BatchMeta.from_env_var_or_guess()
    jobs = batch_system.get_unfinished_batch_jobs(getpass.getuser(), None)
    return [job.get_id() for job in jobs]


def get_eta_from_content(content: str) -> datetime | None:
    """Get the time at which the Gromacs simulation is expected to finish."""

    # find the last line containing the ETA
    eta_line = next((s for s in content if "will finish" in s), None)
    if not eta_line:
        return None

    # assuming the time information is stored in the last 5 words
    eta = " ".join(eta_line.split()[-5:])

    try:
        return datetime.strptime(eta, "%a %b %d %H:%M:%S %Y")
    except Exception:
        return None


def main():
    # parse command line options
    parser = argparse.ArgumentParser(
        "gmx-eta",
        description="Get the estimated time of a Gromacs simulation finishing.",
    )
    parser.add_argument("job_id", nargs="*", help="Job ID(s). Optional.", default=[None])
    parser.add_argument("--all", "-a", action="store_true", help="Show ETA for all jobs.")

    args = parser.parse_args()

    job_ids = get_all_job_ids() if args.all else (args.job_id or [None])

    for job_id in job_ids:
        informer = get_informer(job_id)
        navigator = Navigator.from_informer(informer)

        jobname = f"[yellow bold]{informer.info.job_name}[/yellow bold]"

        if (main_node := navigator.get_main_node()) and (
            work_dir := navigator.get_work_dir()
        ):
            BatchSystem = BatchMeta.from_env_var_or_guess()
            # use the batch system to read the remote file with Gromacs output
            # split the lines and reverse the content to read from the end
            try:
                content = reversed(
                    BatchSystem.read_remote_file(
                        main_node, work_dir / informer.info.stderr_file
                    ).splitlines()
                )
            except Exception as e:
                print(f"{jobname}: No information is available: {e}")
                return

            # get eta
            eta = get_eta_from_content(content)
            if eta and datetime.now() <= eta:
                print(
                    f"{jobname}: Simulation will finish in [bright_blue bold]{format_duration_wdhhmmss(eta - datetime.now())}[/bright_blue bold]."
                )
            elif eta and datetime.now() > eta:
                print(
                    f"{jobname}: Simulation has finished at [bright_green bold]{str(eta)}[/bright_green bold]."
                )
            else:
                print("No information is available.")

        else:
            print(f"{jobname}: No information is available: job does not have a working directory.")
            return


if __name__ == "__main__":
    main()
