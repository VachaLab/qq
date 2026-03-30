# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from tabulate import Line, TableFormat, tabulate

from qq_lib.batch.interface import BatchJobInterface
from qq_lib.batch.interface.interface import BatchInterface
from qq_lib.core.common import (
    format_duration_wdhhmmss,
    get_panel_width,
)
from qq_lib.core.config import CFG
from qq_lib.properties.states import BatchState


class JobsPresenter:
    """
    Present information about a collection of jobs from the batch system and their statistics.
    """

    # Mapping of human-readable color names to ANSI escape codes.
    _ANSI_COLORS = {
        # default
        "default": "",
        # standard colors
        "black": "\033[30m",
        "red": "\033[31m",
        "green": "\033[32m",
        "yellow": "\033[33m",
        "blue": "\033[34m",
        "magenta": "\033[35m",
        "cyan": "\033[36m",
        "white": "\033[37m",
        # bright colors
        "bright_black": "\033[90m",
        "bright_red": "\033[91m",
        "bright_green": "\033[92m",
        "bright_yellow": "\033[93m",
        "bright_blue": "\033[94m",
        "bright_magenta": "\033[95m",
        "bright_cyan": "\033[96m",
        "bright_white": "\033[97m",
        # other colors
        "grey90": "\033[38;5;254m",
        "grey70": "\033[38;5;249m",
        "grey50": "\033[38;5;244m",
        "grey30": "\033[38;5;239m",
        "grey10": "\033[38;5;233m",
        # bold:
        "bold": "\033[1m",
        # reset
        "reset": "\033[0m",
    }

    # Table formatting configuration for `tabulate`.
    _COMPACT_TABLE = TableFormat(
        lineabove=Line("", "", "", ""),
        linebelowheader="",
        linebetweenrows="",
        linebelow=Line("", "", "", ""),
        headerrow=("", " ", ""),
        datarow=("", " ", ""),
        padding=0,
        with_header_hide=["lineabove", "linebelow"],
    )

    def __init__(
        self,
        batch_system: type[BatchInterface],
        jobs: list[BatchJobInterface],
        extra: bool,
        all: bool,
        server: str | None,
    ):
        """
        Initialize the presenter with a list of jobs.

        Args:
            jobs (list[BatchJobInterface]): List of job information objects
                to be presented.
            extra (bool): Should show additional info about jobs.
            all (bool): Show all jobs, not just queued and running.
            server (str | None): Batch server for which the jobs were collected.
                `None` = default server.
        """
        self._batch_system = batch_system
        self._jobs = jobs
        self._stats = JobsStatistics()
        self._extra = extra
        self._all = all
        self._server = server

    def create_jobs_info_panel(self, console: Console | None = None) -> Group:
        """
        Create a Rich panel displaying job information and statistics.

        Args:
            console (Console | None): Optional Rich Console instance.
                If None, a new Console will be created.

        Returns:
            Group: Rich Group containing the jobs table and stats panel.
        """
        console = console or Console()

        jobs_table = self._create_basic_jobs_table()
        if self._extra:
            jobs_table = self._insert_extra_info(jobs_table)

        # convert ANSI codes to Rich Text
        jobs_panel = Text.from_ansi(jobs_table)
        stats_panel = self._stats.create_stats_panel()

        content = Group(
            jobs_panel,
            Text(""),
            stats_panel,
        )

        panel = Panel(
            content,
            title=Text(
                "COLLECTED JOBS",
                style=CFG.jobs_presenter.title_style,
                justify="center",
            ),
            subtitle=Text(
                f"{self._server}",
                style=CFG.jobs_presenter.subtitle_style,
                justify="center",
            )
            if self._server
            else None,
            border_style=CFG.jobs_presenter.border_style,
            padding=(1, 1),
            width=get_panel_width(
                console, 1, CFG.jobs_presenter.min_width, CFG.jobs_presenter.max_width
            ),
            expand=False,
        )

        return Group(Text(""), panel, Text(""))

    def dump_yaml(self) -> None:
        """
        Print the YAML representation of all jobs to stdout.
        """
        for job in self._jobs:
            print(job.to_yaml())

    def _create_basic_jobs_table(self) -> str:
        """
        Build a compact tabulated string representation of the job list.

        Returns:
            str: Tabulated job information with ANSI color codes applied.

        Notes:
            - Uses `tabulate` with `_COMPACT_TABLE` format because
              Rich's Table is prohibitively slow for large number of items.
            - Updates internal job statistics via `self._stats`.
        """
        headers = self._get_visible_headers()
        rows = [self._create_job_row(job, headers) for job in self._jobs]

        return tabulate(
            rows,
            headers=self._format_headers(headers),
            tablefmt=JobsPresenter._COMPACT_TABLE,
            stralign="center",
            numalign="center",
        )

    def _get_visible_headers(self) -> list[str]:
        """
        Get list of headers to display based on the batch system configuration.

        Return:
            list[str]: A list of headers to show.
        """
        all_headers = [
            "S",
            "Job ID",
            "User",
            "Job Name",
            "Queue",
            "NCPUs",
            "NGPUs",
            "NNodes",
            "Times",
            "Node",
            "%CPU",
            "%Mem",
            "Exit" if self._all or CFG.jobs_presenter.columns_to_show else None,
        ]
        headers_to_show = (
            CFG.jobs_presenter.columns_to_show
            or self._batch_system.jobs_presenter_columns_to_show()
        )
        return [h for h in all_headers if h and h in headers_to_show]

    def _create_job_row(self, job: BatchJobInterface, headers: list[str]) -> list[str]:
        """
        Create a single row of job data.

        Args:
            job (BatchJobInterface): Job to show information for.
            headers (list[str]): List of headers to include in the row

        Returns:
            list[str]: List of formatted cell values.
        """
        state = job.get_state()
        start_time, end_time = self._get_job_times(job, state)

        # update statistics
        cpus = job.get_n_cpus() or 0
        gpus = job.get_n_gpus() or 0
        nodes = job.get_n_nodes() or 0
        self._stats.add_job(state, cpus, gpus, nodes)

        if self._server:
            # show full job ID if we are working with a non-standard server
            job_id = job.get_id()
        else:
            # otherwise, show only the numerical portion of the ID
            job_id = JobsPresenter._shorten_job_id(job.get_id())

        # build the row
        row_data: dict[str, str] = {
            "S": JobsPresenter._color(state.to_code(), state.color),
            "Job ID": JobsPresenter._main_color(job_id),
            "User": JobsPresenter._main_color(job.get_user() or ""),
            "Job Name": JobsPresenter._main_color(
                JobsPresenter._shorten_job_name(job.get_name() or "")
            ),
            "Queue": JobsPresenter._main_color(job.get_queue() or ""),
            "NCPUs": JobsPresenter._main_color(str(cpus)),
            "NGPUs": JobsPresenter._main_color(str(gpus)),
            "NNodes": JobsPresenter._main_color(str(nodes)),
            "Times": JobsPresenter._format_time(
                state, start_time, end_time, job.get_walltime()
            ),
            "Node": JobsPresenter._format_nodes_or_comment(state, job),
            "%CPU": JobsPresenter._format_util_cpu(job.get_util_cpu()),
            "%Mem": JobsPresenter._format_util_mem(job.get_util_mem()),
            "Exit": JobsPresenter._format_exit_code(job, state) if self._all else "",
        }

        return [row_data[header] for header in headers if header in row_data]

    @staticmethod
    def _get_job_times(
        job: BatchJobInterface, state: BatchState
    ) -> tuple[datetime | None, datetime | None]:
        """
        Get start and end times for a job based on its state.

        Args:
            job (BatchJobInterface): Job to get times for.
            state (BatchState): The current job state.

        Returns:
            tuple[datetime | None, datetime | None]: Tuple of (start_time, end_time).
        """
        if state in {BatchState.QUEUED, BatchState.HELD, BatchState.WAITING}:
            start_time = job.get_submission_time()
        else:
            start_time = job.get_start_time() or job.get_submission_time()

        if state in {BatchState.FINISHED, BatchState.FAILED}:
            end_time = job.get_completion_time() or job.get_modification_time()
        else:
            end_time = datetime.now()

        return start_time, end_time

    def _format_headers(self, headers: list[str]) -> list[str]:
        """
        Apply formatting to table headers.

        Args:
            headers (list[str]): List of headers to format.

        Returns:
            list[str]: List of formatted and colored headers.
        """
        return [
            JobsPresenter._color(
                header, color=CFG.jobs_presenter.headers_style, bold=True
            )
            for header in headers
        ]

    def _insert_extra_info(self, table: str) -> str:
        """
        Augment a formatted job table with additional information about each job.

        Lines where job attributes are missing are skipped.

        Args:
            table (str): The formatted table string containing one line per job.

        Returns:
            str: A new table string including the extra job information lines.
        """
        split_table = table.splitlines()
        table_with_extra_info = split_table[0] + "\n"

        for line, job in zip(split_table[1:], self._jobs):
            table_with_extra_info += line + "\n"

            if input_machine := job.get_input_machine():
                table_with_extra_info += JobsPresenter._color(
                    f" >   Input machine:   {input_machine}\n",
                    CFG.jobs_presenter.extra_info_style,
                )

            if input_dir := job.get_input_dir():
                table_with_extra_info += JobsPresenter._color(
                    f" >   Input directory: {str(input_dir)}\n",
                    CFG.jobs_presenter.extra_info_style,
                )

            if comment := job.get_comment():
                table_with_extra_info += JobsPresenter._color(
                    f" >   Comment:         {comment}\n",
                    CFG.jobs_presenter.extra_info_style,
                )
            table_with_extra_info += "\n"

        return table_with_extra_info

    @staticmethod
    def _format_time(
        state: BatchState,
        start_time: datetime | None,
        end_time: datetime | None,
        walltime: timedelta | None,
    ) -> str:
        """
        Format the job running time, queued time or completion time with color coding.

        Args:
            state (BatchState): Current job state.
            start_time (datetime | None): Job submission or start time.
            end_time (datetime | None): Job completion or current time.
            walltime (timedelta | None): Scheduled walltime for the job.

        Returns:
            str: ANSI-colored string representing elapsed or finished time.
        """
        # return an empty string if any of the required times is missing
        if start_time is None or end_time is None or walltime is None:
            return ""

        match state:
            case BatchState.UNKNOWN | BatchState.SUSPENDED:
                return ""
            case BatchState.FAILED | BatchState.FINISHED:
                return JobsPresenter._color(
                    end_time.strftime(CFG.date_formats.standard), color=state.color
                )
            case (
                BatchState.HELD
                | BatchState.QUEUED
                | BatchState.WAITING
                | BatchState.MOVING
            ):
                return JobsPresenter._color(
                    format_duration_wdhhmmss(end_time - start_time),
                    color=state.color,
                )
            case BatchState.RUNNING | BatchState.EXITING:
                run_time = end_time - start_time
                return JobsPresenter._color(
                    format_duration_wdhhmmss(run_time),
                    color=CFG.jobs_presenter.strong_warning_style
                    if run_time > walltime
                    else state.color,
                ) + JobsPresenter._main_color(
                    f" / {format_duration_wdhhmmss(walltime)}"
                )

        return Text("")

    @staticmethod
    def _format_util_cpu(util: int | None) -> str:
        """
        Format CPU utilization with color coding.

        Args:
            util (int | None): CPU usage percentage.

        Returns:
            str: ANSI-colored string representation of CPU utilization,
                 or empty string if `util` is None.
        """
        if util is None:
            return ""

        if util > 100:
            color = CFG.jobs_presenter.strong_warning_style
        elif util >= 80:
            color = CFG.jobs_presenter.main_style
        elif util >= 60:
            color = CFG.jobs_presenter.mild_warning_style
        else:
            color = CFG.jobs_presenter.strong_warning_style

        return JobsPresenter._color(str(util), color=color)

    @staticmethod
    def _format_util_mem(util: int | None) -> str:
        """
        Format memory utilization with color coding.

        Args:
            util (int | None): Memory usage percentage.

        Returns:
            str: ANSI-colored string representation of memory utilization,
                 or empty string if `util` is None.
        """
        if util is None:
            return ""

        if util < 90:
            color = CFG.jobs_presenter.main_style
        elif util < 100:
            color = CFG.jobs_presenter.mild_warning_style
        else:
            color = CFG.jobs_presenter.strong_warning_style

        return JobsPresenter._color(str(util), color=color)

    @staticmethod
    def _format_exit_code(job: BatchJobInterface, state: BatchState) -> str:
        """
        Get formatted exit code if the job is completed and color it appropriately.

        The color of the exit code is set based on the state of the job,
        not on the value of the exit code.

        If the job is not completed, returns an empty string.

        Args:
            job (BatchJobInterface): Job to get the exit code for.
            state (BatchState): The current job state.

        Returns:
            str: ANSI-colored exit code. Empty string if the job is not completed
            or the exit code is undefined.
        """
        if (exit_code := job.get_exit_code()) is None:
            return ""

        match state:
            case BatchState.FINISHED:
                return JobsPresenter._main_color(str(exit_code))
            case BatchState.FAILED:
                return JobsPresenter._color(
                    str(exit_code), color=CFG.jobs_presenter.strong_warning_style
                )
            case _:
                return ""

    @staticmethod
    def _format_nodes_or_comment(state: BatchState, job: BatchJobInterface) -> str:
        """
        Format node information or an estimated runtime comment.

        Args:
            state (BatchState): Current job state.
            job (BatchJobInterface): Job information object.

        Returns:
            str: ANSI-colored string for working node(s) or estimated start,
                 or an empty string if neither information is available.
        """
        if nodes := job.get_short_nodes():
            return JobsPresenter._main_color(
                JobsPresenter._shorten_nodes(" + ".join(nodes)),
            )

        if state in {BatchState.FINISHED, BatchState.FAILED}:
            return ""

        if estimated := job.get_estimated():
            truncated_nodes = JobsPresenter._shorten_nodes(estimated[1])
            return JobsPresenter._color(
                f"{truncated_nodes} in {format_duration_wdhhmmss(estimated[0] - datetime.now()).rsplit(':', 1)[0]}",
                color=state.color,
            )

        return ""

    @staticmethod
    def _shorten_job_id(job_id: str) -> str:
        """
        Shorten the job ID to its primary component (before the first dot).

        Args:
            job_id (str): Full job identifier.

        Returns:
            str: Shortened job ID.
        """
        return job_id.split(".", 1)[0]

    @staticmethod
    def _shorten_job_name(job_name: str) -> str:
        """
        Truncate a job name if it exceeds the maximum allowed display length.

        Args:
            job_name (str): The original job name string.

        Returns:
            str: The possibly shortened job name. If the original name length is
                less than or equal to the configured limit, it is returned unchanged.
        """
        if len(job_name) > CFG.jobs_presenter.max_job_name_length:
            return f"{job_name[: CFG.jobs_presenter.max_job_name_length]}…"

        return job_name

    @staticmethod
    def _shorten_nodes(nodes: str) -> str:
        """
        Truncate a list of nodes if it exceeds the maximum allowed display length.

        Args:
            nodes (str): The original nodes string.

        Returns:
            str: The possibly shortened list of nodes. If the original string length
                is less than or equal to the configured limit, it is returned unchanged.
        """
        if len(nodes) > CFG.jobs_presenter.max_nodes_length:
            return f"{nodes[: CFG.jobs_presenter.max_nodes_length]}…"

        return nodes

    @staticmethod
    def _color(string: str, color: str | None = None, bold: bool = False) -> str:
        """
        Apply ANSI color codes and optional bold styling to a string.

        Args:
            string (str): The string to colorize.
            color (str | None): Optional color.
            bold (bool): Whether to apply bold formatting.

        Returns:
            str: ANSI-colored and optionally bolded string.
        """
        return f"{JobsPresenter._ANSI_COLORS['bold'] if bold else ''}{JobsPresenter._ANSI_COLORS[color] if color else ''}{string}{JobsPresenter._ANSI_COLORS['reset'] if color or bold else ''}"

    @staticmethod
    def _main_color(string: str, bold: bool = False) -> str:
        """
        Apply the main presenter color with optional bold styling.

        Args:
            string (str): String to format.
            bold (bool): Whether to apply bold formatting.

        Returns:
            str: ANSI-colored string in the main presenter color.
        """
        return JobsPresenter._color(string, CFG.jobs_presenter.main_style, bold)

    @staticmethod
    def _secondary_color(string: str, bold: bool = False) -> str:
        """
        Apply the secondary presenter color with optional bold styling.

        Args:
            string (str): String to format.
            bold (bool): Whether to apply bold formatting.

        Returns:
            Text: ANSI-colored Rich Text object in secondary color.
        """
        return JobsPresenter._color(string, CFG.jobs_presenter.secondary_style, bold)


@dataclass
class JobsStatistics:
    """
    Dataclass for collecting statistics about jobs.
    """

    # Number of jobs of various types.
    n_jobs: dict[BatchState, int] = field(default_factory=dict)

    # Number of requested CPUs.
    n_requested_cpus: int = 0

    # Number of allocated CPUs.
    n_allocated_cpus: int = 0

    # Number of CPUs for unknown jobs.
    n_unknown_cpus: int = 0

    # Number of requested GPUs.
    n_requested_gpus: int = 0

    # Number of allocated GPUs.
    n_allocated_gpus: int = 0

    # Number of GPUs for unknown jobs.
    n_unknown_gpus: int = 0

    # Number of requested nodes.
    n_requested_nodes: int = 0

    # Number of allocated nodes.
    n_allocated_nodes: int = 0

    # Number of nodes for unknown jobs.
    n_unknown_nodes: int = 0

    def add_job(self, state: BatchState, cpus: int, gpus: int, nodes: int) -> None:
        """
        Update the collected resources based on the state of the job.

        Args:
            state (BatchState): State of the job according to the batch system.
            cpus (int): Number of CPUs requested by the job.
            gpus (int): Number of GPUs requested by the job.
            nodes (int): Number of nodes requested by the job.

        Notes:
            - Resources of QUEUED and HELD jobs are counted as REQUESTED.
            - Resources of RUNNING and EXITING jobs are counted as ALLOCATED.
            - Resources of UNKNOWN jobs are counted as UNKNOWN.
            - Resources of jobs in other states are not counted at all.
        """
        try:
            self.n_jobs[state] += 1
        except KeyError:
            self.n_jobs[state] = 1

        if state in {BatchState.QUEUED, BatchState.HELD}:
            self.n_requested_cpus += cpus
            self.n_requested_gpus += gpus
            self.n_requested_nodes += nodes
        elif state in {BatchState.RUNNING, BatchState.EXITING}:
            self.n_allocated_cpus += cpus
            self.n_allocated_gpus += gpus
            self.n_allocated_nodes += nodes
        elif state == BatchState.UNKNOWN:
            self.n_unknown_cpus += cpus
            self.n_unknown_gpus += gpus
            self.n_unknown_nodes += nodes

    def create_stats_panel(self) -> Group:
        """
        Build a Rich Group containing job statistics sections.

        Returns:
            Group: Rich Group with job state counts and resource usage.
        """
        jobs_text = self._create_job_states_stats()
        resources_table = self._create_resources_stats_table()

        table = Table.grid(expand=False)
        table.add_column(justify="left")
        # spacer column
        table.add_column(justify="center", width=5)
        table.add_column(justify="right")

        table.add_row(jobs_text, "", resources_table)

        return Group(table)

    def _create_job_states_stats(self) -> Text:
        """
        Generate Rich Text summarizing the number of jobs in each state.

        Returns:
            Text: Rich Text object listing job states and counts.
        """
        spacing = "    "
        line = Text(spacing)

        line.append(
            JobsStatistics._secondary_color_text(f"\n\n Jobs{spacing}", bold=True)
        )

        total = 0
        for state in BatchState:
            if state in self.n_jobs:
                count = self.n_jobs[state]
                total += count
                line.append(
                    JobsStatistics._color_text(
                        f"{state.to_code()} ", color=state.color, bold=True
                    )
                )
                line.append(JobsStatistics._secondary_color_text(str(count)))
                line.append(spacing)

        # sum of all jobs
        line.append(
            JobsStatistics._color_text(
                f"{CFG.jobs_presenter.sum_jobs_code} ",
                color=CFG.state_colors.sum,
                bold=True,
            )
        )
        line.append(JobsStatistics._secondary_color_text(str(total)))
        line.append(spacing)

        return line

    def _create_resources_stats_table(self) -> Table:
        """
        Create a Rich Table summarizing requested and allocated resources.

        Returns:
            Table: Rich Table showing CPU, GPU, and node usage for jobs.
        """
        table = Table(
            show_header=True,
            box=None,
            padding=(0, 1),
        )

        table.add_column("", justify="left")
        table.add_column(JobsStatistics._secondary_color_text("CPUs"), justify="center")
        table.add_column(JobsStatistics._secondary_color_text("GPUs"), justify="center")
        table.add_column(
            JobsStatistics._secondary_color_text("Nodes"), justify="center"
        )

        table.add_row(
            JobsStatistics._secondary_color_text("Requested", bold=True),
            JobsStatistics._secondary_color_text(str(self.n_requested_cpus)),
            JobsStatistics._secondary_color_text(str(self.n_requested_gpus)),
            JobsStatistics._secondary_color_text(str(self.n_requested_nodes)),
        )
        table.add_row(
            JobsStatistics._secondary_color_text("Allocated", bold=True),
            JobsStatistics._secondary_color_text(str(self.n_allocated_cpus)),
            JobsStatistics._secondary_color_text(str(self.n_allocated_gpus)),
            JobsStatistics._secondary_color_text(str(self.n_allocated_nodes)),
        )
        # unknown resources are displayed only if non-zero
        if (
            self.n_unknown_cpus > 0
            or self.n_unknown_gpus > 0
            or self.n_unknown_nodes > 0
        ):
            table.add_row(
                JobsStatistics._secondary_color_text("Unknown", bold=True),
                JobsStatistics._secondary_color_text(str(self.n_unknown_cpus)),
                JobsStatistics._secondary_color_text(str(self.n_unknown_gpus)),
                JobsStatistics._secondary_color_text(str(self.n_unknown_nodes)),
            )

        return table

    @staticmethod
    def _color_text(string: str, color: str | None = None, bold: bool = False) -> Text:
        """
        Create Rich Text with optional color and bold formatting.

        Args:
            string (str): The string to colorize.
            color (str | None): Optional color.
            bold (bool): Whether to apply bold formatting.

        Returns:
            Text: Rich Text object with applied style.
        """
        return Text(string, style=f"{color if color else ''} {'bold' if bold else ''}")

    @staticmethod
    def _secondary_color_text(string: str, bold: bool = False) -> Text:
        """
        Apply the secondary presenter color with optional bold style.

        Args:
            string (str): String to format.
            bold (bool): Whether to apply bold formatting.

        Returns:
            str: Rich Text in main color.
        """
        return JobsStatistics._color_text(
            string, color=CFG.jobs_presenter.secondary_style, bold=bold
        )
