# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Structured storage and serialization of qq job metadata.

This module defines the `Info` dataclass, which provides a representation
of qq job information: submission parameters, resource requests, job state,
timing data, dependencies, and execution context. It handles
loading and exporting YAML info files both locally and from remote hosts, and
offers minimal helpers such as command-line reconstruction for resubmission.

`Info` focuses strictly on data representation and safe serialization; higher-level
logic (state interpretation, batch-system interaction, consistency checks) is
implemented in `Informer` and related components.
"""

from dataclasses import dataclass, field, fields
from datetime import datetime
from pathlib import Path
from typing import Self

import yaml

from qq_lib.batch.interface import BatchInterface, BatchMeta
from qq_lib.core.common import load_yaml_dumper, load_yaml_loader
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.depend import Depend
from qq_lib.properties.transfer_mode import Success, TransferMode

from .job_type import JobType
from .loop import LoopInfo
from .resources import Resources
from .states import NaiveState

logger = get_logger(__name__)

SafeLoader: type[yaml.SafeLoader] = load_yaml_loader()
Dumper: type[yaml.Dumper] = load_yaml_dumper()


@dataclass
class Info:
    """
    Dataclass storing information about a qq job.

    Exposes only minimal functionality for loading, exporting, and basic access.
    More complex operations, such as transforming or combining the data
    should be implemented in Informer.
    """

    # The batch system class used
    batch_system: type[BatchInterface]

    # Version of qq that submitted the job
    qq_version: str

    # Name of the user who submitted the job
    username: str

    # Job identifier inside the batch system
    job_id: str

    # Job name
    job_name: str

    # Name of the script executed
    script_name: str

    # Queue the job was submitted to
    queue: str

    # Type of the qq job
    job_type: JobType

    # Host from which the job was submitted
    input_machine: str

    # Directory from which the job was submitted
    input_dir: Path

    # Job state according to qq
    job_state: NaiveState

    # Job submission timestamp
    submission_time: datetime

    # Name of the file for storing standard output of the executed script
    stdout_file: str

    # Name of the file for storing error output of the executed script
    stderr_file: str

    # Resources allocated to the job
    resources: Resources

    # List of files and directories to not copy to the working directory.
    excluded_files: list[Path] = field(default_factory=list)

    # List of files and directories to explicitly copy to the working directory.
    included_files: list[Path] = field(default_factory=list)

    # Mode of transferring files from the working directory to the input directory after job completion.
    transfer_mode: list[TransferMode] = field(default_factory=lambda: [Success()])

    # List of dependencies.
    depend: list[Depend] = field(default_factory=list)

    # Loop job-associated information.
    loop_info: LoopInfo | None = None

    # Account associated with the job
    account: str | None = None

    # Batch server the job was submitted to
    # Can be `None` which indicates the job was submitted
    # to the default (main) batch server the input machine is connected to
    server: str | None = None

    # Interpreter to use for running the submitted script
    interpreter: str | None = None

    # Job start time
    start_time: datetime | None = None

    # Main node assigned to the job
    main_node: str | None = None

    # All nodes assigned to the job
    all_nodes: list[str] | None = None

    # Working directory
    work_dir: Path | None = None

    # Job completion time
    completion_time: datetime | None = None

    # Exit code of qq run
    job_exit_code: int | None = None

    @classmethod
    def from_file(cls, file: Path, host: str | None = None) -> Self:
        """
        Load an Info instance from a YAML file, either locally or on a remote host.

        If `host` is provided, the file will be read from the remote host using
        the batch system's `read_remote_file` method. Otherwise, the file is read locally.

        Args:
            file (Path): Path to the YAML qq info file.
            host (str | None): Optional hostname of the remote machine where the file resides.
                If None, the file is assumed to be local.

        Returns:
            Info: Instance constructed from the file.

        Raises:
            QQError: If the file does not exist, cannot be reached, cannot be parsed,
                    or does not contain all mandatory information.
        """
        try:
            if host:
                # remote file
                logger.debug(f"Loading qq info from '{file}' on '{host}'.")

                BatchSystem = BatchMeta.from_env_var_or_guess()
                data: dict[str, object] = yaml.load(
                    BatchSystem.read_remote_file(host, file),
                    Loader=SafeLoader,
                )
            else:
                # local file
                logger.debug(f"Loading qq info from '{file}'.")

                try:
                    with file.open("r") as input:
                        data: dict[str, object] = yaml.load(input, Loader=SafeLoader)
                except FileNotFoundError:
                    raise QQError(f"qq info file '{file}' does not exist.")
                except PermissionError:
                    raise QQError(
                        f"No permission to read file '{file}' or access its parent directory."
                    )
                except IsADirectoryError:
                    raise QQError(f"Expected a file but path is a directory: {file}.")
                except UnicodeDecodeError as e:
                    raise QQError(f"File is not valid UTF-8 text: {file}.") from e
                except yaml.YAMLError as e:
                    raise QQError(f"Failed to parse YAML in {file}: {e}.") from e

            return cls._from_dict(data)
        except yaml.YAMLError as e:
            raise QQError(f"Could not parse the qq info file '{file}': {e}.") from e
        except TypeError as e:
            raise QQError(f"Invalid qq info file '{file}': {e}.") from e

    def to_file(self, file: Path, host: str | None = None) -> None:
        """
        Export this Info instance to a YAML file, either locally or on a remote host.

        If `host` is provided, the file will be written to the remote host using
        the batch system's `write_remote_file` method. Otherwise, the file is written locally.

        Args:
            file (Path): Path to write the YAML file.
            host (str | None): Optional hostname of the remote machine where the file should be written.
                If None, the file is written locally.

        Raises:
            QQError: If the file cannot be created, reached, or written to.
        """
        try:
            content = "# qq job info file\n" + self._to_yaml() + "\n"

            if host:
                # remote file
                logger.debug(f"Exporting qq info into '{file}' on '{host}'.")
                self.batch_system.write_remote_file(host, file, content)
            else:
                # local file
                logger.debug(f"Exporting qq info into '{file}'.")
                with file.open("w") as output:
                    output.write(content)
        except Exception as e:
            raise QQError(f"Cannot create or write to file '{file}': {e}") from e

    def get_command_line_for_resubmit(self) -> list[str]:
        """
        Construct the command-line arguments required to resubmit the job.

        Returns:
            list[str]: A list of command-line tokens representing all options
            needed to resubmit the job.
        """

        command_line = [
            self.script_name,
            "--queue",
            self.queue,
            "--job-type",
            str(self.job_type),
            "--batch-system",
            str(self.batch_system),
            "--depend",
            f"afterok={self.job_id}",
        ]

        command_line.extend(self.resources.to_command_line())

        if self.server:
            command_line.extend(["--server", self.server])

        if self.account:
            command_line.extend(["--account", self.account])

        if self.excluded_files:
            command_line.extend(
                ["--exclude", ",".join([str(x) for x in self.excluded_files])]
            )

        if self.included_files:
            command_line.extend(
                ["--include", ",".join([str(x) for x in self.included_files])]
            )

        if self.loop_info:
            command_line.extend(self.loop_info.to_command_line())

        command_line.extend(
            [
                "--transfer-mode",
                ":".join(mode.to_str() for mode in self.transfer_mode),
            ]
        )

        return command_line

    def _to_yaml(self) -> str:
        """
        Serialize the Info instance to a YAML string.

        Returns:
            str: YAML representation of the Info object.
        """
        return yaml.dump(
            self._to_dict(), default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    def _to_dict(self) -> dict[str, object]:
        """
        Convert the Info instance into a dictionary of string-object pairs.
        Fields that are None are ignored.

        Returns:
            dict[str, object]: Dictionary containing all fields with non-None values,
            converting enums and nested objects appropriately.
        """
        result: dict[str, object] = {}

        for f in fields(self):
            value = getattr(self, f.name)
            # ignore None fields
            if value is None:
                continue

            # empty lists are ignored
            if isinstance(value, list) and not value:
                continue

            # convert job type
            if f.type == JobType:
                result[f.name] = str(value)
            # convert resources
            elif f.type == Resources or f.type == LoopInfo | None:
                result[f.name] = value.to_dict()
            # convert the state and the batch system
            elif (
                f.type == NaiveState
                or f.type == type[BatchInterface]
                or f.type == Path
                or f.type == Path | None
            ):
                result[f.name] = str(value)
            # convert list of excluded/included files
            elif f.type == list[Path]:
                result[f.name] = [str(x) for x in value]
            # conver transfer modes
            elif f.type == list[TransferMode]:
                result[f.name] = [x.to_str() for x in value]
            elif f.type == list[Depend]:
                result[f.name] = [Depend.to_str(x) for x in value]
            # convert timestamp
            elif f.type == datetime or f.type == datetime | None:
                result[f.name] = value.strftime(CFG.date_formats.standard)
            else:
                result[f.name] = value

        return result

    @classmethod
    def _from_dict(cls, data: dict[str, object]) -> Self:
        """
        Construct an Info instance from a dictionary.

        Args:
            data (dict[str, object]): Dictionary containing field names and values.

        Returns:
            Info: An Info instance.

        Raises:
            TypeError: If required fields are missing.
        """
        init_kwargs = {}
        for f in fields(cls):
            name = f.name
            # skip undefined fields
            if name not in data:
                continue

            value = data[name]

            # convert job type
            if f.type == JobType and isinstance(value, str):
                init_kwargs[name] = JobType.from_str(value)
            # convert optional loop job info
            elif f.type == LoopInfo | None and isinstance(value, dict):
                init_kwargs[name] = LoopInfo.from_dict(value)  # ty: ignore[invalid-argument-type]
            # convert resources
            elif f.type == Resources:
                init_kwargs[name] = Resources(**value)  # ty: ignore[invalid-argument-type]
            # convert the batch system
            elif f.type == type[BatchInterface] and isinstance(value, str):
                init_kwargs[name] = BatchMeta.from_str(value)
            # convert the job state
            elif f.type == NaiveState and isinstance(value, str):
                init_kwargs[name] = (
                    NaiveState.from_str(value) if value else NaiveState.UNKNOWN
                )
            # convert paths (incl. optional paths)
            elif f.type == Path or f.type == Path | None:
                init_kwargs[name] = Path(value)  # ty: ignore[invalid-argument-type]
            # convert the list of excluded paths
            elif f.type == list[Path] and isinstance(value, list):
                init_kwargs[name] = [
                    Path(v) if isinstance(v, str) else v for v in value
                ]
            # convert transfer modes
            elif f.type == list[TransferMode] and isinstance(value, list):
                init_kwargs[name] = [TransferMode.from_str(x) for x in value]  # ty: ignore[invalid-argument-type]
            # convert dependencies
            elif f.type == list[Depend] and isinstance(value, list):
                init_kwargs[name] = [Depend.from_str(x) for x in value]  # ty: ignore[invalid-argument-type]
            # convert timestamp
            elif (f.type == datetime or f.type == datetime | None) and isinstance(
                value, str
            ):
                init_kwargs[name] = datetime.strptime(value, CFG.date_formats.standard)
            else:
                init_kwargs[name] = value

        return cls(**init_kwargs)
