# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from typing import ClassVar, Self

import yaml

from qq_lib.batch.interface import BatchInterface
from qq_lib.core.common import load_yaml_dumper, load_yaml_loader
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)

SafeLoader: type[yaml.SafeLoader] = load_yaml_loader()
Dumper: type[yaml.Dumper] = load_yaml_dumper()


class _YAMLSerializable:
    """
    Mixin for classes that load from and save to YAML files.

    Subclasses must define:
        _file_label:   A short human-readable label used in log and error messages.
        _file_comment: A short human-readable comment used in the YAML header.
        _from_dict:    A classmethod that turns the parsed dict into an instance.
        _to_dict:      A method that returns all fields as a dict.
    """

    _file_label: ClassVar[str]
    _file_comment: ClassVar[str]

    @classmethod
    def _from_dict(cls, data: dict[str, object]) -> Self:
        raise NotImplementedError(
            f"_from_dict method is not implemented for {cls.__name__}"
        )

    def _to_dict(self) -> dict[str, object]:
        raise NotImplementedError(
            f"_to_dict method is not implemented for {type(self).__name__}"
        )

    @classmethod
    def from_file(cls, file: Path, host: str | None = None) -> Self:
        """
        Load an instance from a YAML file, either locally or on a remote host.

        If `host` is provided, the file will be read from the remote host
        using the batch system's `read_remote_file` method.  Otherwise, the
        file is read locally.

        Args:
            file (Path): Path to the YAML file.
            host (str | None): Optional hostname of the remote machine where the file
                resides.  If `None`, the file is assumed to be local.

        Returns:
            Self: A new instance constructed from the file contents.

        Raises:
            QQError: If the file does not exist, cannot be reached, cannot be
                parsed, or does not contain all mandatory information.
        """
        label = cls._file_label

        try:
            if host:
                logger.debug(f"Loading {label} from '{file}' on '{host}'.")

                BatchSystem = BatchInterface.from_env_var_or_guess()
                data: dict[str, object] = yaml.load(
                    BatchSystem.read_remote_file(host, file),
                    Loader=SafeLoader,
                )
            else:
                logger.debug(f"Loading {label} from '{file}'.")
                data = cls._read_local_yaml(file)

            return cls._from_dict(data)
        except yaml.YAMLError as e:
            raise QQError(f"Could not parse the {label} file '{file}': {e}.") from e
        except TypeError as e:
            raise QQError(f"Invalid {label} file '{file}': {e}.") from e

    def to_file(self, file: Path, host: str | None = None) -> None:
        """
        Export this instance to a YAML file, either locally or on a remote host.

        If `host` is provided, the file will be written to the remote host using
        the batch system's `write_remote_file` method. Otherwise, the file is written locally.

        Args:
            file (Path): Path where the YAML file should be written.
            host (str | None): Optional hostname of the remote machine where the file should be written.
                If None, the file is written locally.

        Raises:
            QQError: If the file cannot be created, reached, or written to.
        """
        label = type(self)._file_label
        comment = type(self)._file_comment

        try:
            content = f"# {comment}\n" + self._to_yaml() + "\n"

            if host:
                BatchSystem = BatchInterface.from_env_var_or_guess()

                # remote file
                logger.debug(f"Exporting {label} into '{file}' on '{host}'.")
                BatchSystem.write_remote_file(host, file, content)
            else:
                # local file
                logger.debug(f"Exporting {label} into '{file}'.")
                with file.open("w") as output:
                    output.write(content)
        except Exception as e:
            raise QQError(f"Cannot create or write to file '{file}': {e}") from e

    @staticmethod
    def _read_local_yaml(file: Path) -> dict[str, object]:
        """Read and parse a local YAML file."""
        try:
            with file.open("r") as fh:
                return yaml.load(fh, Loader=SafeLoader)
        except FileNotFoundError:
            raise QQError(f"File '{file}' does not exist.")
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

    def _to_yaml(self) -> str:
        """
        Serialize the instance to a YAML string.

        Returns:
            str: YAML representation of the object.
        """
        return yaml.dump(
            self._to_dict(),
            default_flow_style=False,
            sort_keys=False,
            Dumper=Dumper,
        )
