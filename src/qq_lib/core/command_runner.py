# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import getpass
import logging
import sys
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, NoReturn, Self

from qq_lib.batch.interface import BatchInterface, BatchJobInterface
from qq_lib.core.common import get_info_files, translate_server
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.info import Informer


class CommandRunner:
    """
    Runs a job operation against one or more qq jobs.

    Resolves and prepares informers in parallel using a thread pool, then
    executes the callback serially on the main thread in the original order,
    starting as soon as the next-in-order preparation completes.

    All exceptions are caught internally and converted to `sys.exit` calls.
    Specific exception types can be handled gracefully via `on_exception`.

    Attributes:
        n_jobs (int): The total number of jobs to run.
        encountered_errors (dict[int, Exception]): A dictionary mapping
            job indices to exceptions encountered during preparation or execution.
    """

    def __init__(
        self,
        job_ids: tuple[str, ...],
        directories: tuple[Path, ...],
        all: bool,
        server: str | None,
        callback: Callable,
        logger: logging.Logger,
        *args: Any,
        n_threads: int = 1,
        **kwargs: Any,
    ):
        """
        Initialize a CommandRunner.

        Args:
            job_ids (tuple[str, ...]): List of batch job IDs to operate on.
            directories (tuple[Path, ...]): List of directories to search for qq jobs in.
                If `job_ids` and `directories` are both empty and `all` is `False`, the current directory is searched for jobs.
            all (bool): Collect all qq jobs for the given user on the given server.
            server (str | None): The batch server to collect qq jobs from. If `None`, the local batch server is used.
                The server can be specified as a shortcut.
            callback (Callable): The operation to perform on each resolved Informer.
                The informer is passed as the first argument, followed by `*args` and `**kwargs`.
            logger (logging.Logger): Logger instance used for error and critical messages.
                Should be the module-level logger of the calling CLI module.
            *args (Any): Additional positional arguments forwarded to `callback`.
            n_threads (int): Number of threads for parallel informer resolution. Defaults to 1 (serial).
            **kwargs (Any): Additional keyword arguments forwarded to `callback`.
        """
        self._job_ids = job_ids
        self._directories = directories
        self._all = all
        self._user = getpass.getuser()
        self._server = translate_server(server) if server else None
        if not self._job_ids and not self._directories and not all:
            self._directories = [Path.cwd()]

        self._callback = callback
        self._args = args
        self._kwargs = kwargs

        self._batch_system = BatchInterface.from_env_var_or_guess()
        self._logger = logger
        self._exception_handlers: dict[type[Exception], Callable] = {}
        self._n_threads = n_threads

        self.n_jobs = 0
        self.current_iteration = 0
        self.encountered_errors: dict[int, Exception] = {}

    def on_exception(self, exc_type: type[Exception], handler: Callable) -> Self:
        """
        Register an exception handler for a specific exception type.

        Registered handlers are invoked when the callback or the preparation
        step raises the given exception type. Unregistered exception types
        propagate up and cause the process to exit.

        Args:
            exc_type (type[Exception]): The exception type to handle.
            handler (Callable): Function to call when `exc_type` is raised.
                Must accept two arguments: the exception instance and
                a reference to this `CommandRunner`.

        Returns:
            Self for chaining.
        """
        self._exception_handlers[exc_type] = handler
        return self

    def run(self) -> NoReturn:
        """
        Resolve all jobs, execute the callback for each, and exit the process.

        Resolves informers from job IDs or info files in the target directory,
        prepares them in parallel, and executes the registered callback for each
        job in the original order. Registered exception handlers are invoked for
        known error types; all other exceptions cause the process to exit.

        This method never returns. It always terminates with `sys.exit`:
            - Exit code 0 on success.
            - Exit code `CFG.exit_codes.default` on `QQError`.
            - Exit code `CFG.exit_codes.unexpected_error` on any other exception.
        """
        try:
            targets = self._build_targets()
            self._run_pipeline(targets)
            sys.exit(0)
        except QQError as e:
            self._logger.error(e)
            sys.exit(CFG.exit_codes.default)
        except Exception as e:
            self._logger.critical(e, exc_info=True, stack_info=True)
            sys.exit(CFG.exit_codes.unexpected_error)

    def _build_targets(self) -> list[Callable[[], Informer]]:
        """
        Build a list of callables that each resolve and prepare one Informer.

        If job IDs were provided, each target resolves via `Informer.from_job_id`.
        Otherwise, the specified (or current) directory is searched for qq info files
        and each target resolves via `Informer.from_file`.

        Each target then also reloads the informer's batch info.

        Returns:
            list[Callable[[], Informer]]: One callable per job.

        Raises:
            QQError: If no job IDs were given and no info files were found in the current directory.
        """
        if self._server and not self._all:
            self._logger.warning(
                "Server is only used with --all. Ignoring server option."
            )
            self._server = None

        targets: list[Callable[[], Informer]] = []

        batch_jobs = []
        if self._job_ids:
            batch_jobs.extend(
                self._batch_system.get_batch_jobs_from_ids(list(self._job_ids))
            )
        if self._all:
            batch_jobs.extend(
                self._batch_system.get_unfinished_batch_jobs(self._user, self._server)
            )

        if batch_jobs:
            targets.extend(self._build_targets_from_batch_jobs(batch_jobs))

        if self._directories:
            targets.extend(self._build_targets_from_files(list(self._directories)))

        if not targets:
            if not self._job_ids and not self._all:
                raise QQError("No qq job info file found.")
            raise QQError("No jobs found.")

        return targets

    def _build_targets_from_batch_jobs(
        self,
        jobs: list[BatchJobInterface],
    ) -> list[Callable[[], Informer]]:
        """
        Build preparation targets from a list of batch jobs.

        Args:
            jobs (list[BatchJobInterface]): The batch jobs to resolve.

        Returns:
            list[Callable[[], Informer]]: Per-job preparation callables.
        """
        return [lambda bj=batch_job: Informer.from_batch_job(bj) for batch_job in jobs]

    def _build_targets_from_files(
        self,
        directories: list[Path],
    ) -> list[Callable[[], Informer]]:
        """
        Build preparation targets from qq info files across multiple directories.

        Directories are searched in order. Each target loads an `Informer`
        from a file and queries the batch system for its info individually.

        Args:
            directories (list[Path]): Directories to search for info files.

        Returns:
            list[Callable[[], Informer]]: Per-job preparation callables.
        """

        def _resolve_and_prepare(path: Path) -> Informer:
            informer = Informer.from_file(path)
            informer.load_batch_info()
            return informer

        info_files: list[Path] = []
        for directory in directories:
            info_files.extend(get_info_files(directory))

        return [lambda f=info_file: _resolve_and_prepare(f) for info_file in info_files]

    def _run_pipeline(self, targets: list[Callable[[], Informer]]) -> None:
        """
        Run the prepare-then-execute pipeline.

        Submits all targets to a thread pool for parallel preparation.
        The main thread waits on a condition variable and executes each
        job's callback as soon as it is the next in order, ensuring output
        and side effects follow the original job order.

        Failed preparations and executions are passed to `_handle_error`.

        Args:
            targets: List of callables that each resolve and prepare one Informer.
        """
        self.n_jobs = len(targets)
        results: list[Informer | Exception | None] = [None] * self.n_jobs
        lock = threading.Lock()
        ready = threading.Condition(lock)

        def prepare(index: int, target: Callable[[], Informer]) -> None:
            """
            Execute a single target and store its result or exception.

            Notifies the main thread upon completion so it can check whether
            the next-in-order result is available.

            Args:
                index (int): The position of this target in the original order.
                target (Callable[[], Informer]): The callable to execute.
            """
            try:
                result: Informer | Exception = target()
            except Exception as e:
                result = e

            with ready:
                results[index] = result
                ready.notify()

        with ThreadPoolExecutor(max_workers=self._n_threads) as executor:
            # submit all preparation tasks to the thread pool
            for i, target in enumerate(targets):
                executor.submit(prepare, i, target)

            # process results in order on the main thread
            next_index = 0
            with ready:
                while next_index < self.n_jobs:
                    # block the main thread until the next result is available
                    # later jobs may finish first, but we wait for the one we need
                    while results[next_index] is None:
                        ready.wait()

                    self.current_iteration += 1
                    result = results[next_index]
                    next_index += 1

                    # handle error or execute the callback
                    if isinstance(result, Exception):
                        self._handle_error(result)
                    elif isinstance(result, Informer):
                        self._execute(result)
                    else:
                        raise ValueError(
                            f"Unexpected result type: {type(result)}. This is a bug, please report it."
                        )

    def _execute(self, informer: Informer) -> None:
        """
        Run the callback on a prepared informer.

        Args:
            informer (Informer): A resolved and prepared Informer.
        """
        try:
            self._callback(informer, *self._args, **self._kwargs)
        except tuple(self._exception_handlers.keys()) as e:
            self._handle_error(e)

    def _handle_error(self, error: Exception) -> None:
        """
        Handle an exception using registered handlers, or re-raise.

        Args:
            error (Exception): The exception to handle.

        Raises:
            Exception: If no handler is registered for the exception type.
        """
        self.encountered_errors[self.current_iteration] = error
        handler = self._exception_handlers.get(type(error))
        if handler:
            handler(error, self)
        else:
            raise error
