# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path

from qq_lib.batch.interface import (
    AnyBatchClass,
    BatchInterface,
    BatchJobInterface,
    BatchNodeInterface,
    BatchQueueInterface,
)
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)


class Cder:
    """
    Retrieve and provide the input directory for a specific job in the configured batch system.
    """

    def __init__(
        self,
        BatchSystem: AnyBatchClass,
        job_id: str,
    ):
        """
        Initialize the Cder instance with a batch system interface and job ID.

        Args:
            BatchSystem (AnyBatchClass): The batch system which manages the job.
            job_id (str): Identifier of the job to query.
        """
        self._job_id = job_id
        self._BatchSystem = BatchSystem

    def cd(self) -> str:
        """
        Retrieve the input directory for the job and return it as a string.

        Returns:
            str: Path to the input directory of the job.

        Raises:
            QQError: If the job does not exist.
        """
        path = Cder._get_input_dir_from_job_id(self._BatchSystem, self._job_id)
        logger.debug(f"Changing directory to '{path}'.")
        return str(path)

    @staticmethod
    def _get_input_dir_from_job_id[
        TBatchJob: BatchJobInterface,
        TBatchQueue: BatchQueueInterface,
        TBatchNode: BatchNodeInterface,
    ](
        BatchSystem: type[BatchInterface[TBatchJob, TBatchQueue, TBatchNode]],
        job_id: str,
    ) -> Path | None:
        """
        Query the batch system for the input directory of a job.

        Args:
            BatchSystem (type[BatchInterface]): The batch system which manages the job.
            job_id (str): Identifier of the job to query.

        Returns:
            Path: Path object pointing to the job's input directory.

        Raises:
            QQError: If the specified job does not exist.
        """
        job_info: BatchJobInterface = BatchSystem.get_batch_job(job_id)

        if job_info.is_empty():
            raise QQError(f"Job '{job_id}' does not exist.")

        if not (input_dir := job_info.get_input_dir()):
            raise QQError(f"Job '{job_id}' has an unknown input directory.")

        return input_dir
