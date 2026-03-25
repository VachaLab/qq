# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from qq_lib.batch.slurm import SlurmNode
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)


class SlurmLumiNode(SlurmNode):
    """
    Stores metadata for a single Lumi node.
    """

    # LUMI counts each CPU core as two since each core supports SMT
    # qq wants to show real CPU cores, not threads in the output of qq nodes,
    # so we need to override some default behavior

    def get_n_cpus(self) -> int | None:
        if (cpus := super().get_n_cpus()) is not None:
            if cpus % 2 != 0:
                logger.warning(
                    f"Suspicious number of CPU threads ({cpus}) on node {self.get_name()}. Should be divisible by two but is not."
                )
            return cpus // 2

        return None

    def get_n_free_cpus(self) -> int | None:
        if not (cpus := self.get_n_cpus()):
            return None

        allocated_threads = self._get_int_resource("CPUAlloc") or 0
        if allocated_threads % 2 != 0:
            logger.warning(
                f"Suspicious number of allocated CPU threads ({cpus}) on node {self.get_name()}. Should be divisible by two but is not."
            )

        return cpus - (allocated_threads // 2)
