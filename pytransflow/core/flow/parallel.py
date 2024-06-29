"""
Defines classes and methods related to the ``ParallelFlow``
"""

import logging
import os
from typing import List, Optional
from multiprocessing import Pool
from pytransflow.core.record import Record
from pytransflow.core.transformation import Transformation
from pytransflow.core.flow.pipeline import FlowPipeline, FlowPipelineResult
from pytransflow.core.flow.configuration import FlowConfiguration


logger = logging.getLogger(__name__)


def process_batch(  # pragma: no cover
    records: List[Record],
    transformations: List[Transformation],
    instant_fail: bool,
) -> List[FlowPipelineResult]:
    """Executes processing task

    Args:
        records: Batch of records to be processed
        transformations: List of transformations to be applied
        instant_fail: Configuration for instant failure

    """
    pipeline = FlowPipeline(transformations, instant_fail)
    result = []
    for record in records:
        result.append(pipeline.submit(record))
    return result


class ParallelFlow:
    """Implements Flow execution in parallel mode

    Attributes:
        batch: Number of records in a batch
        cores: Number of cores used for multiprocessing
        instant_fail: Instant fail configuration
        transformations: List of transformation to be applied
        records: Records to be processed

    """

    def __init__(
        self,
        records: List[Record],
        config: FlowConfiguration,
    ) -> None:
        self.records = records
        self.instant_fail = config.instant_fail
        self.transformations = config.transformations
        self.batch = self._set_batch(config.batch)
        self.cores = self._set_cores(config.cores)

    def execute(self) -> List[FlowPipelineResult]:
        """Executes the multiprocessing pool

        Returns:
            List of FlowPipelineResults

        """
        logger.debug(
            "Initializing Multiprocessing Pool, cores: %s, batch: %s",
            self.cores,
            self.batch,
        )
        with Pool(self.cores) as pool:
            processes = []
            for i in range(0, len(self.records), self.batch):
                batch = self.records[i : i + self.batch]
                process = pool.apply_async(
                    process_batch, (batch, self.transformations, self.instant_fail)
                )
                processes.append(process)

            result = []
            for process in processes:
                result.extend(process.get())

            return result

    def _set_batch(self, batch: Optional[int]) -> int:
        """Sets batch size

        Args:
            batch: Batch size flow configuration

        Returns:
            Batch size

        """
        if batch is not None:
            return batch
        return len(self.records)

    @staticmethod
    def _set_cores(cores: Optional[int]) -> int:
        """Sets number of cores

        Args:
            cores: Number of cores flow configuration

        Returns:
            Number of cores

        """
        num_cores = os.cpu_count()
        if num_cores is None:
            raise RuntimeError("Cannot determine number of available cores")
        if cores is None:
            return num_cores
        if cores > num_cores:
            raise RuntimeError(f"Desired number of cores: {cores}, available: {num_cores}")
        return cores
