"""
Defines classes and methods related to the ``Flow``
"""

import logging
from typing import List, Dict, Any, Optional
from pytransflow.core.record import Record
from pytransflow.exceptions import (
    FlowFailedException,
    FlowInstantFailException,
    FlowPipelineInstantFailException,
)
from pytransflow.core.configuration import TransflowConfiguration
from pytransflow.core.flow.dataset import Datasets, FailedDataset
from pytransflow.core.flow.configuration import FlowConfiguration
from pytransflow.core.flow.loader import FlowConfigurationLoader
from pytransflow.core.flow.statistics import FlowStatistics
from pytransflow.core.flow.pipeline import FlowPipeline, FlowPipelineResult
from pytransflow.core.flow.parallel import ParallelFlow

logger = logging.getLogger(__name__)


class Flow:
    """Implements Flow

    Flow class is responsible for setting everything that is required for
    record processing.

    Loading the flow configuration can be done from a `.yml` file through the
    `name` argument or directly providing a flow configuration through `config`
    argument as a `dict` object. Note: `config` argument has precedence over
    `name`.

    Args:
        name: Name of the flow configuration file i.e. `<name>.yml`
        config: Flow configuration as a `dict` object

    Attributes:
        statistics: Flow Statistics object

    """

    def __init__(
        self,
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._config: FlowConfiguration = FlowConfigurationLoader.load(name, config)
        self._datasets: Datasets = Datasets()
        self.statistics: FlowStatistics = FlowStatistics(self._datasets)
        self._pipeline: FlowPipeline = FlowPipeline(
            self._config.transformations, self._config.instant_fail
        )

        if self._config.path_separator is not None:
            TransflowConfiguration().path_separator = self._config.path_separator

    @property
    def datasets(self) -> Dict[str, List[Record]]:
        """Returns datasets"""
        return self._datasets.datasets

    @property
    def failed_records(self) -> List[FailedDataset]:
        """Returns failed dataset"""
        return self._datasets.failed_records

    def process(self, records: List[Dict[str, Any]]) -> None:
        """Prepares inital dataset and initializes processing of records,
        either in parallel or single-threaded mode

        Args:
            records: Records to process

        """
        self._datasets.add_input_records(records)
        self.statistics.before_processing()
        if self._config.parallel:
            self._multi_processing()
        else:
            self._single_processing()
        self.statistics.after_processing()
        self._config.fail_scenarios.evaluate(self._datasets, self.statistics)

    def _single_processing(self) -> None:
        """Executes flow in a single process"""
        logger.debug("Initializing flow processing in single-process mode")
        for record in self._datasets.input_records:
            try:
                result = self._pipeline.submit(record)
                self._add_pipeline_result(result)
            except FlowPipelineInstantFailException as i_err:
                raise FlowInstantFailException() from i_err
            except Exception as e_err:
                raise FlowFailedException(e_err) from e_err

    def _multi_processing(self) -> None:
        """Executes flow in multiprocessing mode"""
        logger.debug("Initializing flow processing in multi-processing mode")
        mt_flow = ParallelFlow(self._datasets.input_records, self._config)
        results = mt_flow.execute()
        for result in results:
            self._add_pipeline_result(result)

    def _add_pipeline_result(self, result: FlowPipelineResult) -> None:
        if not result.success:
            self._datasets.add_failed_records(result.state)
            return

        for name, records in result.state.dataset.items():
            self._datasets.add_to_dataset(name, records)
