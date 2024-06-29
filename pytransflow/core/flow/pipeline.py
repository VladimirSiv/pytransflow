"""
Defines classes and methods related to the ``FlowPipeline``
"""

import logging
from typing import List
from copy import deepcopy
from uuid import uuid4
from pytransflow.core.resolver import Resolver
from pytransflow.core.condition import Condition
from pytransflow.core.record import Record, FailedRecord
from pytransflow.core.controller import Controller
from pytransflow.core.transformation import (
    Transformation,
    TransformationConfiguration,
    OutputDataset,
)
from pytransflow.exceptions import FlowPipelineInstantFailException, ConditionNotMetException
from pytransflow.core.configuration import TransflowConfiguration

logger = logging.getLogger(__name__)


class FlowPipelineState:
    """Implements Flow Pipeline State that's kept during processing of a single record

    Args:
        pipeline_id: Pipeline ID
        record: Submitted record

    Attributes:
        pipeline_id: Pipeline ID
        run_id: Flow Pipeline run ID
        init_record: Submitted record
        dataset: Datasets present during processing
        failed_records: Records that failed processing

    """

    def __init__(self, pipeline_id: str, record: Record) -> None:
        logger.debug("Initialize FlowPipelineState with record: %s", record)
        self.pipeline_id = pipeline_id
        self.run_id = str(uuid4())
        self.init_record = record
        self.dataset = {TransflowConfiguration().default_dataset_name: [record]}
        self.failed_records: List[FailedRecord] = []

    def get_records(self, dataset: str) -> List[Record]:
        """Returns records from a dataset

        Args:
            dataset: Dataset name

        Returns:
            List of records

        """
        if dataset in self.dataset:
            records = self.dataset[dataset]
            del self.dataset[dataset]
            return records
        return []

    def add_transformation_result(self, record: Record, dataset: OutputDataset) -> None:
        """Adds Transformation result to a dataset

        Args:
            record: Transformed record
            dataset: Output dataset

        """
        if dataset.name not in self.dataset:
            self.dataset[dataset.name] = []
        self.dataset[dataset.name].append(deepcopy(record))

    def add_failed_record(self, record: FailedRecord) -> None:
        """Adds failed record to the pipeline state

        Args:
            record: Failed record

        """
        self.failed_records.append(record)


class FlowPipelineResult:
    """Defines Flow Pipeline Result, this object is return when whole pipeline succeeded, record
    failed to be processed, or pipeline unexpectedly failed. The actual behaviour depends on the
    Flow configuration.

    Args:
        success: If processing is successful or not
        state: Pipeline state

    Attributes:
        success: If processing is successful or not
        state: Pipeline state

    """

    def __init__(self, success: bool, state: FlowPipelineState) -> None:
        self.success = success
        self.state = state


class FlowPipeline:
    """Defines Flow Pipeline class which invokes the processing and handles the intermediary
    datasets

    Args:
        transformations: List of transformations that will be executed
        instant_fail: Flow configuration for instant fail logic

    Attributes:
        transformations: List of transformations that will be executed
        instant_fail: Instant fail configuration
        pipeline_id: Pipeline ID

    """

    def __init__(
        self,
        transformations: List[Transformation],
        instant_fail: bool,
    ) -> None:
        self.transformations = transformations
        self.instant_fail = instant_fail
        self.pipeline_id = str(uuid4())

    def submit(self, record: Record) -> FlowPipelineResult:
        """Creates a Flow Pipeline State instance and starts the processing

        Args:
            record: Record to be processed

        Returns:
            Flow Pipeline Result

        """
        logger.debug("Record submitted to pipeline id: %s", self.pipeline_id)
        state = FlowPipelineState(self.pipeline_id, record)
        return self._process(state)

    def _process(self, state: FlowPipelineState) -> FlowPipelineResult:
        """Invokes controller to handle the execution of transformations and handles the state
        of Flow Pipeline

        Args:
            state: Flow Pipeline state

        Returns:
            Flow Pipeline Result

        """
        success = True
        for transformation in self.transformations:
            logger.debug("Flow pipeline executing: %s", transformation)
            input_datasets = self._get_input_records(state, transformation.config)
            for record in input_datasets:
                result = Controller.process_record(record, transformation)
                if isinstance(result, FailedRecord):
                    success = False
                    if self.instant_fail:
                        logger.error("Instant fail, error: %s", result.error)
                        raise FlowPipelineInstantFailException(str(result.error))
                    state.add_failed_record(result)
                else:
                    self._handle_output_datasets(
                        state, transformation, result, transformation.config.output_datasets
                    )
                    # state.add_transformation_result(result, transformation.config.output_datasets)

        return FlowPipelineResult(success=success, state=state)

    @staticmethod
    def _handle_output_datasets(
        state: FlowPipelineState,
        transformation: Transformation,
        result: Record,
        datasets: List[OutputDataset],
    ) -> None:
        """Performs condition checks and handles output datasets"""
        for dataset in datasets:
            if dataset.condition is not None:
                condition = Resolver.resolve_condition(dataset.condition, transformation.variables)
                try:
                    Condition.check(condition, result)
                except ConditionNotMetException as c_err:
                    logger.debug(
                        "Output dataset condition not met, dataset: %s, error: %s",
                        dataset.name,
                        c_err,
                    )
                    continue
            state.add_transformation_result(result, dataset)

    @staticmethod
    def _get_input_records(
        state: FlowPipelineState,
        config: TransformationConfiguration,
    ) -> List[Record]:
        """Gathers input records from the Flow Pipeline State

        Args:
            state: Flow Pipeline State
            config: Transformation configuration

        Returns:
            List of Records

        """
        records = []
        for dataset in config.input_datasets:
            records.extend(state.get_records(dataset))
        return records
