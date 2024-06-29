"""
Defines classes and method related to the ``Datasets``
"""

import logging
from typing import Dict, Any, List
from pytransflow.core.record import Record
from pytransflow.core.flow.pipeline import FlowPipelineState

logger = logging.getLogger(__name__)


class FailedDataset:
    """Implements Failed Dataset

    This dataset contains original record and all failed records that it encountered during the
    processing of the original record within a single pipeline job

    Args:
        state: Flow Pipeline State

    Attribute:
        failed_records: Records that failed the processing
        record: Original record that was submitted to the pipeline
        run_id: Pipeline job run ID

    """

    def __init__(self, state: FlowPipelineState) -> None:
        self.failed_records = state.failed_records
        self.record = state.init_record
        self.run_id = state.run_id


class Datasets:
    """Implements Datasets

    Datasets object stores and controls the datasets of the Flow. It creates new
    datasets, adds records to existing datasets, and stores failed records

    Attributes:
        datasets: Contains datases and records
        failed_records: Contains failed records
        input_records: Input records


    """

    def __init__(self) -> None:
        self.datasets: Dict[str, List[Record]] = {}
        self.failed_records: List[FailedDataset] = []
        self.input_records: List[Record] = []

    def add_failed_records(
        self,
        state: FlowPipelineState,
    ) -> None:
        """Add failed record to the Dataset

        Args:
            state: Flow pipeline state

        """
        self.failed_records.append(FailedDataset(state))

    def add_input_records(
        self,
        records: List[Dict[Any, Any]],
    ) -> None:
        """Adds initial records to default dataset

        Args:
            records: Initial records

        """
        logger.debug("Initializing State with: %s", records)
        self.input_records = [Record(x) for x in records]

    def add_to_dataset(
        self,
        dataset: str,
        data: List[Record],
    ) -> None:
        """Adds records to a dataset

        Args:
            dataset: Dataset
            data: Records to add

        """
        logger.debug("Adding to dataset: %s, records: %s", dataset, data)
        self._add_dataset(dataset, data)

    def get_dataset_names(self) -> List[str]:
        """Returns all available datasets"""
        return list(self.datasets.keys())

    def _add_dataset(
        self,
        dataset: str,
        data: List[Record],
    ) -> None:
        """Adds records to a dataset

        Args:
            dataset: Dataset
            data: Records

        """
        if not self._contains_dataset(dataset):
            self._create_dataset(dataset)
        records = self.datasets[dataset]
        records.extend(data)
        self.datasets[dataset] = records

    def _create_dataset(
        self,
        dataset: str,
    ) -> None:
        """Creates a new dataset

        Args:
            dataset: Dataset

        """
        self.datasets[dataset] = []

    def _contains_dataset(
        self,
        dataset: str,
    ) -> bool:
        """Checks if a dataset exists in the ``State``

        Returns:
            bool: True if present, otherwise False

        """
        if dataset in self.datasets:
            return True
        return False
