"""
Defines classes and methods related to the ``FlowStatistics``
"""

from pytransflow.core.flow.dataset import Datasets


class FlowStatistics:
    """Gathers and calculates flow statistics

    Args:
        datasets: Flow datasets

    Attributes:
        datasets: Flow datasets
        number_of_input_records: Number of input records
        number_of_output_datasets: Number of output records
        number_of_failed_records: Number of failed records
        percentage_of_failed_records: Percentage of failed records

    """

    def __init__(self, datasets: Datasets) -> None:
        self.datasets = datasets
        self.number_of_input_records = 0
        self.number_of_output_datasets = 0
        self.number_of_failed_records = 0
        self.percentage_of_failed_records = 0

    def before_processing(self) -> None:
        """Gathers statistics before processing records"""
        self.number_of_input_records = len(self.datasets.input_records)

    def after_processing(self) -> None:
        """Gathers statistics after processing records"""
        datasets = self.datasets.get_dataset_names()
        self.number_of_output_datasets = len(datasets)
        self.number_of_failed_records = len(self.datasets.failed_records)
        self.percentage_of_failed_records = round(
            (self.number_of_failed_records / self.number_of_input_records) * 100
        )
