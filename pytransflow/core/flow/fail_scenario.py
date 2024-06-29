"""
Defines classes and methods related to the ``FlowFailScenario``
"""

from enum import Enum
from typing import Dict, Any, Optional, List
from pytransflow.core.flow.dataset import Datasets
from pytransflow.core.flow.statistics import FlowStatistics
from pytransflow.exceptions import (
    FlowFailScenarioValueNotProperlyDefinedException,
    FlowFailScenarioException,
)


class FlowFailScenarioChoices(Enum):
    """Defines Flow Fail Scenario Choices"""

    PERCENTAGE = "percentage_of_failed_records"
    FAILED_RECORDS = "number_of_failed_records"
    DATASETS_PRESENT = "datasets_present"
    DATASETS_NOT_PRESENT = "datasets_not_present"


class FlowFailScenario:
    """Handles flow fail scenarios

    Attributes:
        scenarios: Flow fail scenarios

    Args:
        config: Flow fail scenarios configuration

    """

    def __init__(self, config: Optional[Dict[FlowFailScenarioChoices, Any]]) -> None:
        self.scenarios = config if config is not None else {}
        self._validate_choices()

    def _validate_choices(self) -> None:
        """Validates flow fail scenario values for each scenario, checks types and ranges

        Raises:
            FlowFailScenarioValueNotProperlyDefinedException - If some value is not properly set

        """
        for choice, value in self.scenarios.items():
            if choice is FlowFailScenarioChoices.PERCENTAGE:
                if not isinstance(value, int) or (value <= 0 or value > 100):
                    raise FlowFailScenarioValueNotProperlyDefinedException(choice.value)
            if choice is FlowFailScenarioChoices.FAILED_RECORDS:
                if not isinstance(value, int) or value < 0:
                    raise FlowFailScenarioValueNotProperlyDefinedException(choice.value)
            if choice is FlowFailScenarioChoices.DATASETS_PRESENT:
                if not isinstance(value, list) or value == []:
                    raise FlowFailScenarioValueNotProperlyDefinedException(choice.value)
            if choice is FlowFailScenarioChoices.DATASETS_NOT_PRESENT:
                if not isinstance(value, list) or value == []:
                    raise FlowFailScenarioValueNotProperlyDefinedException(choice.value)

    def evaluate(self, datasets: Datasets, statistics: FlowStatistics) -> None:
        """Evaluates flow fail scenarios and triggers flow failure if condition is met

        Args:
            datasets: Flow Datasets
            statistics: Flow statistics

        """
        for choice, value in self.scenarios.items():
            if choice is FlowFailScenarioChoices.PERCENTAGE:
                self._evaluate_percentage(statistics, value)
            if choice is FlowFailScenarioChoices.FAILED_RECORDS:
                self._evaluate_failed_records(statistics, value)
            if choice is FlowFailScenarioChoices.DATASETS_PRESENT:
                self._evaluate_dataset_present(datasets, value)
            if choice is FlowFailScenarioChoices.DATASETS_NOT_PRESENT:
                self._evaluate_dataset_not_present(datasets, value)

    @staticmethod
    def _evaluate_percentage(statistics: FlowStatistics, threshold: int) -> None:
        """Evaluates Percentage flow fail scenario

        Args:
            statistics: Flow statistics
            threshold: Threshold value

        Raises:
            FlowFailScenarioException - If scenario condition is met

        """
        percentage = statistics.percentage_of_failed_records
        if threshold <= percentage:
            raise FlowFailScenarioException(
                FlowFailScenarioChoices.PERCENTAGE.value,
                f"Threshold defined as {threshold}, got {percentage}",
            )

    @staticmethod
    def _evaluate_failed_records(statistics: FlowStatistics, threshold: int) -> None:
        """Evaluates Failed Records flow fail scenario

        Args:
            statistics: Flow Statistics
            threshold: Threshold value

        Raises:
            FlowFailScenarioException - If scenario condition is met

        """
        failed_records = statistics.number_of_failed_records
        if threshold <= failed_records:
            raise FlowFailScenarioException(
                FlowFailScenarioChoices.FAILED_RECORDS.value,
                f"Threshold defined as {threshold}, got {failed_records}",
            )

    @staticmethod
    def _evaluate_dataset_present(datasets: Datasets, values: List[str]) -> None:
        """Evaluates Dataset Present flow fail scenario

        Args:
            datasets: Flow Datasets
            dataset: Dataset name

        Raises:
            FlowFailScenarioException - If scenario condition is met

        """
        dataset_names = datasets.get_dataset_names()
        for dataset in values:
            if dataset in dataset_names:
                raise FlowFailScenarioException(
                    FlowFailScenarioChoices.DATASETS_PRESENT.value,
                    f"Dataset '{dataset}' is present",
                )

    @staticmethod
    def _evaluate_dataset_not_present(datasets: Datasets, values: List[str]) -> None:
        """Evaluates Dataset Not Present flow fail scenario

        Args:
            datasets: Flow Datasets
            dataset: Dataset name

        Raises:
            FlowFailScenarioException - If scenario condition is met

        """
        dataset_names = datasets.get_dataset_names()
        for dataset in values:
            if dataset not in dataset_names:
                raise FlowFailScenarioException(
                    FlowFailScenarioChoices.DATASETS_NOT_PRESENT.value,
                    f"Dataset '{dataset}' is not present",
                )
