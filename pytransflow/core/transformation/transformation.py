"""
Defines ``Transformation`` abstract class
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional
from pytransflow.core.record import Record
from pytransflow.core.flow.variables import FlowVariables
from pytransflow.core.transformation.configuration import TransformationConfiguration
from pytransflow.core.transformation.exception_handler import ExceptionHandler


logger = logging.getLogger(__name__)


class Transformation(ABC):
    """Implements Transformation logic

    This class is a base class for all transformations and implements methods
    that are generic for all transformations.

    Args:
        config: Transformation Configuration

    Attributes:
        config: Transformation configuration defined in a flow
        variables: Flow variables

    """

    def __init__(self, config: TransformationConfiguration) -> None:
        self.config = config
        self.variables: Optional[FlowVariables] = None

    def __repr__(self) -> str:
        return f"{self.config.schema.__class__.__name__}({self.config})"

    @ExceptionHandler()
    def execute(self, record: Record) -> Record:
        """Executes abstract method transform with exception handler decorator

        Args:
            record: Record to be processed

        Returns:
            Record object

        """
        return self.transform(record)

    @abstractmethod
    def transform(
        self,
        record: Record,
    ) -> Record:
        """Transforms initial record

        Args:
            record: Record to be processed

        Returns:
            Record object

        """
