"""
Defines classes and methods related to the ``FailedRecord``
"""

from __future__ import annotations
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytransflow.core.transformation import TransformationConfiguration
    from pytransflow.core.record.record import Record

logger = logging.getLogger(__name__)


class FailedRecord:
    """Implements FailedRecord class

    Attributes:
        record: Record that failed to be processed
        transformation_name: Name of the transformation that failed
        transformation_configuration: Configuration of the transformation
        error: Exception

    """

    def __init__(
        self,
        record: Record,
        transformation_name: str,
        transformation_configuration: TransformationConfiguration,
        error: Exception,
    ) -> None:
        self.record = record
        self.transformation_name = transformation_name
        self.transformation_configuration = transformation_configuration
        self.error = error

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, FailedRecord):
            if (
                __o.record == self.record
                and __o.transformation_name == self.transformation_name
                and __o.transformation_configuration == self.transformation_configuration
                and __o.error == self.error
            ):
                return True
        return False

    def __repr__(self) -> str:
        repr_ = ", ".join([f"{k}={v}" for k, v in self.__dict__.items()])
        return f"FailedRecord({repr_})"
