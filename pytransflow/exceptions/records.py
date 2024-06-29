"""
Defines class related to ``Records`` exceptions
"""

from typing import Any
from pytransflow.exceptions.base import TransflowBaseException


class RecordBaseException(TransflowBaseException):
    """Implements Record Base Exception"""


class RecordAddException(RecordBaseException):
    """Implements Record Add Exception"""

    def __init__(self, path: str, value: Any) -> None:
        super().__init__(f"Failed to add element to a record, path: {path}, value: {value}")
