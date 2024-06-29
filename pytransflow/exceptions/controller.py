"""
Defines class related to ``Controller`` exceptions
"""

from pytransflow.exceptions.base import TransflowBaseException


class ControllerBaseException(TransflowBaseException):
    """Implements Controller Base Exception"""


class ControllerTransformationFailedException(ControllerBaseException):
    """Implements Controller Transformation Failed Exception"""

    def __init__(self, error: Exception) -> None:
        super().__init__(f"Unexpected Controller Transformation Failure, error: {error}")
