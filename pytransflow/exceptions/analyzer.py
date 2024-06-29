"""
Defines class related to ``Analyzer`` exceptions
"""

from pytransflow.exceptions.base import TransflowBaseException


class AnalyzerBaseException(TransflowBaseException):
    """Implements Analyzer Base Exception"""


class ConditionNotMetException(AnalyzerBaseException):
    """Implements Condition Not Met Exception"""

    name = "condition_not_met"

    def __init__(self, condition: str) -> None:
        super().__init__(f"Condition '{condition}' not met")


class FieldDoesNotExistException(AnalyzerBaseException):
    """Implements Field Does Not Exist Exception"""

    name = "field_does_not_exist"

    def __init__(self, field: str) -> None:
        super().__init__(f"Field '{field}' does not exist")


class OutputAlreadyExistsException(AnalyzerBaseException):
    """Implements Output Already Exists Exception"""

    name = "output_already_exists"

    def __init__(self, field: object) -> None:
        super().__init__(f"Output field '{field}' already exists")
