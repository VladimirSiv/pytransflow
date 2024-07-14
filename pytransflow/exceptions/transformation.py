"""
Defines class related to ``Transformation`` exceptions
"""

from typing import Type, Any
from pytransflow.exceptions.base import TransflowBaseException


class TransformationBaseException(TransflowBaseException):
    """Implements Transformation Base Exception

    Attribute:
        name: Name of the exception

    """

    name = "transformation_exception"


class FieldWrongTypeException(TransformationBaseException):
    """Implements Field Wrong Type Exception"""

    name = "field_wrong_type"

    def __init__(self, field: str, _type: Type[Any], expected: str) -> None:
        super().__init__(f"Field '{field}' is of type '{_type}', expected: '{expected}'")


class SchemaValidationException(TransformationBaseException):
    """Implements Schema Validation Exception"""

    name = "validation_error"

    def __init__(self, error: str) -> None:
        super().__init__(f"Schema validation error: {error}")
