"""
Defines class related to ``TransflowConfiguration`` exceptions
"""

from pathlib import Path
from pytransflow.exceptions.base import TransflowBaseException


class TransflowConfigurationBaseException(TransflowBaseException):
    """Implements Transflow Configuration Base Exception"""


class PathNotDefinedProperlyException(TransflowConfigurationBaseException):
    """Implements Path Not Defined Properly Exception"""

    def __init__(self, parameter: str, path: Path) -> None:
        super().__init__(f"{parameter}: '{path}' - doesn't exist")
