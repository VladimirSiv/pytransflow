"""
Defines classes and methods related to the ``FlowVariables``
"""

from typing import Any, Dict, Optional
from pytransflow.exceptions import (
    FlowVariableAlreadyExistsException,
    FlowVariableDoesNotExistException,
)


class FlowVariables:
    """Defines variables that will be accessed on a flow level. Each transformation can
    access a flow variable and use it

    Args:
        variables: Flow variables configuration value

    """

    def __init__(self, variables: Optional[Dict[str, Any]] = None) -> None:
        self._variables: Dict[str, Any] = variables if variables is not None else {}

    def set_variable(self, name: str, value: Any) -> None:
        """Sets new variable

        Args:
            name: Variable name
            value: Variable value

        Raises:
            FlowVariableAlreadyExists - When variable is not found

        """
        if name in self._variables:
            raise FlowVariableAlreadyExistsException(name)
        self._variables[name] = value

    def update_variable(self, name: str, value: Any) -> None:
        """Updates existing variable

        Args:
            name: Variable name
            value: Variable value

        """
        if name not in self._variables:
            raise FlowVariableDoesNotExistException(name)
        self._variables[name] = value

    def delete_variable(self, name: str) -> None:
        """Deletes existing variable

        Args:
            name: Variable name

        """
        if name not in self._variables:
            raise FlowVariableDoesNotExistException(name)
        del self._variables[name]

    def get_variable(self, name: str) -> Any:
        """Returns existing variable

        Args:
            name: Variable name

        Returns:
            Variable value

        """
        if name not in self._variables:
            raise FlowVariableDoesNotExistException(name)
        return self._variables[name]
