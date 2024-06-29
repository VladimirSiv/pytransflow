"""
Defines classes and methods related to the ``SimpleEval``
"""

from typing import Dict, Any
from simpleeval import DEFAULT_FUNCTIONS  # type: ignore


class SimpleEval:
    """Implements a wrapper around `simpleeval` library that allows users to defined additional
    functions that will be used in evaluating processing conditions

    Attributes:
        functions - Evaluating functions

    Note:
        Default functions will be automatically included, allowing users to
        easily override default behaviour

    """

    functions: Dict[str, Any] = {**DEFAULT_FUNCTIONS}

    @classmethod
    def add_function(cls, name: str, function: Any) -> None:
        """Adds a custom function to the simpleeval evaluation

        Args:
            name: Function name that will be used in conditions
            function: Function object

        """
        if not isinstance(name, str):
            raise RuntimeError("Function name should be of type <str>")
        if not hasattr(function, "__call__"):
            raise RuntimeError(
                "Function object is not callable, hence it cannot be used for evaluating."
            )
        cls.functions[name] = function
