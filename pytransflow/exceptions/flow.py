"""
Defines class related to ``Flow`` exceptions
"""

from pytransflow.exceptions.base import TransflowBaseException


class FlowBaseException(TransflowBaseException):
    """Implements Flow Base Exception"""


class FlowFailedException(FlowBaseException):
    """Implements Flow Failed Exception"""

    def __init__(self, error: Exception) -> None:
        super().__init__(f"Flow failed, error: {error}")


class FlowInstantFailException(FlowBaseException):
    """Implements Flow Instant Fail Exception"""

    def __init__(self) -> None:
        super().__init__("Flow raised instant fail exception")


class FlowConfigurationException(FlowBaseException):
    """Implements Flow Configuration Exception"""


class FlowSchemaNotProperlyDefinedException(FlowBaseException):
    """Implements Flow Schema Not Properly Defined Exception"""

    def __init__(self, flow_name: str) -> None:
        super().__init__(
            f"Flow schema for flow '{flow_name}'is not properly defined, "
            "please check the .yaml file."
        )


class FlowConfigurationFileNotFoundException(FlowBaseException):
    """Implements Flow Configuration File Not Found Exception"""

    def __init__(self, name: str) -> None:
        super().__init__(f"Flow configuration file for '{name}' not found")


class TransformationDoesNotExistException(FlowBaseException):
    """Implements Transformation Does Not Exist Exception"""

    def __init__(self, name: str) -> None:
        super().__init__(f"Transformation '{name}' does not exist")


class SchemaFileNotFoundException(FlowBaseException):
    """Implements Schema File Not Found Exception"""

    def __init__(self, filename: str) -> None:
        super().__init__(f"Schema defined in file '{filename}' could not be found")


class SchemaFailedToLoadException(FlowBaseException):
    """Implements Schema Failed To Load Exception"""

    def __init__(self, name: str) -> None:
        super().__init__(f"Failed to load schema: {name}")


class SchemaNotProperlyDefinedException(FlowBaseException):
    """Implements Schema Not Properly Defined Exception"""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"Schema class '{name}' should be a subclass of BaseModel class from pydantic"
        )


class FlowVariableAlreadyExistsException(FlowBaseException):
    """Implements Flow Variable Already Exists Exception"""

    def __init__(self, name: str) -> None:
        super().__init__(f"Flow variable '{name}' already exists")


class FlowVariableDoesNotExistException(FlowBaseException):
    """Implements Flow Variable Does Not Exist Exception"""

    def __init__(self, name: str) -> None:
        super().__init__(f"Flow variable '{name}' doesn't exist")


class FlowFailScenarioValueNotProperlyDefinedException(FlowBaseException):
    """Implements Flow Fail Scenario Value Not Properly Defined Exception"""

    def __init__(self, choice: str) -> None:
        super().__init__(f"Flow Fail Scenario '{choice}' not defined properly")


class FlowFailScenarioException(FlowBaseException):
    """Implements Flow Fail Scenario Exception"""

    def __init__(self, choice: str, message: str) -> None:
        super().__init__(f"Flow Fail Scenario '{choice}': {message}")


class FlowPipelineInstantFailException(FlowBaseException):
    """Implements Flow Pipeline Instant Fail Exception"""

    def __init__(self, error: str) -> None:
        super().__init__(
            f"Flow Pipeline raised the instant fail in the flow caused by the error: {error}"
        )
