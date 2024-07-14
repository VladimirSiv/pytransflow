"""
Exceptions module exports
"""

from pytransflow.exceptions.base import TransflowBaseException
from pytransflow.exceptions.analyzer import (
    AnalyzerBaseException,
    ConditionNotMetException,
    FieldDoesNotExistException,
    OutputAlreadyExistsException,
)
from pytransflow.exceptions.flow import (
    FlowBaseException,
    FlowFailedException,
    FlowInstantFailException,
    FlowSchemaNotProperlyDefinedException,
    FlowConfigurationException,
    FlowConfigurationFileNotFoundException,
    TransformationDoesNotExistException,
    SchemaFileNotFoundException,
    SchemaFailedToLoadException,
    SchemaNotProperlyDefinedException,
    FlowVariableAlreadyExistsException,
    FlowVariableDoesNotExistException,
    FlowFailScenarioValueNotProperlyDefinedException,
    FlowFailScenarioException,
    FlowPipelineInstantFailException,
)
from pytransflow.exceptions.transformation import (
    TransformationBaseException,
    FieldWrongTypeException,
    SchemaValidationException,
)
from pytransflow.exceptions.controller import (
    ControllerBaseException,
    ControllerTransformationFailedException,
)
from pytransflow.exceptions.configuration import (
    TransflowConfigurationBaseException,
    PathNotDefinedProperlyException,
)
from pytransflow.exceptions.records import RecordBaseException, RecordAddException


__all__ = [
    "TransflowBaseException",
    "AnalyzerBaseException",
    "ConditionNotMetException",
    "FieldDoesNotExistException",
    "OutputAlreadyExistsException",
    "FlowBaseException",
    "FlowSchemaNotProperlyDefinedException",
    "FlowConfigurationFileNotFoundException",
    "FlowFailedException",
    "FlowInstantFailException",
    "FlowConfigurationException",
    "SchemaFileNotFoundException",
    "SchemaFailedToLoadException",
    "SchemaNotProperlyDefinedException",
    "FlowVariableAlreadyExistsException",
    "FlowVariableDoesNotExistException",
    "FlowFailScenarioValueNotProperlyDefinedException",
    "FlowFailScenarioException",
    "FlowPipelineInstantFailException",
    "TransformationDoesNotExistException",
    "TransformationBaseException",
    "FieldWrongTypeException",
    "SchemaValidationException",
    "ControllerBaseException",
    "ControllerTransformationFailedException",
    "TransflowConfigurationBaseException",
    "PathNotDefinedProperlyException",
    "RecordBaseException",
    "RecordAddException",
]
