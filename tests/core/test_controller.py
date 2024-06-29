import pytest
from pytransflow.core.transformation import TransformationConfiguration
from pytransflow.core.flow.configuration import FlowConfiguration
from pytransflow.core.controller import Controller
from pytransflow.core.record import Record, FailedRecord
from pytransflow.transformations.rename import RenameTransformation, RenameTransformationSchema
from pytransflow.exceptions import (
    ControllerTransformationFailedException,
    FieldDoesNotExistException,
)


# TODO: Rewrite all tests, Controller class is completely refactored

class TestController:
    ...