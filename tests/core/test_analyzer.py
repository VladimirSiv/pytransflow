import pytest
from pytransflow.transformations.rename import RenameTransformationSchema, RenameTransformation
from pytransflow.core.record import Record
from pytransflow.core.analyzer import Analyzer
from pytransflow.core.transformation import TransformationConfiguration
from pytransflow.exceptions import (
    OutputAlreadyExistsException,
    FieldDoesNotExistException,
)


def test_init_analyzer():
    record = Record({"a": 1})
    config = TransformationConfiguration(
        config={"field": "a", "output": "b"}, schema=RenameTransformationSchema
    )
    transformation = RenameTransformation(config)
    analyzer = Analyzer(transformation, record)

    assert analyzer.config == config
    assert analyzer.record == record


def test_analyzer_happy():
    record = Record({"a": 1})
    config = TransformationConfiguration(
        config={
            "field": "a",
            "output": "b",
        },
        schema=RenameTransformationSchema,
    )
    transformation = RenameTransformation(config)
    analyzer = Analyzer(transformation, record)
    assert analyzer.should_perform_transformation() == True


def test_analyzer_output_already_exists():
    record = Record({"a": 1, "b": 2})
    config = TransformationConfiguration(
        config={
            "field": "a",
            "output": "b",
        },
        schema=RenameTransformationSchema,
    )
    transformation = RenameTransformation(config)
    analyzer = Analyzer(transformation, record)
    with pytest.raises(OutputAlreadyExistsException):
        analyzer.should_perform_transformation()


def test_analyzer_required_field_not_present():
    record = Record({})
    config = TransformationConfiguration(
        config={
            "field": "a",
            "output": "b",
        },
        schema=RenameTransformationSchema,
    )
    transformation = RenameTransformation(config)
    analyzer = Analyzer(transformation, record)
    with pytest.raises(FieldDoesNotExistException):
        analyzer.should_perform_transformation()


def test_analyzer_condition_not_met():
    record = Record({"a": 1})
    config = TransformationConfiguration(
        config={
            "field": "a",
            "output": "b",
            "condition": "record['a'] != 1",
        },
        schema=RenameTransformationSchema,
    )
    transformation = RenameTransformation(config)
    analyzer = Analyzer(transformation, record)
    assert analyzer.should_perform_transformation() == False
