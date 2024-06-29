import pytest
from pydantic import ValidationError
from pytransflow.transformations.rename import RenameTransformation, RenameTransformationSchema
from pytransflow.core.record import Record
from pytransflow.core.transformation import TransformationConfiguration


def test_configuration_missing_field():
    config = {"output": "d"}
    with pytest.raises(ValidationError):
        TransformationConfiguration(RenameTransformationSchema, config)


def test_configuration_missing_output():
    config = {"field": "a"}
    with pytest.raises(ValidationError):
        TransformationConfiguration(RenameTransformationSchema, config)


def test_configuration_happy():
    config = {"field": "a", "output": "b"}
    t_config = TransformationConfiguration(RenameTransformationSchema, config)
    RenameTransformation(t_config)


def test_transform_happy():
    config = {"field": "a", "output": "d"}
    initial_record = Record({"a": 1, "b": 2})
    expected_output = Record({"d": 1, "b": 2})

    t_config = TransformationConfiguration(RenameTransformationSchema, config)
    transformation = RenameTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result
