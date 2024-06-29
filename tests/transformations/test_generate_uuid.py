import pytest
from unittest.mock import patch
from pydantic import ValidationError
from pytransflow.transformations.generate_uuid import (
    GenerateUuidTransformation,
    GenerateUuidTransformationSchema,
)
from pytransflow.core.record import Record
from pytransflow.core.transformation import TransformationConfiguration


def _mock_uuid():
    return "asd-123-asd-123"


def test_configuration_missing_name():
    config = {}
    with pytest.raises(ValidationError):
        TransformationConfiguration(GenerateUuidTransformationSchema, config)


def test_configuration_happy():
    config = {"output": "a"}
    t_config = TransformationConfiguration(GenerateUuidTransformationSchema, config)
    GenerateUuidTransformation(t_config)


@patch("uuid.uuid4", return_value=_mock_uuid())
def test_transform(mock):
    config = {"output": "b"}
    initial_record = Record({"a": 1})
    expected_output = Record({"a": 1, "b": "asd-123-asd-123"})

    t_config = TransformationConfiguration(GenerateUuidTransformationSchema, config)
    transformation = GenerateUuidTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


@patch("uuid.uuid4", return_value=_mock_uuid())
def test_transorm_nested(mock):
    config = {"output": "b/c"}
    initial_record = Record({"a": 1})
    expected_output = Record({"a": 1, "b": {"c": "asd-123-asd-123"}})

    t_config = TransformationConfiguration(GenerateUuidTransformationSchema, config)
    transformation = GenerateUuidTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result
