import pytest
from pydantic import ValidationError
from pytransflow.transformations.add_field import (
    AddFieldTransformation,
    AddFieldTransformationSchema,
)
from pytransflow.exceptions import OutputAlreadyExistsException
from pytransflow.core.record import Record
from pytransflow.core.transformation import TransformationConfiguration


def test_configuration_missing_name():
    with pytest.raises(ValidationError):
        TransformationConfiguration(AddFieldTransformationSchema, {"value": "a"},)


def test_configuration_happy():
    config = {"name": "a", "value": "b"}
    TransformationConfiguration(AddFieldTransformationSchema, config)


def test_configuration_default_value():
    config = {"name": "b"}

    with pytest.raises(ValidationError):
        TransformationConfiguration(AddFieldTransformationSchema, config)


def test_transform_happy():
    config = {"name": "b", "value": "d"}
    initial_record = Record({"a": 1})
    expected_output = Record({"a": 1, "b": "d"})

    t_config = TransformationConfiguration(AddFieldTransformationSchema, config)
    transformation = AddFieldTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_nested_value():
    config = {"name": "b/d/e", "value": "g"}
    initial_record = Record({"a": 1})
    expected_output = Record({"a": 1, "b": {"d": {"e": "g"}}})

    t_config = TransformationConfiguration(AddFieldTransformationSchema, config)
    transformation = AddFieldTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_output_already_exists_ignored():
    config = {
        "name": "a",
        "value": "d",
        "ignore_errors": [OutputAlreadyExistsException.name],
    }
    initial_record = Record({"a": 1})
    expected_output = Record({"a": "d"})

    t_config = TransformationConfiguration(AddFieldTransformationSchema, config)
    transformation = AddFieldTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result
