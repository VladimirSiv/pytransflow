import pytest
from pydantic import ValidationError
from pytransflow.transformations.remove_fields import (
    RemoveFieldsTransformation,
    RemoveFieldsTransformationSchema,
)
from pytransflow.core.record import Record
from pytransflow.exceptions import FieldDoesNotExistException
from pytransflow.core.transformation import TransformationConfiguration


def test_configuration_missing_name():
    config = {}
    with pytest.raises(ValidationError):
        TransformationConfiguration(RemoveFieldsTransformationSchema, config)


def test_configuration_happy():
    config = {"fields": ["a", "b"]}
    t_config = TransformationConfiguration(RemoveFieldsTransformationSchema, config)
    RemoveFieldsTransformation(t_config)


def test_transform():
    config = {"fields": ["a"]}
    initial_record = Record({"a": 1})
    expected_output = Record({})

    t_config = TransformationConfiguration(RemoveFieldsTransformationSchema, config)
    transformation = RemoveFieldsTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_multiple_fields():
    config = {"fields": ["a", "b"]}
    initial_record = Record({"a": 1, "b": 2, "c": 3})
    expected_output = {"c": 3}

    t_config = TransformationConfiguration(RemoveFieldsTransformationSchema, config)
    transformation = RemoveFieldsTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_field_does_not_exist():
    config = {"fields": ["a"]}
    initial_record = Record({"b": 1})

    t_config = TransformationConfiguration(RemoveFieldsTransformationSchema, config)
    transformation = RemoveFieldsTransformation(t_config)
    with pytest.raises(FieldDoesNotExistException):
        transformation.execute(initial_record)


def test_transform_field_does_not_exist_ignored():
    config = {
        "fields": ["a", "b", "c"],
        "ignore_errors": [FieldDoesNotExistException.name],
    }
    initial_record = Record({"a": 1, "d": 2})
    expected_output = Record({"d": 2})

    t_config = TransformationConfiguration(RemoveFieldsTransformationSchema, config)
    transformation = RemoveFieldsTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_remove_nested_fields():
    config = {"fields": ["a/b/c"]}
    initial_record = Record({"a": {"b": {"c": 1, "d": 2}}, "e": 2})
    expected_output = Record({"a": {"b": {"d": 2}}, "e": 2})

    t_config = TransformationConfiguration(RemoveFieldsTransformationSchema, config)
    transformation = RemoveFieldsTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result
