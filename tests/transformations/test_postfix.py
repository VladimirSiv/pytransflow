import pytest
from pydantic import ValidationError
from pytransflow.transformations.postfix import PostfixTransformation, PostfixTransformationSchema
from pytransflow.core.record import Record
from pytransflow.exceptions import FieldWrongTypeException
from pytransflow.core.transformation import TransformationConfiguration


def test_configuration_missing_field():
    config = {"value": "d", "output": "a"}
    with pytest.raises(ValidationError):
        TransformationConfiguration(PostfixTransformationSchema, config)


def test_configuration_missing_value():
    config = {"field": "a", "output": "d"}
    with pytest.raises(ValidationError):
        TransformationConfiguration(PostfixTransformationSchema, config)


def test_configuration_happy():
    config = {"field": "a", "value": "b", "output": "d"}
    t_config = TransformationConfiguration(PostfixTransformationSchema, config)
    PostfixTransformation(t_config)


def test_transform_happy_str():
    config = {"field": "a", "value": "test", "output": "d"}
    initial_record = Record({"a": "a"})
    expected_output = Record({"d": "atest"})

    t_config = TransformationConfiguration(PostfixTransformationSchema, config)
    transformation = PostfixTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_keep_original():
    config = {
        "field": "a",
        "value": "_",
        "output": "d",
        "keep_original": True,
    }
    initial_record = Record({"a": "a"})
    expected_output = Record({"a": "a", "d": "a_"})

    t_config = TransformationConfiguration(PostfixTransformationSchema, config)
    transformation = PostfixTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_output_not_specified():
    config = {
        "field": "a",
        "value": "_",
    }
    initial_record = Record({"a": "a"})
    expected_output = Record({"a": "a_"})

    t_config = TransformationConfiguration(PostfixTransformationSchema, config)
    transformation = PostfixTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_field_wrong_type():
    config = {
        "field": "a",
        "value": "d",
        "output": "c",
    }
    initial_record = Record({"a": {"a": 1}})

    t_config = TransformationConfiguration(PostfixTransformationSchema, config)
    transformation = PostfixTransformation(t_config)
    with pytest.raises(FieldWrongTypeException):
        transformation.execute(initial_record)


def test_transform_field_wrong_type_ignored():
    config = {
        "field": "a",
        "value": "d",
        "output": "c",
        "ignore_errors": [FieldWrongTypeException.name],
    }
    initial_record = Record({"a": {"a": 1}})

    t_config = TransformationConfiguration(PostfixTransformationSchema, config)
    transformation = PostfixTransformation(t_config)
    result = transformation.execute(initial_record)

    assert isinstance(result, Record)
    assert result == initial_record
