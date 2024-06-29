import pytest
from pydantic import ValidationError
from pytransflow.transformations.prefix import PrefixTransformation, PrefixTransformationSchema
from pytransflow.core.record import Record
from pytransflow.exceptions import FieldWrongTypeException
from pytransflow.core.transformation import TransformationConfiguration


def test_configuration_missing_field():
    config = {"value": "d", "output": "a"}
    with pytest.raises(ValidationError):
        TransformationConfiguration(PrefixTransformationSchema, config)


def test_configuration_missing_value():
    config = {"field": "a", "output": "d"}
    with pytest.raises(ValidationError):
        TransformationConfiguration(PrefixTransformationSchema, config)


def test_configuration_happy():
    config = {"field": "a", "value": "b", "output": "d"}
    t_config = TransformationConfiguration(PrefixTransformationSchema, config)
    PrefixTransformation(t_config)


def test_transform_happy_str():
    config = {"field": "a", "value": "test", "output": "d"}
    initial_record = Record({"a": "a"})
    expected_output = Record({"d": "testa"})

    t_config = TransformationConfiguration(PrefixTransformationSchema, config)
    transformation = PrefixTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_keep_original():
    config = {
        "field": "a",
        "value": "_",
        "output": "d",
        "output_type": "int",
        "keep_original": True,
    }
    initial_record = Record({"a": "a"})
    expected_output = Record({"a": "a", "d": "_a"})

    t_config = TransformationConfiguration(PrefixTransformationSchema, config)
    transformation = PrefixTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_output_not_specified():
    config = {
        "field": "a",
        "value": "_",
    }
    initial_record = Record({"a": "a"})
    expected_output = Record({"a": "_a"})

    t_config = TransformationConfiguration(PrefixTransformationSchema, config)
    transformation = PrefixTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_field_wrong_type():
    config = {
        "field": "a",
        "value": "d",
        "output": "c",
    }
    initial_record = Record({"a": {"a": 1}})

    t_config = TransformationConfiguration(PrefixTransformationSchema, config)
    transformation = PrefixTransformation(t_config)
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

    t_config = TransformationConfiguration(PrefixTransformationSchema, config)
    transformation = PrefixTransformation(t_config)
    result = transformation.execute(initial_record)

    assert isinstance(result, Record)
    assert result == initial_record
