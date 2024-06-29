import pytest
from pytransflow.transformations.flatten import FlattenTransformation, FlattenTransformationSchema
from pytransflow.exceptions import FieldWrongTypeException
from pytransflow.core.record import Record
from pytransflow.core.transformation import TransformationConfiguration


def test_configuration_happy():
    config = {"field": "a", "output": "b"}
    t_config = TransformationConfiguration(FlattenTransformationSchema, config)
    FlattenTransformation(t_config)


def test_transform_default():
    config = {"field": "a", "output": "b"}
    initial_record = Record({"a": {"b": {"c": 1}}})
    expected_output = Record({"a": {"b": {"c": 1}}, "b": {"b.c": 1}})

    t_config = TransformationConfiguration(FlattenTransformationSchema, config)
    transformation = FlattenTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_separator():
    config = {"field": "a", "output": "b", "separator": "/"}
    initial_record = Record({"a": {"b": {"c": 1}}})
    expected_output = Record({"a": {"b": {"c": 1}}, "b": {"b/c": 1}})

    t_config = TransformationConfiguration(FlattenTransformationSchema, config)
    transformation = FlattenTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_parent_key():
    config = {"field": "a", "output": "b", "parent_key": "X"}
    initial_record = Record({"a": {"b": {"c": 1}}})
    expected_output = Record({"a": {"b": {"c": 1}}, "b": {"X.b.c": 1}})

    t_config = TransformationConfiguration(FlattenTransformationSchema, config)
    transformation = FlattenTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_field_wrong_data_type():
    config = {"field": "a", "output": "b", "parent_key": "X"}
    initial_record = Record({"a": 1})

    t_config = TransformationConfiguration(FlattenTransformationSchema, config)
    transformation = FlattenTransformation(t_config)
    with pytest.raises(FieldWrongTypeException):
        transformation.execute(initial_record)


def test_transform_field_wrong_data_type_ignored():
    config = {
        "field": "a",
        "output": "b",
        "parent_key": "X",
        "ignore_errors": [FieldWrongTypeException.name],
    }
    initial_record = Record({"a": 1})

    t_config = TransformationConfiguration(FlattenTransformationSchema, config)
    transformation = FlattenTransformation(t_config)
    result = transformation.execute(initial_record)

    assert isinstance(result, Record)
    assert result == initial_record
