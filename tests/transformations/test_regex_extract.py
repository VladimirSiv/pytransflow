import pytest
from pydantic import ValidationError
from pytransflow.transformations.regex_extract import (
    RegexExtractTransformation,
    RegexExtractTransformationSchema,
)
from pytransflow.core.record import Record
from pytransflow.exceptions import FieldWrongTypeException
from pytransflow.core.transformation import TransformationConfiguration


def test_configuration_missing_field():
    config = {"regex": "d", "output": "a"}
    with pytest.raises(ValidationError):
        TransformationConfiguration(RegexExtractTransformationSchema, config)


def test_configuration_missing_regex():
    config = {"field": "a", "output": "d"}
    with pytest.raises(ValidationError):
        TransformationConfiguration(RegexExtractTransformationSchema, config)


def test_configuration_missing_output():
    config = {"field": "a", "regex": "d"}
    with pytest.raises(ValidationError):
        TransformationConfiguration(RegexExtractTransformationSchema, config)


def test_configuration_happy():
    config = {"field": "a", "regex": "test", "output": "d"}
    t_config = TransformationConfiguration(RegexExtractTransformationSchema, config)
    RegexExtractTransformation(t_config)


def test_transform_happy():
    config = {"field": "a", "regex": r"\d{2}", "output": "b"}
    initial_record = Record({"a": "asd12asd"})
    expected_output = Record({"a": "asd12asd", "b": "12"})

    t_config = TransformationConfiguration(RegexExtractTransformationSchema, config)
    transformation = RegexExtractTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_extracted():
    config = {"field": "a", "regex": r"\d{2}", "output": "b"}
    initial_record = Record({"a": "asd12asd"})
    expected_output = Record({"a": "asd12asd", "b": "12"})

    t_config = TransformationConfiguration(RegexExtractTransformationSchema, config)
    transformation = RegexExtractTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_not_found():
    config = {"field": "a", "regex": r"\d{2}", "output": "b"}
    initial_record = Record({"a": "asdasd"})
    expected_output = Record({"a": "asdasd", "b": None})

    t_config = TransformationConfiguration(RegexExtractTransformationSchema, config)
    transformation = RegexExtractTransformation(t_config)
    result = transformation.execute(initial_record)

    assert expected_output == result


def test_transform_wrong_data_type():
    config = {"field": "a", "regex": r"\d{2}", "output": "b"}
    initial_record = Record({"a": 123})

    t_config = TransformationConfiguration(RegexExtractTransformationSchema, config)
    transformation = RegexExtractTransformation(t_config)
    with pytest.raises(FieldWrongTypeException):
        transformation.execute(initial_record)
