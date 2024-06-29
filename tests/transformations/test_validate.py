import pytest
from unittest.mock import patch
from pydantic import BaseModel, StrictStr
from pydantic.error_wrappers import ValidationError
from pytransflow.transformations.validate import (
    ValidateTransformation,
    ValidateTransformationSchema,
)
from pytransflow.core.schema import SchemaLoader
from pytransflow.core.record import Record
from pytransflow.core.transformation import TransformationConfiguration
from pytransflow.core.configuration import TransflowConfiguration
from pytransflow.exceptions import SchemaFileNotFoundException, SchemaFailedToLoadException


def mock_schema(*args):
    class TestSchema(BaseModel):
        a: StrictStr

    return TestSchema


def test_schema_load(tmp_path):
    schemas_path = tmp_path / "schemas"
    schemas_path.mkdir()
    TransflowConfiguration().schemas_path = schemas_path

    schema_file = schemas_path / "test.py"
    schema_file.write_text(
        "from pydantic import BaseModel\n"
        "class TestSchema(BaseModel):\n"
        "\ta: str"
    )

    config = {"schema_name": "test.TestSchema"}
    initial_record = Record({"a": "a"})

    t_config = TransformationConfiguration(ValidateTransformationSchema, config)
    transformation = ValidateTransformation(t_config)
    result = transformation.execute(initial_record)
    assert result == initial_record


def test_schema_wrong_format():
    config = {"schema_name": "test/TestSchema"}
    t_config = TransformationConfiguration(ValidateTransformationSchema, config)
    transformation = ValidateTransformation(t_config)
    with pytest.raises(
        ValueError,
        match="Schema should be defined as '<module-name>.<class-name>'",
    ):
        transformation.execute(Record({}))


def test_schema_file_not_found():
    config = {"schema_name": "something.TestSchema"}
    t_config = TransformationConfiguration(ValidateTransformationSchema, config)
    transformation = ValidateTransformation(t_config)
    with pytest.raises(
        SchemaFileNotFoundException,
        match=r"Schema defined in file '[\w\-\/]+/something.py' could not be found",
    ):
        transformation.execute(Record({}))


@patch("pytransflow.core.schema.importlib.util.spec_from_file_location")
def test_schema_failed_specs(mock, tmp_path):
    mock.return_value = None
    schemas_path = tmp_path / "schemas"
    schemas_path.mkdir()
    TransflowConfiguration().schemas_path = schemas_path

    schema_file = schemas_path / "test.py"
    schema_file.write_text(
        "from pydantic import BaseModel\n"
        "class TestSchema(BaseModel):\n"
        "\ta: str"
    )

    config = {"schema_name": "test.TestSchema"}
    initial_record = Record({"a": "a"})

    t_config = TransformationConfiguration(ValidateTransformationSchema, config)
    transformation = ValidateTransformation(t_config)
    with pytest.raises(
        SchemaFailedToLoadException,
        match="Failed to load schema: test.TestSchema"
    ):
        transformation.execute(initial_record)


@patch("pytransflow.core.schema.importlib.util.spec_from_file_location")
@patch("pytransflow.core.schema.importlib.util.module_from_spec")
def test_schema_failed_loader(mock_module, mock_spec, tmp_path):
    class Loader:
        loader = None
    mock_module.return_value = ""
    mock_spec.return_value = Loader()
    schemas_path = tmp_path / "schemas"
    schemas_path.mkdir()
    TransflowConfiguration().schemas_path = schemas_path

    schema_file = schemas_path / "test.py"
    schema_file.write_text(
        "from pydantic import BaseModel\n"
        "class TestSchema(BaseModel):\n"
        "\ta: str"
    )

    config = {"schema_name": "test.TestSchema"}
    initial_record = Record({"a": "a"})

    t_config = TransformationConfiguration(ValidateTransformationSchema, config)
    transformation = ValidateTransformation(t_config)
    with pytest.raises(
        SchemaFailedToLoadException,
        match="Failed to load schema: test.TestSchema"
    ):
        transformation.execute(initial_record)


def test_schema_not_model_metaclass(tmp_path):
    schemas_path = tmp_path / "schemas"
    schemas_path.mkdir()
    TransflowConfiguration().schemas_path = schemas_path

    schema_file = schemas_path / "test.py"
    schema_file.write_text(
        "class TestSchema:\n"
        "\ta: str"
    )

    config = {"schema_name": "test.TestSchema"}
    initial_record = Record({"a": "a"})

    t_config = TransformationConfiguration(ValidateTransformationSchema, config)
    transformation = ValidateTransformation(t_config)
    with pytest.raises(
        SchemaFailedToLoadException,
        match="Failed to load schema: test.TestSchema"
    ):
        transformation.execute(initial_record)


def test_happy_schema():
    SchemaLoader.load = mock_schema

    config = {"schema_name": "test.TestSchema"}
    initial_record = Record({"a": "a"})

    t_config = TransformationConfiguration(ValidateTransformationSchema, config)
    transformation = ValidateTransformation(t_config)
    result = transformation.execute(initial_record)
    assert result == initial_record


def test_wrong_types():
    SchemaLoader.load = mock_schema

    config = {"schema_name": "test.TestSchema"}
    initial_record = Record({"a": 1})

    t_config = TransformationConfiguration(ValidateTransformationSchema, config)
    transformation = ValidateTransformation(t_config)
    with pytest.raises(ValidationError):
        transformation.execute(initial_record)


def test_remove_fields():
    SchemaLoader.load = mock_schema

    config = {"schema_name": "test.TestSchema"}
    initial_record = Record({"a": "a", "b": "b"})

    t_config = TransformationConfiguration(ValidateTransformationSchema, config)
    transformation = ValidateTransformation(t_config)
    result = transformation.execute(initial_record)
    assert result == Record({"a": "a"})
