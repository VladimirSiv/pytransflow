from typing_extensions import Self
import pytest
from pydantic import Field, model_validator
from pytransflow.core.record.record import Record
from pytransflow.core.transformation import (
    Transformation,
    TransformationSchema,
    TransformationConfiguration,
    TransformationCatalogue,
)
from pytransflow.transformations.rename import RenameTransformationSchema


class CustomTransformation(Transformation):
    def transform(self, record: Record) -> Record:
        return record


class CustomTransformationSchema(TransformationSchema):
    name: str


class CustomTransformationSchemaError(TransformationSchema):
    name: str = Field(
        title="Field",
        description="Input field where the data to be processed is stored",
        json_schema_extra="",
    )

    @model_validator(mode="after")
    def configure(self) -> Self:
        """Configures Add Field Transformation Schema"""
        self.set_dynamic_fields()
        return self


def test_transformation_init_without_output_datasets():
    Transformation.__abstractmethods__ = set()
    config = {
        "field": "a",
        "output": "b",
        "input_datasets": ["new_1", "new_2"],
    }
    expected_config = {
        **config,
        "ignore_errors": [],
        "output_datasets": ["new_1", "new_2"],
        "condition": None,
        "output_fields": ["b"],
        "required_in_record": ['a'],
    }
    t_config = TransformationConfiguration(RenameTransformationSchema, config)
    transformation = Transformation(t_config)
    assert transformation.config == expected_config
    assert isinstance(transformation.config.schema, RenameTransformationSchema)


def test_transformation_init_with_output_datasets():
    Transformation.__abstractmethods__ = set()
    config = {
        "field": "a",
        "output": "b",
        "input_datasets": ["new_1", "new_2"],
        "output_datasets": ["new_3"],
    }
    expected_config = {
        **config,
        "ignore_errors": [],
        "output_datasets": ["new_3"],
        "condition": None,
        "output_fields": ["b"],
        "required_in_record": ['a'],
    }
    t_config = TransformationConfiguration(RenameTransformationSchema, config)
    transformation = Transformation(t_config)
    assert transformation.config == expected_config
    assert isinstance(transformation.config.schema, RenameTransformationSchema)


def test_transformation_repr():
    config = {
        "field": "a",
        "output": "b",
        "input_datasets": ["new_1", "new_2"],
        "output_datasets": ["new_3"],
    }
    t_config = TransformationConfiguration(RenameTransformationSchema, config)
    transformation = Transformation(t_config)
    output = (
        "RenameTransformationSchema(TransformationConfiguration("
        "input_datasets=['new_1', 'new_2'], "
        "output_datasets=['new_3'], "
        "ignore_errors=[], "
        "condition=None, "
        "required_in_record=['a'], "
        "output_fields=['b'], "
        "field=a, "
        "output=b))"
    )
    assert output == str(transformation)


def test_catalogue_add():
    TransformationCatalogue.add_transformation(
        "custom_transformation",
        CustomTransformation,
        CustomTransformationSchema,
    )

    transformation, schema = TransformationCatalogue.get_transformation("custom_transformation")
    assert transformation is CustomTransformation
    assert schema is CustomTransformationSchema


def test_catalogue_already_registered():
    TransformationCatalogue.add_transformation(
        "custom",
        CustomTransformation,
        CustomTransformationSchema,
    )
    with pytest.raises(
        RuntimeError,
        match="Custom Transformation already registered"
    ):
        TransformationCatalogue.add_transformation(
        "custom",
        CustomTransformation,
        CustomTransformationSchema,
    )


def test_catalogue_transformation_not_class():
    with pytest.raises(
        RuntimeError,
        match="Custom Transformation has to be a subclass of Transformation class",
    ):
        TransformationCatalogue.add_transformation(
        "custom_transformation",
        "test",
        CustomTransformationSchema,
    )


def test_catalogue_transformation_wrong_subclass():
    class WrongTransformation:
        def transform():
            return None

    with pytest.raises(
        RuntimeError,
        match="Custom Transformation has to be a subclass of Transformation class",
    ):
        TransformationCatalogue.add_transformation(
        "custom_transformation",
        WrongTransformation,
        CustomTransformationSchema,
    )


def test_catalogue_transformation_schema_not_class():
    with pytest.raises(
        RuntimeError,
        match="Custom Transformation Schema has to be a subclass of TransformationSchema class",
    ):
        TransformationCatalogue.add_transformation(
        "custom_transformation",
        CustomTransformation,
        "test",
    )


def test_catalogue_transformation_schema_wrong_subclass():
    class WrongTransformationSchema:
        pass

    with pytest.raises(
        RuntimeError,
        match="Custom Transformation Schema has to be a subclass of TransformationSchema class",
    ):
        TransformationCatalogue.add_transformation(
        "custom_transformation",
        CustomTransformation,
        WrongTransformationSchema,
    )


def test_transformation_config_eq_different_type():
    config = {"field": "a", "output": "b"}
    t_config = TransformationConfiguration(RenameTransformationSchema, config)

    assert not t_config == ""


def test_transformation_schema_extra_not_dict():
    with pytest.raises(
        RuntimeError,
        match="Transformation schema extra is not of type <dict>"
    ):
        CustomTransformationSchemaError(name="test")
