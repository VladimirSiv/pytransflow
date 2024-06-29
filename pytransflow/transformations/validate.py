"""
Defines classes and methods related to ``Validate`` Transformation
"""

import logging
from typing_extensions import Self
from pydantic import Field, model_validator
from pytransflow.core.schema import SchemaLoader
from pytransflow.core.record import Record
from pytransflow.core.transformation import (
    Transformation,
    TransformationSchema,
)

logger = logging.getLogger(__name__)


class ValidateTransformationSchema(TransformationSchema):
    """Implements Validate Transformation Schema"""

    schema_name: str = Field(
        title="Schema",
        description="Name of the schema that will be used for validation. "
        "The input format is '<name_of_the_file>.<name_of_the_schema_class>'",
    )

    @model_validator(mode="after")
    def configure(self) -> Self:
        """Configures Validate Transformation Schema"""
        self.set_dynamic_fields()
        return self


class ValidateTransformation(Transformation):
    """Implements Validate transformation logic

    Add Field Transformation adds a new field to the record.

    """

    def transform(
        self,
        record: Record,
    ) -> Record:
        logger.debug("Applying transformation: Validate")
        schema_class = SchemaLoader.load(self.config.schema.schema_name)
        return Record(schema_class(**record.data).model_dump())
