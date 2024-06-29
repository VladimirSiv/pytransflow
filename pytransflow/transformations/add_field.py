"""
Defines classes and methods related to ``AddField`` Transformation
"""

import logging
from typing import Any
from typing_extensions import Self
from pydantic import Field, model_validator
from pytransflow.core.record import Record
from pytransflow.core.transformation import (
    Transformation,
    TransformationSchema,
)

logger = logging.getLogger(__name__)


class AddFieldTransformationSchema(TransformationSchema):
    """Implements Add Field Transformation Schema"""

    name: str = Field(
        title="Field",
        description="Input field where the data to be processed is stored",
        json_schema_extra={"output_field": True},
    )
    value: Any = Field(
        title="Output",
        description="Output field where the processed data will be stored",
    )

    @model_validator(mode="after")
    def configure(self) -> Self:
        """Configures Add Field Transformation Schema"""
        self.set_dynamic_fields()
        return self


class AddFieldTransformation(Transformation):
    """Implements Set Value transformation logic

    Add Field Transformation adds a new field to the record.

    """

    def transform(
        self,
        record: Record,
    ) -> Record:
        logger.debug("Applying transformation: Add Field")
        name = self.config.schema.name
        value = self.config.schema.value
        record.add(name, value)

        return record
