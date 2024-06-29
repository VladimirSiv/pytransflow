"""
Defines classes and methods related to ``Rename`` transformation
"""

import logging
from typing_extensions import Self
from pydantic import Field, model_validator
from pytransflow.core.record import Record
from pytransflow.core.transformation import (
    Transformation,
    TransformationSchema,
)

logger = logging.getLogger(__name__)


class RenameTransformationSchema(TransformationSchema):
    """Implements Rename Transformation Schema"""

    field: str = Field(
        title="Field",
        description="Input field where the data to be processed is stored",
        json_schema_extra={"required_in_record": True},
    )
    output: str = Field(
        title="Output",
        description="Output field where the processed data will be stored",
        json_schema_extra={"output_field": True},
    )

    @model_validator(mode="after")
    def configure(self) -> Self:
        """Configures Rename Transformation Schema"""
        self.set_dynamic_fields()
        return self


class RenameTransformation(Transformation):
    """Implements Rename transformation logic

    Rename Transformation transforms a record by renaming a field.

    """

    def transform(
        self,
        record: Record,
    ) -> Record:
        logger.debug("Applying transformation: Rename")
        field = self.config.schema.field
        output_field = self.config.schema.output
        record.remove(output_field, False)
        value = record[field]
        record.remove(field)
        record[output_field] = value

        return record
