"""
Defines classes and methods related to ``GenerateUUID`` transformation
"""

import logging
import uuid
from typing_extensions import Self
from pydantic import Field, model_validator
from pytransflow.core.record import Record
from pytransflow.core.transformation import (
    Transformation,
    TransformationSchema,
)

logger = logging.getLogger(__name__)


class GenerateUuidTransformationSchema(TransformationSchema):
    """Implements Generate UUID Transformation Schema"""

    output: str = Field(
        title="Field",
        description="Output field",
        json_schema_extra={"output_field": True},
    )

    @model_validator(mode="after")
    def configure(self) -> Self:
        """Configures Generate Uuid Transformation Schema"""
        self.set_dynamic_fields()
        return self


class GenerateUuidTransformation(Transformation):
    """Implements Generate UUID transformation logic

    Generate UUID Transformation generates an UUID and adds it to the record.

    """

    def transform(
        self,
        record: Record,
    ) -> Record:
        logger.debug("Applying transformation: Generate UUID")
        output = self.config.schema.output
        value = uuid.uuid4()

        record.add(output, value)

        return record
