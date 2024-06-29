"""
Defines classes and methods related to ``Remove Fields`` transformation
"""

import logging
from typing import List
from typing_extensions import Self
from pydantic import Field, model_validator
from pytransflow.exceptions import FieldDoesNotExistException
from pytransflow.core.record import Record
from pytransflow.core.transformation import (
    Transformation,
    TransformationSchema,
)

logger = logging.getLogger(__name__)


class RemoveFieldsTransformationSchema(TransformationSchema):
    """Implements Remove Fields Transformation Schema"""

    fields: List[str] = Field(
        title="Field",
        description="Fields to be removed",
    )

    @model_validator(mode="after")
    def configure(self) -> Self:
        """Configures Remove Fields Transformation Schema"""
        self.set_dynamic_fields()
        return self


class RemoveFieldsTransformation(Transformation):
    """Implements Remove Fields transformation logic

    Remove Fields Transformation removes a field to the record.

    """

    def transform(
        self,
        record: Record,
    ) -> Record:
        logger.debug("Applying transformation: Remove Fields")
        return self._transform(record)

    def _transform(self, record: Record) -> Record:
        fields = self.config.schema.fields
        for field in fields:
            if (
                not record.contains(field)
                and FieldDoesNotExistException.name not in self.config.schema.ignore_errors
            ):
                logger.warning("Record does not contain field: %s", field)
                raise FieldDoesNotExistException(field)
            record.remove(field)
        return record
