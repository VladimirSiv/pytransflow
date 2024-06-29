"""
Defines classes and methods related to ``Prefix`` transformation
"""

import logging
from typing import Optional
from typing_extensions import Self
from pydantic import Field, model_validator
from pytransflow.exceptions import FieldWrongTypeException
from pytransflow.core.record import Record
from pytransflow.core.transformation import (
    Transformation,
    TransformationSchema,
)

logger = logging.getLogger(__name__)


class PrefixTransformationSchema(TransformationSchema):
    """Implements Prefix Transformation Schema"""

    field: str = Field(
        title="Input Field",
        description="Input field where the data to be processed is stored",
        json_schema_extra={"required_in_record": True},
    )
    value: str = Field(
        title="Value",
        description="Defines prefix value",
    )
    keep_original: bool = Field(
        default=False,
        title="Keep Original",
        description="If True the original field will be kept, otherwise it will be deleted",
    )
    output: Optional[str] = Field(
        default=None,
        title="Output field",
        description="Output field where the processed data will be stored",
        json_schema_extra={"output_field": True},
    )

    @model_validator(mode="after")
    def configure(self) -> Self:
        """Configures Prefix Transformation Schema"""
        if self.output is None:
            self.output = self.field
            self.ignore_errors.append("output_already_exists")  # pylint: disable=no-member
        self.set_dynamic_fields()
        return self


class PrefixTransformation(Transformation):
    """Implements Prefix transformation logic

    Prefix Transformation transforms a record by appending a prefix value to
    the field already present in the record.

    """

    def transform(
        self,
        record: Record,
    ) -> Record:
        logger.debug("Applying transformation: Prefix")
        output_field = self.config.schema.output
        field = self.config.schema.field
        prefix_value = self.config.schema.value
        keep_original = self.config.schema.keep_original

        value = record[field]

        if not isinstance(value, str):
            logger.warning("Field wrong type: %s", type(value))
            raise FieldWrongTypeException(field, type(value), "str")

        record[output_field] = prefix_value + value

        if not keep_original and field != output_field:
            record.remove(field)

        return record
