# pylint: disable=duplicate-code
"""
Defines classes and methods related to ``Postfix`` transformation
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


class PostfixTransformationSchema(TransformationSchema):
    """Implements Postfix Transformation Schema"""

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
        """Configures Postfix Transformation Schema"""
        if self.output is None:
            self.output = self.field
            self.ignore_errors.append("output_already_exists")  # pylint: disable=no-member
        self.set_dynamic_fields()
        return self


class PostfixTransformation(Transformation):
    """Implements Postfix transformation logic

    Postfix Transformation transforms a record by appending a postfix value to
    the field already present in the record.

    """

    def transform(
        self,
        record: Record,
    ) -> Record:
        logger.debug("Applying transformation: Postfix")
        output_field = self.config.schema.output
        field = self.config.schema.field
        postfix_value = self.config.schema.value
        keep_original = self.config.schema.keep_original

        value = record[field]

        if not isinstance(value, str):
            logger.warning("Field wrong type: %s", type(value))
            raise FieldWrongTypeException(field, type(value), "str")

        record[output_field] = value + postfix_value

        if not keep_original and field != output_field:
            record.remove(field)

        return record
