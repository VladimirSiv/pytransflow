"""
Defines classes and methods related to ``RegexExtract`` transformation
"""

import re
import logging
from typing import Any, Optional
from typing_extensions import Self
from pydantic import Field, model_validator
from pytransflow.exceptions import FieldWrongTypeException
from pytransflow.core.record import Record
from pytransflow.core.transformation import (
    Transformation,
    TransformationSchema,
)

logger = logging.getLogger(__name__)


class RegexExtractTransformationSchema(TransformationSchema):
    """Implements Regex Extract Transformation Schema"""

    field: str = Field(
        title="Field",
        description="Input field to extract",
        json_schema_extra={"required_in_record": True},
    )
    output: str = Field(
        title="Output",
        description="Output field where the extracted value will be stored",
        json_schema_extra={"output_field": True},
    )
    regex: str = Field(
        title="Regex",
        description="Regex format",
    )

    @model_validator(mode="after")
    def configure(self) -> Self:
        """Configures Regex Extract Transformation Schema"""
        self.set_dynamic_fields()
        return self


class RegexExtractTransformation(Transformation):
    """Implements Regex Extract transformation logic

    Regex Extract Transformation transforms a record by renaming a field.

    """

    def transform(
        self,
        record: Record,
    ) -> Record:
        logger.debug("Applying transformation: Regex Extract")
        field = self.config.schema.field
        output_field = self.config.schema.output
        regex = self.config.schema.regex

        value = record[field]
        if not isinstance(value, str):
            raise FieldWrongTypeException(field, type(value), "str")

        extracted = self._extract(value, regex)
        record[output_field] = extracted

        return record

    @staticmethod
    def _extract(value: str, regex: str) -> Optional[Any]:
        result = re.search(regex, value)
        if result:
            return result.group(0)
        return None
