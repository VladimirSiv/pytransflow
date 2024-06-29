"""
Defines classes and methods related to ``Flatten`` transformation
"""

import logging
from typing import Dict, Any, Generator, Tuple
from typing_extensions import Self
from pydantic import Field, model_validator
from pytransflow.exceptions import FieldWrongTypeException
from pytransflow.core.record import Record
from pytransflow.core.transformation import (
    Transformation,
    TransformationSchema,
)

logger = logging.getLogger(__name__)


class FlattenTransformationSchema(TransformationSchema):
    """Implements Flatten Transformation Schema"""

    field: str = Field(
        title="Field",
        description="Input field to be flattened",
        json_schema_extra={"required_in_record": True},
    )
    separator: str = Field(
        default=".",
        title="Separator",
        description="Separator for flattened fields",
    )
    parent_key: str = Field(
        default="",
        title="Parent Key",
        description="Parent Key for flattened fields",
    )
    output: str = Field(
        title="Output",
        description="Output field where the flattened field will be stored",
        json_schema_extra={"output_field": True},
    )

    @model_validator(mode="after")
    def configure(self) -> Self:
        """Configures Flatten Transformation Schema"""
        self.set_dynamic_fields()
        return self


class FlattenTransformation(Transformation):
    """Implements Flatten transformation logic

    Flatten Transformation flattens a specified field.

    """

    def transform(
        self,
        record: Record,
    ) -> Record:
        logger.debug("Applying transformation: Rename")
        field = self.config.schema.field
        output_field = self.config.schema.output
        sep = self.config.schema.separator
        parent_key = self.config.schema.parent_key

        value_to_flatten = record[field]
        if not isinstance(value_to_flatten, dict):
            raise FieldWrongTypeException(field, type(value_to_flatten), "dict")

        record.remove(output_field, False)
        flattend = self._flatten_dict(record[field], parent_key, sep)
        record.add(output_field, flattend)

        return record

    def _flatten_dict(
        self,
        data: Dict[str, Any],
        parent_key: str = "",
        sep: str = ".",
    ) -> Dict[str, Any]:
        return dict(self._flatten_dict_gen(data, parent_key, sep))

    def _flatten_dict_gen(
        self,
        data: Dict[str, Any],
        parent_key: str = "",
        sep: str = ".",
    ) -> Generator[Tuple[Any, Any], Any, Any]:
        for key, value in data.items():
            new_key = parent_key + sep + key if parent_key else key
            if isinstance(value, dict):
                yield from self._flatten_dict(value, new_key, sep=sep).items()
            else:
                yield new_key, value
