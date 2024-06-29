"""
Defines classes and methods related to ``TransformationSchema``
"""

import logging
from typing import Optional, List, Any
from typing_extensions import Self, Annotated
from pydantic import BaseModel, Field, model_validator, BeforeValidator
from pytransflow.core.configuration import TransflowConfiguration

logger = logging.getLogger(__name__)


class OutputDataset:
    """Implements Output Dataset

    Attributes:
        name: Dataset name
        condition: Output condition

    Note:
        This object is used in transformations to specify `output_datasets`. It can contain
        optional `condition` attribute. This condition will be evaluated and if it's met it will
        be included in the output dataset.

    """

    def __init__(self, name: str, condition: Optional[str] = None) -> None:
        self.name = name
        self.condition = condition

    def __eq__(self, value: object) -> bool:
        return value == self.name

    def __repr__(self) -> str:
        return repr(self.name)


def output_dataset_before(v: Any) -> List[OutputDataset]:
    """Handles output dataset field before validation

    Args:
        v: Schema field value

    Returns:
        List of OutputDataset objects

    """
    final = []
    for dataset in v:
        if isinstance(dataset, str):
            final.append(OutputDataset(name=dataset))
        if isinstance(dataset, dict):
            if "name" in dataset and "condition" in dataset:
                final.append(OutputDataset(**dataset))
            elif len(list(dataset)) == 1:
                name = list(dataset.keys())[0]
                final.append(OutputDataset(name=name, condition=dataset[name]))
            else:
                raise ValueError("Output dataset not configured properly")
    return final


class TransformationSchema(BaseModel):
    """Implements Transformation Schema

    This class is the base schema for all Transformations

    """

    model_config = {"arbitrary_types_allowed": True}

    input_datasets: List[str] = Field(
        default=[],
        title="Input Datasets",
        description="List of input datasets which records will be processed",
    )
    output_datasets: Annotated[List[OutputDataset], BeforeValidator(output_dataset_before)] = Field(
        default=[],
        title="Output Datasets",
        description="List of output datasets where the processed records will be routed",
    )
    ignore_errors: List[str] = Field(
        default=[],
        title="Ignore Failures",
        description=(
            "List of ignore failures that will be ignored if Transformation encouters them "
            "during the processing"
        ),
    )
    condition: Optional[str] = Field(
        default=None,
        title="Condition",
        description=(
            "Defines a condition for applying a Transformation. If condition is met the "
            "Transformation will be applied, otherwise it will be skipped"
        ),
    )
    required_in_record: List[str] = Field(
        default=[],
        title="Required fields in a Record",
        description=(
            "This parameter is used to specify the fields that are required in the record. These "
            "fields are then checked by the Analyzer before the transformation is applied"
        ),
    )
    output_fields: List[str] = Field(
        default=[],
        title="Record output fields",
        description=(
            "This parameter is used to specify the output fields. The presence of these fields "
            "will be checked by the Analyzer and the OutputAlreadyExistsException will be thrown "
            "if not ignored"
        ),
    )

    @model_validator(mode="after")
    def set_default_dataset(self) -> Self:
        """Sets default dataset"""
        if len(self.input_datasets) == 0:
            self.input_datasets = [TransflowConfiguration().default_dataset_name]
        return self

    def set_dynamic_fields(self) -> Self:
        """Sets schema dynamic fields

        Note:
            These fields are dynamically set based on the transformation schema. This method is
            called from a subclass at the end of @model_validation. These fields are later on used
            in Analyzer to perform required checks before the actual processing.

        """
        required_fields = []
        output_fields = []
        data = self.model_dump()
        for name, value in self.model_fields.items():
            extra = value.json_schema_extra
            if extra is not None:
                if not isinstance(extra, dict):
                    raise RuntimeError("Transformation schema extra is not of type <dict>")
                if extra.get("required_in_record"):
                    required_fields.append(data.get(name))
                if extra.get("output_field"):
                    output_fields.append(data.get(name))
        self.required_in_record = required_fields  # type: ignore
        self.output_fields = output_fields  # type: ignore
        if len(self.output_datasets) == 0:
            self.output_datasets = [OutputDataset(name) for name in self.input_datasets]

        return self
