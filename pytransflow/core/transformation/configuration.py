"""
Defines classes and methods related to ``TransformationConfiguration``
"""

import logging
from typing import Dict, Any, List, TypeVar
from pytransflow.core.transformation.schema import OutputDataset

logger = logging.getLogger(__name__)
TransformationSchema = TypeVar("TransformationSchema")


class TransformationConfiguration:
    """Implements Transformation Configuration class

    This class sets the configuration of the Transformation based on schema and yaml configuration.
    It implements methods that ease further processing and dealing with configurations.

    Args:
        schema: Transformation schema class
        config: Transformation schema configuration

    Attributes:
        schema: Transformation schema

    """

    def __init__(
        self,
        schema: TransformationSchema,
        config: Dict[str, Any],
    ) -> None:
        logger.debug(
            "Transformation Configuration initialized with schema: %s, config: %s",
            schema,
            config,
        )
        self.schema = schema(**config)  # type: ignore

    @property
    def input_datasets(self) -> List[str]:
        """Defines input_datasets attribute"""
        return self.schema.input_datasets  # type: ignore

    @property
    def output_datasets(self) -> List[OutputDataset]:
        """Defines output_datasets attribute"""
        return self.schema.output_datasets  # type: ignore

    def __repr__(self) -> str:
        config_repr = ", ".join([f"{k}={v}" for k, v in self.schema.model_dump().items()])
        return f"TransformationConfiguration({config_repr})"

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, dict):
            return __o == self.schema.model_dump()  # type: ignore
        if isinstance(__o, TransformationConfiguration):
            return __o.schema == self.schema  # type: ignore
        return False

    def get_required_fields(self) -> List[str]:
        """Returns fields that are required in the record"""
        return self.schema.required_in_record  # type: ignore

    def get_output_fields(self) -> List[str]:
        """Returns transformation output fields"""
        return self.schema.output_fields  # type: ignore
