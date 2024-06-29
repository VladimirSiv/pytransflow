"""
Defines classes and methods related to the ``FlowSchema``
"""

import logging
from typing import Dict, Any, Optional, List
from typing_extensions import Self
from pydantic import BaseModel, Field, model_validator
from pytransflow.core.flow.fail_scenario import FlowFailScenarioChoices

logger = logging.getLogger(__name__)


class FlowSchema(BaseModel):
    """Defines Flow Schema configuration"""

    description: Optional[str] = Field(
        default=None,
        title="Flow description",
        description="Description of the Flow",
    )
    path_separator: Optional[str] = Field(
        default=None,
        title="Path separator",
        description=(
            "Sets field path separator for all transformations in the flow. Setting this parameter "
            "will overwrite the configuration set in 'pyproject.toml' or '.pytransflowrc' only for "
            "this flow"
        ),
    )
    parallel: bool = Field(
        default=False,
        title="Enable Flow multi-processing",
        description=(
            "If enabled, the flow execution will be done using multiprocessing, each process "
            "will run its own pipeline, and the data will be joined at the end to produce as "
            "single dataset result. Defaults to False, i.e. single process"
        ),
    )
    cores: Optional[int] = Field(
        default=None,
        title="Number of cores",
        description=(
            "Number of cores which will be used to execute a Flow in multiprocessing mode"
        ),
    )
    batch: Optional[int] = Field(
        default=None,
        title="Batch size",
        description=(
            "Batch size i.e. number of records that will be processed in a single process when "
            "the multiprocessing mode is enabled"
        ),
    )
    variables: Optional[Dict[str, Any]] = Field(
        default=None,
        title="Flow level variables",
        description=(
            "Defines flow level variables that can be used in any "
            "transformation during the processing"
        ),
    )
    fail_scenarios: Optional[Dict[FlowFailScenarioChoices, Any]] = Field(
        default=None,
        title="Flow fail scenarios",
        description=(
            "Defines in which scenarios we want to fail the flow after processing. For example, "
            "when the percentage of failed records is above some threshold. Check "
            "``FlowFailScenarioChoices`` for more choices."
        ),
    )
    instant_fail: bool = Field(
        default=False,
        title="Instant Fail",
        description="Stops the whole flow if a single transformation fails",
    )
    transformations: List[Dict[str, Any]] = Field(
        title="Transformations",
        description="List of transformations that will be applied on each record",
    )

    @model_validator(mode="after")
    def validation(self) -> Self:
        """Flow schema validation"""
        if not self.parallel and self.cores is not None:
            raise ValueError("Cores parameter cannot be set if 'parallel' is not set to 'True'")
        if not self.parallel and self.batch is not None:
            raise ValueError("Batch parameter cannot be set if 'parallel' is not set to 'True'")
        if self.parallel:
            if self.cores is not None and self.cores <= 0:
                raise ValueError("Cores parameter has to be greater than 0")
            if self.batch is not None and self.batch <= 0:
                raise ValueError("Batch parameter has to be greater than 0")
        return self
