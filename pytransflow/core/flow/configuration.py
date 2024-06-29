"""
Defines classes and methods related to the ``FlowConfiguration``
"""

import logging
from typing import List, Dict, Any
from pytransflow.core.transformation import (
    Transformation,
    TransformationConfiguration,
    TransformationCatalogue,
)
from pytransflow.core.flow.schema import FlowSchema
from pytransflow.core.flow.variables import FlowVariables
from pytransflow.core.flow.fail_scenario import FlowFailScenario

logger = logging.getLogger(__name__)


class FlowConfiguration:  # pylint: disable=too-many-instance-attributes
    """Implements Flow Configuration

    Args:
        flow_schema: Flow configuration schema

    Attributes:
        batch: Number of records in a batch
        cores: Number of cores used in multiprocessing mode
        fail_scenarios: Flow fail scenarios
        instant_fail: If True flow should fail if one record fails
        path_separator: Flow level path separator
        parallel: If multiprocessing mode is enabled
        variables: Flow level variables
        transformations: List of Transformation objects

    """

    def __init__(
        self,
        flow_schema: FlowSchema,
    ) -> None:
        self.batch = flow_schema.batch
        self.cores = flow_schema.cores
        self.fail_scenarios = FlowFailScenario(flow_schema.fail_scenarios)
        self.instant_fail = flow_schema.instant_fail
        self.path_separator = flow_schema.path_separator
        self.parallel = flow_schema.parallel
        self.variables = FlowVariables(flow_schema.variables)
        self.transformations = self._resolve_transformations(flow_schema.transformations)

    def _resolve_transformations(
        self,
        transformations: List[Dict[str, Any]],
    ) -> List[Transformation]:
        """Resolves transformations from Flow yaml configuration file to the transformation classes
        using `TransformationCatalogue`

        Args:
            transformations: List of transformations from .yaml file

        Returns:
            List of resolved transformations

        """
        resolved_transformations = []
        for t_element in transformations:
            for t_name, t_config in t_element.items():
                logger.debug("Resolving transformation: %s", t_name)
                transformation, schema = TransformationCatalogue.get_transformation(t_name)
                transformation_config = TransformationConfiguration(schema=schema, config=t_config)
                resolved_transformation = transformation(transformation_config)
                resolved_transformation.variables = self.variables
                resolved_transformations.append(resolved_transformation)
        logger.debug(
            "Number of resolved transformations: %d",
            len(resolved_transformations),
        )
        return resolved_transformations
