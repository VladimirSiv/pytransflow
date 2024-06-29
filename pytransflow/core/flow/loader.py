"""
Defines classes and methods related to the ``FlowConfigurationLoader``
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional
import yaml
from pydantic import ValidationError
from pytransflow.exceptions import (
    FlowConfigurationFileNotFoundException,
    FlowSchemaNotProperlyDefinedException,
    FlowConfigurationException,
)
from pytransflow.core.configuration import TransflowConfiguration
from pytransflow.core.flow.schema import FlowSchema
from pytransflow.core.flow.configuration import FlowConfiguration


logger = logging.getLogger(__name__)


class FlowConfigurationLoader:
    """Implements Flow Configuration Loader class"""

    @staticmethod
    def load(
        name: Optional[str],
        config: Optional[Dict[str, Any]],
    ) -> FlowConfiguration:
        """Loads Flow Configuration

        Args:
            name: Name of the flow configuration file
            config: Flow configuration as a `dict` object

        """
        if config is not None:
            return FlowConfigurationLoader._load_configuration_dict(config)
        if name is not None:
            return FlowConfigurationLoader._load_configuration_file(name)
        raise FlowConfigurationException("Flow requires 'name' or 'config' arguments")

    @staticmethod
    def _load_configuration_dict(config: Dict[str, Any]) -> FlowConfiguration:
        """Load flow configuration from a dict

        Args:
            config: Dictionary that defines flow configuration

        Returns:
            FlowConfiguration instance

        """
        logger.info("Loading Flow configuration from config object...")
        flow_schema = FlowSchema(**config)
        return FlowConfiguration(
            flow_schema=flow_schema,
        )

    @staticmethod
    def _load_configuration_file(name: str) -> FlowConfiguration:
        """Load flow configuration from a yml file. The filename defines the
        name of the flow.

        Args:
            name: Filename of a .yml

        Returns:
            FlowConfiguration instance

        Raises:
            FlowConfigurationFileDoesNotExistException - File .yml file not found

        """
        logger.info("Loading Flow configuration from config yaml file: %s", name)
        flow_config_path = FlowConfigurationLoader._find_configuration_file(name)

        try:
            with open(flow_config_path, "r", encoding="utf8") as f_in:
                flow_schema = FlowSchema(**yaml.safe_load(f_in))
                return FlowConfiguration(
                    flow_schema=flow_schema,
                )
        except ValidationError as v_err:
            raise FlowSchemaNotProperlyDefinedException(name) from v_err

    @staticmethod
    def _find_configuration_file(name: str) -> Path:
        """Searches for a flow configuration file based on the name and FLOWS_PATH configuration

        Args:
            name: Filename of a .yml

        Raises:
            FlowConfigurationFileNotFoundException - If flow configuration file is not found

        """
        extensions = ["yml", "yaml"]
        flows_path = TransflowConfiguration().flows_path

        for extension in extensions:
            flow_config_path = Path(flows_path) / f"{name}.{extension}"
            if flow_config_path.exists() and flow_config_path.is_file():
                return flow_config_path

        logger.error("Configuration yaml file not found")
        raise FlowConfigurationFileNotFoundException(str(flow_config_path))
