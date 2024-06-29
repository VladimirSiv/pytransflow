"""
Defines classes and method related to the ``SchemaLoader``
"""

import logging
import importlib.util
from pathlib import Path
from pydantic._internal._model_construction import ModelMetaclass
from pytransflow.exceptions import (
    SchemaFileNotFoundException,
    SchemaFailedToLoadException,
    SchemaNotProperlyDefinedException,
)
from pytransflow.core.configuration import TransflowConfiguration

logger = logging.getLogger(__name__)


class SchemaLoader:
    """Implements SchemaLoader

    SchemaLoader loads a schema defined in `SCHEMAS_DIR` folder. It loads the
    module from a specific file and imports the requried schema class.

    """

    @staticmethod
    def load(name: str) -> ModelMetaclass:
        """Loads Schema

        Args:
            name: Name of the module and schema class in the format:
            '<module-name>.<class-name>'

        Raises:
            SchemaFileNotFoundException - If schema file is not found

        """
        transflow_config = TransflowConfiguration()
        name_split = name.split(".")
        if len(name_split) != 2:
            raise ValueError("Schema should be defined as '<module-name>.<class-name>'")
        filename = name_split[0]
        class_name = name_split[1]
        filename = filename + ".py"
        schema_config_path = Path(transflow_config.schemas_path) / filename
        if not schema_config_path.is_file():
            logger.error("Schema file not found: %s", schema_config_path)
            raise SchemaFileNotFoundException(str(schema_config_path))

        return SchemaLoader._load_schema_class(name, schema_config_path, class_name)

    @staticmethod
    def _load_schema_class(name: str, path: Path, class_name: str) -> ModelMetaclass:
        """Loads schema class based on the filepath and class name

        Raises:
            SchemaNotProperlyDefinedException - If is not properly defined
            SchemaFailedToLoadException - If Exception is raised during the schema loading

        """
        try:
            spec = importlib.util.spec_from_file_location(name, path)
            if spec is None:
                raise RuntimeError("Failed to import specs from a file location")
            module = importlib.util.module_from_spec(spec)
            loader = spec.loader
            if loader is None:
                raise RuntimeError("Imported specs cannot load the module")
            loader.exec_module(module)
            schema_class = getattr(module, class_name)
            if not isinstance(schema_class, ModelMetaclass):
                raise SchemaNotProperlyDefinedException(class_name)
            return schema_class
        except Exception as err:
            logger.error("Failed to load the schema: %s", name)
            raise SchemaFailedToLoadException(name) from err
