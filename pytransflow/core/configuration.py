# pylint: disable=attribute-defined-outside-init
"""
Defines classes and methods related to ``TransflowConfiguration``
"""

from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional, Dict
import configparser
import tomli
from pytransflow.exceptions import PathNotDefinedProperlyException
from pytransflow.core.constants import Constants

CURRENT_DIR = Path.cwd()
logger = logging.getLogger(__name__)


class TransflowConfigurationLoader:
    """Implements Transflow Configuration Loader

    Checks if `pyproject.toml` or `.pytransflowrc` exist and loads configuration,
    otherwise it applies the default values

    """

    @classmethod
    def load(cls) -> Dict[str, str]:
        """Loads Transflow Configuration"""
        default_values = {
            "path_separator": Constants.PATH_SEPARATOR.value,
            "schemas_path": Constants.SCHEMAS_PATH.value,
            "flows_path": Constants.FLOWS_PATH.value,
            "default_dataset_name": Constants.DEFAULT_DATASET_NAME.value,
        }
        logger.debug("Loading Transflow Configuration...")
        pyproject_toml = cls.load_pyproject_toml()
        if pyproject_toml:
            return {**default_values, **pyproject_toml}

        pytransflow_rc = cls.load_pytransflow_rc()
        if pytransflow_rc:
            return {**default_values, **pytransflow_rc}

        return default_values

    @staticmethod
    def load_pyproject_toml() -> Optional[Dict[str, str]]:
        """Loads configuration from pyproject.toml"""
        toml_path = CURRENT_DIR / "pyproject.toml"
        if not toml_path.exists():
            logger.debug("pyproject.toml not found")
            return None

        try:
            logger.debug("pyproject.toml found, parsing....")
            with open(toml_path, "rb") as f_in:
                toml_dict = tomli.load(f_in)
                if toml_dict.get("tool") and toml_dict["tool"].get("pytransflow"):
                    return toml_dict["tool"]["pytransflow"]  # type: ignore[no-any-return]
        except tomli.TOMLDecodeError:
            logger.warning(
                "pyproject.toml found but cannot be decoded, loading default configuration"
            )
        return None

    @staticmethod
    def load_pytransflow_rc() -> Optional[Dict[str, str]]:
        """Loads configuration from .pytransflowrc"""
        rc_path = CURRENT_DIR / ".pytransflowrc"
        if not rc_path.exists():
            logger.debug(".pytransflowrc not found")
            return None

        try:
            logger.debug(".pytransflowrc found, parsing...")
            config = configparser.ConfigParser()
            config.read(rc_path)
            if "MASTER" in config.sections():
                return dict(config["MASTER"].items())
        except configparser.Error:
            logger.warning(
                ".pytransflowrc found but cannot be parsed, loading default configuration"
            )
        return None


class TransflowConfiguration:
    """Implements Transflow Configuration logic

    This class contains all the library configurations used through the library. It's using the
    Singleton class pattern.

    Attributes:
        path_separator: Path separator configuration
        schemas_path: Path where schemas ar.raises(
            PathNotDefinedProperlyException,
            match=f"FLOWS_PATH: \'{tmpdir / 'flows'}\' - doesn't exist",
        ):e stored
        flows_path: Path where flows are stored

    """

    _instance = None

    def __new__(cls) -> TransflowConfiguration:
        if cls._instance is None:
            cls._instance = super(TransflowConfiguration, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        config = TransflowConfigurationLoader.load()
        self.path_separator = config["path_separator"]
        self.default_dataset_name = config["default_dataset_name"]

        self.schemas_path = Path(config["schemas_path"])
        if not self.schemas_path.is_absolute():
            self.schemas_path = CURRENT_DIR / self.schemas_path

        self.flows_path = Path(config["flows_path"])
        if not self.flows_path.is_absolute():
            self.flows_path = CURRENT_DIR / self.flows_path

        self._validate()

    def _validate(self) -> None:
        """Validates Transflow Configuration

        Raises:
            PathNotDefinedProperlyException: If paths are not defined properly

        """

        if not self.schemas_path.exists():
            raise PathNotDefinedProperlyException("SCHEMAS_PATH", self.schemas_path)

        if not self.flows_path.exists():
            raise PathNotDefinedProperlyException("FLOWS_PATH", self.flows_path)
