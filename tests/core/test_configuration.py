import pytest
from unittest.mock import patch
from pytransflow.core.configuration import TransflowConfigurationLoader, TransflowConfiguration
from pytransflow.exceptions import PathNotDefinedProperlyException


def test_configuration_pyproject_toml(tmpdir):
    with patch("pytransflow.core.configuration.CURRENT_DIR", tmpdir):
        toml = tmpdir / "pyproject.toml"
        toml.write_text(
            "[tool.pytransflow]\n"
            "path_separator = \".\"\n"
            f"schemas_path = \"{tmpdir / 'schemas'}\"\n"
            f"flows_path = \"{tmpdir / 'flows'}\"\n"
            "default_dataset_name = 'test'",
            encoding="utf-8"
        )

        data = TransflowConfigurationLoader.load()
        assert data == {
            "path_separator": ".",
            "schemas_path": f"{tmpdir / 'schemas'}",
            "flows_path": f"{tmpdir / 'flows'}",
            "default_dataset_name": "test"
        }


def test_configuration_current_dir_path(tmpdir):
    with patch("pytransflow.core.configuration.CURRENT_DIR", tmpdir):
        toml = tmpdir / "pyproject.toml"
        toml.write_text(
            "[tool.pytransflow]\n"
            "path_separator = \".\"\n"
            "schemas_path = \"schemas\"\n"
            "flows_path = \"flows\"\n"
            "default_dataset_name = 'test'",
            encoding="utf-8"
        )
        schemas = tmpdir / 'schemas'
        schemas.write_text("test", 'utf-8')
        flows = tmpdir / 'flows'
        flows.write_text("test", 'utf-8')

        TransflowConfiguration._instance = None
        config = TransflowConfiguration()
        assert config.schemas_path == f"{tmpdir / 'schemas'}"
        assert config.flows_path == f"{tmpdir / 'flows'}"

        TransflowConfiguration._instance = None



def test_configuration_pyproject_toml_error(tmpdir):
    with patch("pytransflow.core.configuration.CURRENT_DIR", tmpdir):
        toml = tmpdir / "pyproject.toml"
        toml.write_text(
            "[tool.pytransflow]\n"
            "path_separator = \".\"\n"
            "asda13%#^^322",
            encoding="utf-8"
        )

        data = TransflowConfigurationLoader.load()
        assert data["path_separator"] == "/"


def test_configuration_pytransflowrc(tmpdir):
    with patch("pytransflow.core.configuration.CURRENT_DIR", tmpdir):
        toml = tmpdir / ".pytransflowrc"
        toml.write_text(
            "[MASTER]\n\n"
            "path_separator=.\n"
            f"schemas_path={tmpdir / 'schemas'}\n"
            f"flows_path={tmpdir / 'flows'}\n"
            "default_dataset_name=test",
            encoding="utf-8"
        )

        data = TransflowConfigurationLoader.load()
        assert data == {
            "path_separator": ".",
            "schemas_path": f"{tmpdir / 'schemas'}",
            "flows_path": f"{tmpdir / 'flows'}",
            "default_dataset_name": "test"
        }


def test_configuration_pytransflowrc_error(tmpdir):
    with patch("pytransflow.core.configuration.CURRENT_DIR", tmpdir):
        toml = tmpdir / ".pytransflowrc"
        toml.write_text(
            "[MASTER]\n\n"
            "path_separator=.\n"
            "a12452^^%#1",
            encoding="utf-8"
        )

        data = TransflowConfigurationLoader.load()
        assert data["path_separator"] == "/"


def test_configuration_schemas_does_not_exist(tmpdir):
    with patch("pytransflow.core.configuration.CURRENT_DIR", tmpdir):
        toml = tmpdir / "pyproject.toml"
        toml.write_text(
            "[tool.pytransflow]\n"
            "path_separator = \".\"\n"
            f"schemas_path = \"{tmpdir / 'schemas'}\"\n"
            f"flows_path = \"{tmpdir / 'flows'}\"\n"
            "default_dataset_name = 'test'",
            encoding="utf-8"
        )

        data = TransflowConfigurationLoader.load()
        assert data == {
            "path_separator": ".",
            "schemas_path": f"{tmpdir / 'schemas'}",
            "flows_path": f"{tmpdir / 'flows'}",
            "default_dataset_name": "test"
        }

        TransflowConfiguration._instance = None

        with pytest.raises(
            PathNotDefinedProperlyException,
            match=f"SCHEMAS_PATH: \'{tmpdir / 'schemas'}\' - doesn't exist",
        ):
            TransflowConfiguration()

        TransflowConfiguration._instance = None

def test_configuration_flows_does_not_exist(tmpdir):
    with patch("pytransflow.core.configuration.CURRENT_DIR", tmpdir):
        toml = tmpdir / "pyproject.toml"
        toml.write_text(
            "[tool.pytransflow]\n"
            "path_separator = \".\"\n"
            f"schemas_path = \"{tmpdir / 'schemas'}\"\n"
            f"flows_path = \"{tmpdir / 'flows'}\"\n"
            "default_dataset_name = 'test'",
            encoding="utf-8"
        )
        schemas = tmpdir / 'schemas'
        schemas.write_text("test", encoding="utf-8")

        data = TransflowConfigurationLoader.load()
        assert data == {
            "path_separator": ".",
            "schemas_path": f"{schemas}",
            "flows_path": f"{tmpdir / 'flows'}",
            "default_dataset_name": "test"
        }
        TransflowConfiguration._instance = None
        with pytest.raises(
            PathNotDefinedProperlyException,
            match=f"FLOWS_PATH: \'{tmpdir / 'flows'}\' - doesn't exist",
        ):
            TransflowConfiguration()

        TransflowConfiguration._instance = None
