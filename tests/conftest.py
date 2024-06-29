from enum import Enum
from pathlib import Path
import shutil
import yaml
import pytest
from unittest import mock
import pytransflow.core.configuration


@pytest.fixture(scope="session", autouse=True)
def temp_folders(tmp_path_factory):
    schemas_path = tmp_path_factory.mktemp("schemas")
    flows_path = tmp_path_factory.mktemp("flows")

    with open(Path(str(flows_path)) / "flow.yml", "w") as f_out:
        config = {"transformations": []}
        yaml.dump(config, f_out)

    with open(Path(str(flows_path)) / "flow_validation_fail.yml", "w") as f_out:
        config = {"description": "test"}
        yaml.dump(config, f_out)

    class Constants(Enum):
        PATH_SEPARATOR = "/"
        SCHEMAS_PATH = str(schemas_path)
        FLOWS_PATH = str(flows_path)
        DEFAULT_DATASET_NAME = "default"

    with mock.patch.object(pytransflow.core.configuration, "Constants", Constants):
        yield

    shutil.rmtree(schemas_path)
    shutil.rmtree(flows_path)
