"""
Defines project information
"""

import logging
import inspect
import importlib
from typing import Dict, Any
from pkgutil import iter_modules
from pytransflow.core import TransformationCatalogue

__version__ = "0.1.1"
logging.getLogger("pytransflow").addHandler(logging.NullHandler())


def load_built_in_transformations() -> Dict[str, Any]:
    """Dynamically imports and adds built-in transformations to the TransformationCatalogue"""

    module_name = "pytransflow.transformations"
    result = {}
    module = importlib.import_module(module_name)
    for submodule in iter_modules(module.__path__):
        loaded = importlib.import_module(f"{module_name}.{submodule.name}")
        name = submodule.name.replace("_", " ").title().replace(" ", "")
        transformation = None
        schema = None
        for member in inspect.getmembers(loaded):
            if member[0] == f"{name}Transformation":
                transformation = member[1]
            if member[0] == f"{name}TransformationSchema":
                schema = member[1]

        if transformation is None or schema is None:  # pragma: no cover
            raise RuntimeError(f"Built-in transformations not properly loaded for: {name}")

        result[submodule.name] = (transformation, schema)
    return result


TransformationCatalogue.transformations = load_built_in_transformations()
