"""
Defines classes and methods related to the ``Record``
"""

from __future__ import annotations
import logging
from typing import Dict, Any, Iterable, Optional, Tuple
from typing_extensions import Self
from pytransflow.exceptions import RecordAddException
from pytransflow.core.configuration import TransflowConfiguration


logger = logging.getLogger(__name__)


class Record:
    """Implements Record class

    This class is used to store record data and it implements some of the
    dictionary functionality

    Attributes:
        data: Record data
        path_separator: Record path separator

    """

    def __init__(self, data: Optional[Dict[Any, Any]] = None) -> None:
        self.data = data if data else {}
        self.path_separator = TransflowConfiguration().path_separator

    def __repr__(self) -> str:
        return repr(self.data)

    def __eq__(self, __o: Any) -> bool:
        if isinstance(__o, dict):
            return self.data == __o
        if isinstance(__o, Record):
            return self.data == __o.data
        return False

    def __setitem__(self, key: str, value: Any) -> None:
        self.data[key] = value

    def __getitem__(self, name: str) -> Any:
        return self.data[name]

    def __iter__(self) -> Iterable[Any]:
        return iter(self.data)

    def __contains__(self, key: Any) -> bool:
        return key in self.data

    def items(self) -> Iterable[Tuple[Any, Any]]:
        """Returns items of the data: (key, value)"""
        return self.data.items()

    def fields(self) -> Iterable[str]:
        """Return fields of the data"""
        return self.data.keys()

    def values(self) -> Iterable[Any]:
        """Return values of the data"""
        return self.data.values()

    def add(self, path: str, value: Any) -> None:
        """Adds element in the data

        It supports nested paths. If the path doesn't exist, it will get
        created

        Args:
            path: Path of the element
            value: Element's value

        Raises:
            RecordAddError: When value cannot be added to the path

        """
        logger.debug("Adding to record, path: %s, value: %s", path, value)
        keys = path.split(self.path_separator)
        element = self.data
        try:
            for key in keys[:-1]:
                if key not in element:
                    element[key] = {}
                element = element[key]
            element[keys[-1]] = value
        except TypeError as error:
            logger.error("Failed to add to record, path: %s", path)
            raise RecordAddException(path, value) from error

    def remove(self, path: str, is_required: bool = False) -> Self:
        """Removes element in the data

        It removes deeply nested elements

        Args:
            path: Path of the element
            is_required: If presence and deletion of the element is required

        """
        logger.debug("Removing from the record, path: %s", path)
        if self.contains(path):
            keys = path.split(self.path_separator)
            element = self.data
            for key in keys[:-1]:
                element = element[key]
            element.pop(keys[-1])
        else:
            if is_required:
                logger.error("Record does not contain path: %s", path)
                raise ValueError(f"Element at path '{path}' is not contained in the record")
            logger.warning("Record does not contain path: %s", path)
        return self

    def contains(self, path: str) -> bool:
        """Checks if the path is contained in the data

        Args:
            path: Path of the element

        """
        element = self.data
        for key in path.split(self.path_separator):
            try:
                element = element[key]
            except (KeyError, TypeError):
                return False
        return True
