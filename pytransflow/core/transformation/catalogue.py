"""
Defines classes and methods related to the ``TransformationCatalogue``
"""

from typing import Dict, Tuple, Type
import inspect
from pytransflow.core.transformation.transformation import Transformation
from pytransflow.core.transformation.schema import TransformationSchema
from pytransflow.exceptions import TransformationDoesNotExistException


class TransformationCatalogue:
    """Implements Transformation Catalogue logic

    Transformation Catalogue contains mappings between transformation names
    that are used in the Flow configuration and actual classes that define them.

    Attributes:
        transformations: Dictionary of transformation names and their classes

    """

    transformations: Dict[str, Tuple[Type[Transformation], Type[TransformationSchema]]] = {}

    @classmethod
    def get_transformation(
        cls, name: str
    ) -> Tuple[Type[Transformation], Type[TransformationSchema]]:
        """Gets transformation from registered mapping based on the name of
        the transformation

        Args:
            name: Transformation name

        Returns:
            ``Transformation`` and ``TransformationSchema``

        Raises:
            TransformationDoesNotExist: If mapping not found

        """
        if name in cls.transformations:
            return cls.transformations[name]
        raise TransformationDoesNotExistException(name)

    @classmethod
    def add_transformation(
        cls,
        transformation_name: str,
        transformation: Type[Transformation],
        schema: Type[TransformationSchema],
        overwrite: bool = False,
    ) -> None:
        """Adds a custom transformation to the catalogue of transformations

        Args:
            transformation_name: Name of the transformation that will be used in `.yml`
                configuration files
            transformation: Transformation class that implements transformation logic
            schema: Transformation schema that defines the configuration of the transformation
            overwrite: Overwrite already defined transformation

        """
        if not inspect.isclass(transformation) or not issubclass(transformation, Transformation):
            raise RuntimeError("Custom Transformation has to be a subclass of Transformation class")
        if not inspect.isclass(schema) or not issubclass(schema, TransformationSchema):
            raise RuntimeError(
                "Custom Transformation Schema has to be a subclass of TransformationSchema class"
            )
        if not overwrite and transformation_name in cls.transformations:
            raise RuntimeError("Custom Transformation already registered")
        cls.transformations[transformation_name] = (
            transformation,
            schema,
        )
