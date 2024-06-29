import pytest
from pytransflow.core.transformation.catalogue import TransformationCatalogue
from pytransflow.exceptions.flow import TransformationDoesNotExistException
from pytransflow.transformations.rename import RenameTransformation, RenameTransformationSchema


def test_get_transformation():
    transformation = TransformationCatalogue.get_transformation("rename")
    assert transformation == (RenameTransformation, RenameTransformationSchema)


def test_transformation_is_not_in_catalogue():
    with pytest.raises(TransformationDoesNotExistException):
        TransformationCatalogue.get_transformation("something")
