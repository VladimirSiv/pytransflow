"""
Defines classes and methods related to ``Analyzer``
"""

import logging
from typing import List
from pytransflow.exceptions import (
    ConditionNotMetException,
    FieldDoesNotExistException,
    OutputAlreadyExistsException,
)
from pytransflow.core.record import Record
from pytransflow.core.transformation import Transformation
from pytransflow.exceptions import AnalyzerBaseException
from pytransflow.core.resolver import Resolver
from pytransflow.core.condition import Condition


logger = logging.getLogger(__name__)


class Analyzer:
    """Implements Analyzer logic

    Analyzes the initial state of the transformation and decides if the
    transformation can be executed based on the transformation configuration
    and the state of the record

    Args:
        transformation: Transformation that should be applied
        record: Record to be analyzed

    Attributes:
        config: Transformation configuration
        record: Record to be analyzed

    """

    def __init__(
        self,
        transformation: Transformation,
        record: Record,
    ) -> None:
        self.config = transformation.config
        self.variables = transformation.variables
        self.record = record

    def should_perform_transformation(self) -> bool:
        """Performs the analysis"""
        required_fields = self.config.get_required_fields()
        output_fields = self.config.get_output_fields()
        try:
            self._check_required_fields(fields=required_fields)
            self._check_output_field(output_fields=output_fields, record=self.record)
            self._check_condition(self.record)
            return True
        except AnalyzerBaseException as a_err:
            if isinstance(a_err, ConditionNotMetException):
                return False
            if a_err.name in self.config.schema.ignore_errors:
                return False
            raise

    def _check_condition(
        self,
        record: Record,
    ) -> None:
        """Checks if the condition is met

        Args:
            record: Record to be analyzed

        Raises:
            ConditionNotMet: If condition fails to be checked or is not met

        """
        condition = self.config.schema.condition
        if condition is not None:
            condition = Resolver.resolve_condition(condition, self.variables)
            Condition.check(condition, record)
            logger.debug("Checking condition: %s", condition)

    def _check_required_fields(
        self,
        fields: List[str],
    ) -> None:
        """Confirms that required fields for transformation execution are
        present in the record

        Args:
            fields: Required fields

        Raises:
            FieldDoesNotExist: If required field does not exist

        """
        for field in fields:
            if not self.record.contains(field):
                logger.warning("Record does not contain field: %s", field)
                raise FieldDoesNotExistException(field)

    def _check_output_field(
        self,
        output_fields: List[str],
        record: Record,
    ) -> None:
        """Checks if output field is in the record and if it should be ignored.

        Args:
            output_fields: Transformation output fields
            record: Record to be analyzed

        Raises:
            ``OutputAlreadyExists``: If the output is in the record and the
                error is not ignored

        """
        for output in output_fields:
            logger.info("CHECKING OUTPUT FIELD %s", output)
            if (
                output in record
                and OutputAlreadyExistsException.name not in self.config.schema.ignore_errors
            ):
                logger.warning("Record already contains output field: %s", output)
                raise OutputAlreadyExistsException(output)
