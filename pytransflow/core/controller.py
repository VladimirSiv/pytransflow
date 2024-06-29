"""
Defines classes and methods related to the ``Controller``
"""

import logging
from typing import Union
from pytransflow.core.record import FailedRecord, Record
from pytransflow.core.analyzer import Analyzer
from pytransflow.core.transformation import Transformation
from pytransflow.exceptions import (
    AnalyzerBaseException,
    TransformationBaseException,
    ControllerTransformationFailedException,
)

logger = logging.getLogger(__name__)


class Controller:
    """Implements Controller logic

    Controller calls ``Analyzer`` to perform analysis before executing a transformation.
    It controls what happens if something fails during the processing.

    Args:
        record: Record to be processed
        transformation: Transformation to be applied

    """

    @staticmethod
    def process_record(
        record: Record,
        transformation: Transformation,
    ) -> Union[Record, FailedRecord]:
        """Applies transformation to a record, if something goes wrong returns a FailedRecord"""
        analyzer = Analyzer(transformation, record)
        try:
            logger.debug(
                "Controller process: Transformation: %s, record: %s",
                transformation,
                record,
            )
            if not analyzer.should_perform_transformation():
                return record
            return transformation.execute(record)
        except (AnalyzerBaseException, TransformationBaseException) as err:
            logger.warning("Transformation failed with error: %s", err)
            return FailedRecord(
                record=record,
                transformation_name=transformation.__class__.__name__,
                transformation_configuration=transformation.config,
                error=err,
            )
        except Exception as e_err:
            logger.error("Unexpected Controller Transformation Failure, error: %s", e_err)
            raise ControllerTransformationFailedException(e_err) from e_err
