"""
Defines classes and methods related to ``ExceptionHandler``
"""

import logging
from typing import Dict, Any, Callable
from pytransflow.core.record import Record
from pytransflow.exceptions.transformation import TransformationBaseException

logger = logging.getLogger(__name__)


class ExceptionHandler:
    """Implements Exception Handler

    Exception Handler is a callable class that defines a wrapper that handles the exceptions raised
    from a Transformation. It checks if a raised exception is in `ignore_errors` and acts
    accordingly.

    Returns:
        TransformationResult: If raised exception is in `ignore_errors` this returned
            `TransformationResult` will have `successful` equal to False

    Raises:
        TransformationBaseError: Raises subclass of TransformationBaseError
            that is raised from a transformation

    """

    def __call__(
        self,
        function: Callable[[Any, Record], Record],
    ) -> Callable[[Any, Record], Record]:
        def wrapper(
            *args: Any,
            **kwargs: Dict[str, Any],
        ) -> Record:
            config = args[0].config
            try:
                return function(*args, **kwargs)
            except TransformationBaseException as err:
                if err.name not in config.schema.ignore_errors:
                    logger.error("Error occured while applying transformation: %s", err)
                    raise err
                logger.warning(
                    "Error occured while applying transformation, but it's ignored: %s",
                    err,
                )
                return args[1]  # type: ignore[no-any-return]

        return wrapper
