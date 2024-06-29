"""
Defines classes and methods related to ``Condition``
"""

import logging
from simpleeval import simple_eval  # type: ignore
from pytransflow.exceptions import ConditionNotMetException
from pytransflow.core.record import Record
from pytransflow.core.eval import SimpleEval

logger = logging.getLogger(__name__)


class Condition:
    """Implements methods that handle conditions"""

    @staticmethod
    def check(condition: str, record: Record) -> bool:
        """Checks condition using simpleeval

        Args:
            condition: Condition expression
            record: Record

        Returns:
            True if condition is met, otherwise false

        Raises:
            ConditionNotMetException - If condition is not met

        """
        try:
            if not simple_eval(
                condition,
                names={"record": record},
                functions=SimpleEval.functions,
            ):
                logger.warning("Condition not met!")
                raise ConditionNotMetException(condition)
            logger.debug("Condition met!")
            return True
        except (KeyError, TypeError) as err:
            logger.warning("Condition not met, error: %s", err)
            raise ConditionNotMetException(condition) from err
        except SyntaxError as err:
            logger.error("Condition syntax is not defined properly!")
            raise RuntimeError(f"Condition syntax '{condition}' is not defined properly") from err
