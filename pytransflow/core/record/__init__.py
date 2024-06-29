"""
Record module exports
"""

from pytransflow.core.record.record import Record
from pytransflow.core.record.failed import FailedRecord

__all__ = [
    "Record",
    "FailedRecord",
]
