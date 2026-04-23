from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List

from metricflow.converters.models import OSIDocument


class ConverterIssueType(Enum):
    """Identifies the kind of information loss that occurred during conversion."""

    CONVERSION_METRIC_DROPPED = "CONVERSION_METRIC_DROPPED"
    PRIVATE_METRIC_DROPPED = "PRIVATE_METRIC_DROPPED"
    NATURAL_ENTITY_DROPPED = "NATURAL_ENTITY_DROPPED"
    CUMULATIVE_SEMANTICS_LOSS = "CUMULATIVE_SEMANTICS_LOSS"


@dataclass(frozen=True)
class ConverterIssue:
    """Records a single instance of information loss during MSI→OSI conversion."""

    issue_type: ConverterIssueType
    element_name: str


@dataclass(frozen=True)
class ConverterResult:
    """Return value of MSIToOSIConverter.convert(), pairing the output document with any conversion issues."""

    document: OSIDocument
    issues: List[ConverterIssue]
