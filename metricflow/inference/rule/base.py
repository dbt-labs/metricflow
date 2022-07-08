from __future__ import annotations
from abc import ABC, abstractmethod

from dataclasses import dataclass
from enum import Enum
import logging
from typing import List, Tuple, Type

from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.context.base import InferenceContext
from metricflow.inference.context.data_warehouse import DataWarehouseInferenceContext


logger = logging.getLogger(__name__)


class InferenceSignalConfidence(Enum):
    """A discrete enumeration of possible confidence values for an inference signal.

    We chose discrete confidence values instead of a continuous range (e.g a float between 0 and 1)
    to standardize confidence outputs between heuristic rules. We want to avoid different rules
    assuming different values for, say, "medium" or "high" confidence, since this could skew
    results in favor/against certain rules.
    """

    FOR_SURE = 3
    HIGH = 2
    MEDIUM = 1
    LOW = 0


class InferenceSignalType(str, Enum):
    """The type of the inferred signal about a column. Effectively, this represents what a rule can think a column is."""

    # identifiers
    IDENTIFER = "identifier"
    PRIMARY_IDENTIFIER = "identifier_primary"
    FOREIGN_IDENTIFIER = "identifier_foreign"

    # dimension detection
    DIMENSION = "dimension"
    TIME_DIMENSION = "dimension_time"
    CATEGORICAL_DIMENSION = "dimension_categorical"

    # measure fields, i.e, not an ID nor a dimension
    MEASURE_FIELD = "measure_field"

    @staticmethod
    def conflict(a: InferenceSignalType, b: InferenceSignalType) -> bool:
        """Helper function for determining whether two signal types conflict with each other.

        This allows us to implement broad signal types with specializations. It makes it possible
        for rules to produce results such as "I think this is an identifier, but I don't know
        whether it is primary or foreign".

        Example: A column cannot be both PRIMARY_IDENTIFIER and a FOREIGN_IDENTIFIER,
        so there is a conflict in this case. However, a column that is a TIME_DIMENSION is still
        a DIMENSION, so no conflict is found in that case.

        NOTE: The implementation assumes that broader types are prefixes of more specific types,
        e.g, "identifier" (generic) is a prefix of "identifier_primary" (specific).
        """
        return not a.value.startswith(b.value) and not b.value.startswith(a.value)


@dataclass(frozen=True)
class InferenceSignal:
    """Encapsulates a piece of evidence about a column produced by an inference rule.

    column: the target column for this signal
    type: the type of the signal
    reason: a human-readable string that explains why this signal was produced. It may
        eventually reach the user's eyeballs.
    confidence: the confidence that this signal is correct.
    """

    column: SqlColumn
    type: InferenceSignalType
    reason: str
    confidence: InferenceSignalConfidence


RequiredContextsType = Tuple[Type[InferenceContext], ...]


class InferenceRule(ABC):
    """Implements some sort of heuristic that produces signals about columns.

    An inference rule produces zero or more `InferenceSignal` instances about whatever
    columns it thinks it should, based on input `InferenceContext`s.

    Concrete implementations should aim to be short and modularized. It is preferred to
    compose multiple small rules that each produce a signal type than to make one large
    rule with complex logic to produce a bunch of signals.

    It currently only accepts DataWarehouseInferenceContext as input, but this will probably
    be generalized later.
    """

    @abstractmethod
    def process(self, warehouse: DataWarehouseInferenceContext) -> List[InferenceSignal]:
        """The actual rule implementation that returns a list of signals based on the input contexts."""
        raise NotImplementedError
