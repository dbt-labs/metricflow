from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import logging
import inspect
from typing import List, Optional, Sequence, Tuple, Type

from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.context.base import InferenceContext


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


RequiredContextsType = Tuple[Optional[Type[InferenceContext]], ...]


class InferenceRule(ABC):
    """Implements some sort of heuristic that produces signals about columns.

    An inference rule produces zero or more `InferenceSignal` instances about whatever
    columns it thinks it should, based on input `InferenceContext`s.

    Concrete implementations should aim to be short and modularized. It is preferred to
    compose multiple small rules that each produce a signal type than to make one large
    rule with complex logic to produce a bunch of signals.
    """

    _required_contexts: RequiredContextsType

    def __init_subclass__(cls) -> None:
        """Initializes `cls._required_contexts` from `cls.process` type annotations."""
        parameters = inspect.signature(cls.process).parameters
        context_types: List[Optional[Type[InferenceContext]]] = []
        for param in list(parameters.values())[1:]:  # exclude self
            if param.annotation == inspect.Parameter.empty:
                logger.warning(
                    f"`{cls.__name__}.process`'s parameters must all be type annotated. "
                    "Unanottated types will be provided None contexts, which might trigger further runtime errors."
                )
                context_types.append(None)
            elif not issubclass(param.annotation, InferenceContext):
                logger.warning(
                    f"`{cls.__name__}.process` must only accept input contexts as arguments (except for `self`). "
                    "Extra arguments will always be None."
                )
                context_types.append(None)
            else:
                context_types.append(param.annotation)

        cls._required_contexts = tuple(context_types)

        super().__init_subclass__()

    @property
    def required_contexts(self) -> RequiredContextsType:
        """The context types this rule requires to run successfully."""
        return self._required_contexts

    @abstractmethod
    def process(self, *contexts: Sequence[InferenceContext]) -> List[InferenceSignal]:
        """The actual rule implementation that returns a list of signals based on the input contexts.

        All input context instances will be provided in the same order as specified in REQUIRED_CONTEXTS.
        """
        raise NotImplementedError
