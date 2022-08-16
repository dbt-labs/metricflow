from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Tuple, Type

from metricflow.inference.models import InferenceSignal
from metricflow.inference.context.base import InferenceContext
from metricflow.inference.context.data_warehouse import DataWarehouseInferenceContext


@dataclass(frozen=True)
class InferenceRuleInputContexts:
    """Class that holds all possible input contexts for a rule."""

    _warehouse: Optional[DataWarehouseInferenceContext]

    @property
    def warehouse(self) -> DataWarehouseInferenceContext:
        if self._warehouse is None:
            raise RuntimeError("Cannot access None property")
        return self._warehouse


class InferenceRule(ABC):
    """Implements some sort of heuristic that produces signals about columns.

    An inference rule produces zero or more `InferenceSignal` instances about whatever
    columns it thinks it should, based on input `InferenceContext`s.

    Concrete implementations should aim to be short and modularized. It is preferred to
    compose multiple small rules that each produce a signal type than to make one large
    rule with complex logic to produce a bunch of signals.
    """

    required_contexts: Tuple[Type[InferenceContext]]

    @abstractmethod
    def process(self, contexts: InferenceRuleInputContexts) -> List[InferenceSignal]:
        """The actual rule implementation that returns a list of signals based on the input contexts."""
        raise NotImplementedError
