from __future__ import annotations

from typing import Callable, List

from metricflow.inference.context.data_warehouse import ColumnProperties, DataWarehouseInferenceContext
from metricflow.inference.rule.base import InferenceRule
from metricflow.inference.models import (
    InferenceSignal,
    InferenceSignalConfidence,
    InferenceSignalNode,
)


ColumnMatcher = Callable[[ColumnProperties], bool]


class ColumnMatcherRule(InferenceRule):
    """Inference rule that checks for matches across all columns.

    This is a useful shortcut for making rules that match columns one by one with preset confidence
    values, types and match reasons.

    If you need a more specific rule with varying confidence, column cross-checking and that outputs
    multiple types, inherit from `InferenceRule` directly.
    """

    def __init__(
        self,
        matcher: ColumnMatcher,
        type_node: InferenceSignalNode,
        confidence: InferenceSignalConfidence,
        match_reason: str,
    ) -> None:
        """Initialize the class.

        matcher: a function to determine whether `ColumnProperties` matches. If it does, produce the signal
        type_node: the `InferenceSignalNode` to produce whenever the pattern is matched
        confidence: the `InferenceSignalConfidence` to produce whenever the pattern is matched
        """
        self.matcher = matcher
        self.type_node = type_node
        self.confidence = confidence
        self.match_reason = match_reason

    def process(self, warehouse: DataWarehouseInferenceContext) -> List[InferenceSignal]:  # type: ignore
        """Try to match all columns' properties with the matching function.

        If they do match, produce a signal with the configured type and confidence.
        """
        matching_columns = [column for column, props in warehouse.columns.items() if self.matcher(props)]
        signals = [
            InferenceSignal(
                column=column,
                type_node=self.type_node,
                reason=self.match_reason,
                confidence=self.confidence,
            )
            for column in matching_columns
        ]
        return signals
