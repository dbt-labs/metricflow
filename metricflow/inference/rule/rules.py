from __future__ import annotations

import re
from typing import List

from metricflow.inference.context.data_warehouse import DataWarehouseInferenceContext
from metricflow.inference.rule.base import (
    InferenceRule,
    InferenceSignal,
    InferenceSignalConfidence,
    InferenceSignalType,
)


class ColumnRegexMatcherRule(InferenceRule):
    """Inference rule that checks for matches across all column names."""

    def __init__(
        self, pattern: re.Pattern, signal_type: InferenceSignalType, confidence: InferenceSignalConfidence
    ) -> None:
        """Initialize the class.

        pattern: regex Pattern to match against columns' full names, i.e `<db>.<schema>.<table>.<column>`
        signal_type: the `InferenceSignalType` to produce whenever the pattern is matched
        confidence: the `InferenceSignalConfidence` to produce whenever the pattern is matched
        """
        self.pattern = pattern
        self.signal_type = signal_type
        self.confidence = confidence

    def process(self, warehouse: DataWarehouseInferenceContext) -> List[InferenceSignal]:  # type: ignore
        """Try to match all columns' full names with the matching function or pattern.

        If they do match, produce a signal with the configured type and confidence.
        """
        matching_columns = [column for column in warehouse.columns if self.pattern.match(column.sql)]
        signals = [
            InferenceSignal(
                column=column,
                type=self.signal_type,
                reason=f"Column name matches regex pattern '{self.pattern.pattern}'",
                confidence=self.confidence,
            )
            for column in matching_columns
        ]
        return signals
