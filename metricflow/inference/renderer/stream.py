from __future__ import annotations

import sys
from typing import List, TextIO

from metricflow.inference.models import InferenceResult
from metricflow.inference.renderer.base import InferenceRenderer


class StreamInferenceRenderer(InferenceRenderer):
    """Writes inference results to a TextIO as human-readable text."""

    def __init__(self, stream: TextIO) -> None:
        """Initializes the renderer.

        stream: the `TextIO` to write outputs to.
        """
        self.stream = stream

    @staticmethod
    def stdout() -> StreamInferenceRenderer:
        """Factory method to create a `StreamInferenceRenderer` that writes to stdout."""
        return StreamInferenceRenderer(stream=sys.stdout)

    def render(self, results: List[InferenceResult]) -> None:
        """Write the results to the configured TextIO as human-readable text."""
        reason_delim = "\n  - "
        for i, result in enumerate(results):
            reasons_str = "No reason found for this result."
            if len(result.reasons) > 0:
                reasons_str = reason_delim + reason_delim.join(result.reasons)
            lines = [
                f"{result.column.sql} (#{i + 1})\n",
                f"  type: {result.type_node.name}\n",
                "  reasons: ",
                reasons_str,
                "\n\n",
            ]
            self.stream.writelines(lines)
