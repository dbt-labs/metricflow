from __future__ import annotations

from collections import defaultdict
from typing import Callable, Dict, List, Optional

from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.models import (
    InferenceResult,
    InferenceSignal,
    InferenceSignalConfidence,
    InferenceSignalNode,
    InferenceSignalType,
)
from metricflow.inference.solver.base import InferenceSolver

NodeWeighterFunction = Callable[[InferenceSignalConfidence], int]


class WeightedTypeTreeInferenceSolver(InferenceSolver):
    """Assigns weights to each type in the column type tree and attempts to traverse it using a weight percentage threshold."""

    @staticmethod
    def default_weighter_function(confidence: InferenceSignalConfidence) -> int:
        """The default weighter function.

        It assigns weights 1, 2, 4 and 8 for LOW, MEDIUM, HIGH and VERY_HIGH confidences, respectively. It then sums
        the weights of all provided confidence scores.
        """
        confidence_weight_map = {
            InferenceSignalConfidence.LOW: 1,
            InferenceSignalConfidence.MEDIUM: 2,
            InferenceSignalConfidence.HIGH: 4,
            InferenceSignalConfidence.VERY_HIGH: 8,
        }

        return confidence_weight_map[confidence]

    def __init__(
        self, weight_percent_threshold: float = 0.6, weighter_function: Optional[NodeWeighterFunction] = None
    ) -> None:
        """Initialize the solver.

        weight_percent_threshold: a number in (0.5, 1]. If a node's weight corresponds to a percentage
            above this threshold with respect to its siblings' total weight sum, the solver will progress deeper
            into the type tree, entering that node. If not, it stops at the parent.
        weighter_function: a function that returns a weight given a confidence score. It will be used
            to assign integer weights to each node in the type tree based on its input signals.
        """
        assert (
            weight_percent_threshold > 0.5 and weight_percent_threshold <= 1
        ), f"weight_percent_threshold is {weight_percent_threshold}, but it must be > 0.5 and <= 1!"
        self._weight_percent_threshold = weight_percent_threshold

        self._weighter_function = (
            weighter_function
            if weighter_function is not None
            else WeightedTypeTreeInferenceSolver.default_weighter_function
        )

    def _get_cumulative_weights_for_root(
        self,
        root: InferenceSignalNode,
        output_weights: Dict[InferenceSignalNode, int],
        output_parent_only_weights: Dict[InferenceSignalNode, int],
        signals_by_type: Dict[InferenceSignalNode, List[InferenceSignal]],
    ) -> Dict[InferenceSignalNode, int]:
        """Get a dict of cumulative weights, starting at `root`.

        A parent node's weight is the sum of all its children plus its own weight. Children tagged as only
        applying to their parent are excluded from their grand-parent's sum.

        root: the root to start assigning cumulative weights from.
        output_weights: the output dictionary to assign the cumulative weights to
        output_parent_only_weights: similar to output_weights, but the weight of each node excludes the weights
            of all of its parent-only children
        signals_by_type: a dictionary that maps signal type nodes to signals
        """
        for child in root.children:
            self._get_cumulative_weights_for_root(
                root=child,
                output_weights=output_weights,
                output_parent_only_weights=output_parent_only_weights,
                signals_by_type=signals_by_type,
            )

        output_weights[root] = sum(self._weighter_function(signal.confidence) for signal in signals_by_type[root])
        output_weights[root] += sum(output_parent_only_weights[child] for child in root.children)

        output_parent_only_weights[root] = sum(
            self._weighter_function(signal.confidence)
            for signal in signals_by_type[root]
            if not signal.only_applies_to_parent
        )

        return output_weights

    def _get_cumulative_weights(self, signals: List[InferenceSignal]) -> Dict[InferenceSignalNode, int]:
        """Get the cumulative weights dict for a list of signals."""
        signals_by_type: Dict[InferenceSignalNode, List[InferenceSignal]] = defaultdict(list)
        for signal in signals:
            signals_by_type[signal.type_node].append(signal)

        return self._get_cumulative_weights_for_root(
            root=InferenceSignalType.UNKNOWN,
            output_weights=defaultdict(int),
            output_parent_only_weights=defaultdict(int),
            signals_by_type=signals_by_type,
        )

    def solve_column(self, column: SqlColumn, signals: List[InferenceSignal]) -> InferenceResult:
        """Find the appropriate type for a column by traversing through the type tree.

        It traverses the tree by giving weights to all nodes and greedily finding the path with the most
        weight until it either finds a leaf or there is a "weight bifurcation" in the path with respect
        to the provided `weight_percent_threshold`.
        """
        if len(signals) == 0:
            return InferenceResult(
                column=column,
                type_node=InferenceSignalType.UNKNOWN,
                reasons=[],
                problems=[
                    "No signals were extracted for this column",
                    "Inference solver could not determine if column was an entity, a dimension, or a measure",
                ],
            )

        reasons_by_type: Dict[InferenceSignalNode, List[str]] = defaultdict(list)
        for signal in signals:
            reasons_by_type[signal.type_node].append(f"{signal.reason} ({signal.type_node.name})")

        node_weights = self._get_cumulative_weights(signals)

        reasons: List[str] = []
        problems: List[str] = []
        node = InferenceSignalType.UNKNOWN
        while len(node.children) > 0:
            children_weight_total = sum(node_weights[child] for child in node.children)

            if children_weight_total == 0:
                break

            next_node = None
            for child in node.children:
                if node_weights[child] / children_weight_total >= self._weight_percent_threshold:
                    next_node = child
                    reasons += reasons_by_type[child]
                    break

            if next_node is None:
                if len(node.children) > 0:  # there was confusion
                    children_weight_strings = [
                        f"{child.name} (weight {node_weights[child]})"
                        for child in node.children
                        if node_weights[child] != 0
                    ]
                    child_str = " / ".join(children_weight_strings)
                    problems.append(f"Solver is confused between {child_str}")
                break

            node = next_node

        if node == InferenceSignalType.UNKNOWN:
            problems.append("Inference solver could not determine if column was an entity, a dimension, or a measure")

        return InferenceResult(column=column, type_node=node, reasons=reasons, problems=problems)
