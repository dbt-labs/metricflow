from collections import defaultdict
from typing import Callable, Dict, List, Tuple, Optional

from metricflow.inference.rule.base import (
    InferenceSignal,
    InferenceSignalConfidence,
    InferenceSignalNode,
    InferenceSignalType,
)
from metricflow.inference.solver.base import InferenceSolver


NodeWeighterFunction = Callable[[List[InferenceSignalConfidence]], int]


class WeightedTypeTreeInferenceSolver(InferenceSolver):
    """Assigns weights to each type in the column type tree and attemptins to traverse it using a weight percentage threshold."""

    @staticmethod
    def default_weighter_function(confidence_scores: List[InferenceSignalConfidence]) -> int:
        """The default weighter function.

        It assigns weights 1, 2, 3 and 5 for LOW, MEDIUM, HIGH and FOR_SURE confidences, respectively. It then sums
        the weights of all provided confidence scores.
        """
        confidence_weight_map = {
            InferenceSignalConfidence.LOW: 1,
            InferenceSignalConfidence.MEDIUM: 2,
            InferenceSignalConfidence.HIGH: 3,
            InferenceSignalConfidence.FOR_SURE: 5,
        }

        return sum(confidence_weight_map[confidence] for confidence in confidence_scores)

    def __init__(
        self, weight_percent_threshold: float = 0.9, weighter_function: Optional[NodeWeighterFunction] = None
    ) -> None:
        """Initialize the solver.

        weight_percent_threshold: a number between 0.5 and 1. If a node's weight corresponds to a percentage
            above this threshold with respect to its siblings' total weight sum, the solver will progress deeper
            into the type tree, entering that node. If not, it stops at the parent.
        weighter_function: a function that returns a weight given a list of confidence scores. It will be used
            to assign integer weights to each node in the type tree based on its input signals.
        """
        assert weight_percent_threshold >= 0.5 and weight_percent_threshold <= 1
        self._weight_percent_threshold = weight_percent_threshold

        self._weighter_function = (
            weighter_function
            if weighter_function is not None
            else WeightedTypeTreeInferenceSolver.default_weighter_function
        )

    def _set_cumulative_weight(self, root: InferenceSignalNode, weights_dict: Dict[InferenceSignalNode, int]) -> None:
        """Turn a dictionary of node weights in a tree into a cumulative weight dict, starting at `root`.

        It makes it so that a parent node's weight is the sum of all its children plus its own weight.
        """
        if len(root.children) == 0:
            return

        for child in root.children:
            self._set_cumulative_weight(child, weights_dict)

        weights_dict[root] += sum(weights_dict[child] for child in root.children)

    def solve_column(self, signals: List[InferenceSignal]) -> Tuple[InferenceSignalNode, List[str]]:
        """Find the appropriate type for a column by traversing through the type tree.

        It traverses the tree by giving weights to all nodes and greedily finding the path with the most
        weight until it either finds a leaf or there is a "weight bifurcation" in the path with respect
        to the provided `weight_percent_threshold`.
        """
        if len(signals) == 0:
            return InferenceSignalType.UNKNOWN, [
                "No signals were extracted for this column, so we know nothing about it."
            ]

        confidences_by_type: Dict[InferenceSignalNode, List[InferenceSignalConfidence]] = defaultdict(list)
        for signal in signals:
            confidences_by_type[signal.type_node].append(signal.confidence)

        node_weights: Dict[InferenceSignalNode, int] = defaultdict(lambda: 0)
        for type_node, confidences_for_type in confidences_by_type.items():
            node_weights[type_node] = self._weighter_function(confidences_for_type)

        self._set_cumulative_weight(InferenceSignalType.UNKNOWN, node_weights)

        node = InferenceSignalType.UNKNOWN
        while node is not None and len(node.children) > 0:
            children_weight_total = sum(node_weights[child] for child in node.children)

            if children_weight_total == 0:
                break

            next_node = None
            for child in node.children:
                if node_weights[child] / children_weight_total >= self._weight_percent_threshold:
                    next_node = child
                    break

            if next_node is None:
                break

            node = next_node

        # TODO: return reasons too
        return node, []
