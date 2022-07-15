"""Module with common models that are used across multiple classes/modules in inference."""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Optional, List
from enum import Enum

from metricflow.dataflow.sql_column import SqlColumn


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


class InferenceSignalNode(ABC):
    """A node in the inference signal type hierarchy.

    This class can be used to assembly a type hierarchy tree. It can be used by heuristics
    to check whether signals produced by rules are conflicting or complimentary, relying
    on the property that sibling nodes are mutually exclusive in the hierarchy.
    """

    def __init__(self, parent: Optional[InferenceSignalNode], name: str) -> None:  # noqa: D
        self.name = name

        self.parent = parent
        self.children: List[InferenceSignalNode] = []

        if parent is not None:
            parent.children.append(self)

    def __str__(self) -> str:  # noqa: D
        return f"InferenceSignalNode(name={self.name})"

    @property
    def ancestors(self) -> List[InferenceSignalNode]:
        """The list of all ancestors for this node, from the root to the direct parent."""
        if self.parent is None:
            return []

        return self.parent.ancestors + [self.parent]

    def is_descendant(self, other: InferenceSignalNode) -> bool:
        """Whether self is a descendant of other."""
        return other in self.ancestors


# This is kinda horrible but there's no way of instancing the tree with type safety without
# hardcoding it. Having some magic that dynamically assigns attributes could work, but then
# we lose IDE autocompletion and static checking
class _TreeNodes:  # noqa: D
    root = InferenceSignalNode(None, "UNKNOWN")
    id = InferenceSignalNode(root, "IDENTIFIER")
    foreign_id = InferenceSignalNode(id, "FOREIGN_IDENTIFIER")
    unique_id = InferenceSignalNode(id, "UNIQUE_IDENTIFIER")
    primary_id = InferenceSignalNode(unique_id, "PRIMARY_IDENTIFIER")
    dimension = InferenceSignalNode(root, "DIMENSION")
    time_dimension = InferenceSignalNode(dimension, "TIME_DIMENSION")
    categorical_dimension = InferenceSignalNode(dimension, "CATEGORICAL_DIMENSION")
    measure = InferenceSignalNode(root, "MEASURE")


class InferenceSignalType:
    """All possible inference signal types."""

    UNKNOWN = _TreeNodes.root

    class ID:
        """Indicates a column was inferred to be an ID."""

        UNKNOWN = _TreeNodes.id
        FOREIGN = _TreeNodes.foreign_id
        UNIQUE = _TreeNodes.unique_id
        PRIMARY = _TreeNodes.primary_id

    class DIMENSION:
        """Indicates a column was inferred to be a dimension."""

        UNKNOWN = _TreeNodes.dimension
        TIME = _TreeNodes.time_dimension
        CATEGORICAL = _TreeNodes.categorical_dimension

    class MEASURE:
        """Indicates a column was inferred to be a measure."""

        UNKNOWN = _TreeNodes.measure


@dataclass(frozen=True)
class InferenceSignal:
    """Encapsulates a piece of evidence about a column produced by an inference rule.

    column: the target column for this signal
    type_node: the type node of the signal
    reason: a human-readable string that explains why this signal was produced. It may
        eventually reach the user's eyeballs.
    confidence: the confidence that this signal is correct.
    """

    column: SqlColumn
    type_node: InferenceSignalNode
    reason: str
    confidence: InferenceSignalConfidence
