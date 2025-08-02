from __future__ import annotations

import logging
from typing import ContextManager, Optional, Type

from metricflow_semantics.collection_helpers.mf_type_aliases import ExceptionTracebackAnyType
from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder import MetricflowPathfinder
from metricflow_semantics.experimental.mf_graph.path_finding.traversal_profile import (
    GraphTraversalProfile,
    MutableGraphTraversalProfile,
)

logger = logging.getLogger(__name__)


class TraversalProfileDiffer(ContextManager["TraversalProfileDiffer"]):
    """A context manager to help log pathfinder performance during development.

    This and associated classes are a WIP and may be removed.
    """

    def __init__(self, path_finder: MetricflowPathfinder) -> None:  # noqa: D107
        self._path_finder = path_finder
        self._start_profile: GraphTraversalProfile = MutableGraphTraversalProfile()
        self._profile_delta: GraphTraversalProfile = MutableGraphTraversalProfile()

    def __enter__(self) -> TraversalProfileDiffer:  # noqa: D105
        self._start_profile = self._path_finder.traversal_profile_snapshot
        return self

    def __exit__(  # noqa: D105
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[ExceptionTracebackAnyType],
    ) -> None:
        self._profile_delta = self._path_finder.traversal_profile_snapshot.difference(self._start_profile)

    @property
    def profile_delta(self) -> GraphTraversalProfile:  # noqa: D102
        return self._profile_delta
