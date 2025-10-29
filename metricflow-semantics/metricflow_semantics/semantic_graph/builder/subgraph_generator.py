from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Sequence

from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.sg_interfaces import (
    SemanticGraphEdge,
)

logger = logging.getLogger(__name__)


class SemanticSubgraphGenerator(ABC):
    """Generates a specific portion of the semantic graph.

    As the semantic graph has many elements, it's helpful to break down the generation of the graph into logical pieces.
    For example, the `TimeEntitySubgraphGenerator` creates a subgraph containing the nodes and edges related to the
    time entities.

    To create the complete semantic graph, the output of each generator is combined into a single graph.

    Only the edges need to be returned as the graph implementation adds the end nodes of the edge as needed.
    """

    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:  # noqa: D107
        self._manifest_object_lookup = manifest_object_lookup

    @abstractmethod
    def add_edges_for_manifest(self, edge_list: list[SemanticGraphEdge]) -> None:
        """Add edges appropriate for the manifest / generator to the `edge_list`.

        This method mutates `edge_list` to reduce the overhead of concatenating edges from multiple generators. This is
        a factor for manifests with high complexity.
        """
        raise NotImplementedError

    def generate_edges(self) -> Sequence[SemanticGraphEdge]:
        """Return a list of edges that should be added to the graph."""
        edge_list: list[SemanticGraphEdge] = []
        self.add_edges_for_manifest(edge_list)
        return edge_list
