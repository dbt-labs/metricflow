from __future__ import annotations

import logging

from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest

from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder import MetricflowPathfinder
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.sg_linkable_spec_resolver import (
    SemanticGraphGroupByItemSetResolver,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import SemanticGraphEdge, SemanticGraphNode
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.time.time_spine_source import TimeSpineSource

logger = logging.getLogger(__name__)


class SemanticManifestLookup:
    """Provides convenient lookup methods to get semantic attributes for a manifest."""

    def __init__(self, semantic_manifest: SemanticManifest) -> None:
        """Initializer.

        Args:
            semantic_manifest: The semantic manifest for lookups.
        """
        self._semantic_manifest = semantic_manifest
        self._time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(semantic_manifest)
        self.custom_granularities = TimeSpineSource.build_custom_granularities(list(self._time_spine_sources.values()))
        self._semantic_model_lookup = SemanticModelLookup(
            semantic_manifest=semantic_manifest, custom_granularities=self.custom_granularities
        )

        pathfinder = MetricflowPathfinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]()
        manifest_object_lookup = ManifestObjectLookup(semantic_manifest)
        graph_builder = SemanticGraphBuilder(manifest_object_lookup=manifest_object_lookup)
        semantic_graph = graph_builder.build()

        linkable_spec_resolver = SemanticGraphGroupByItemSetResolver(
            manifest_object_lookup=manifest_object_lookup,
            semantic_graph=semantic_graph,
            path_finder=pathfinder,
        )

        self._metric_lookup = MetricLookup(
            semantic_manifest=semantic_manifest,
            semantic_model_lookup=self._semantic_model_lookup,
            custom_granularities=self.custom_granularities,
            linkable_spec_resolver=linkable_spec_resolver,
        )

    @property
    def semantic_manifest(self) -> SemanticManifest:  # noqa: D102
        return self._semantic_manifest

    @property
    def semantic_model_lookup(self) -> SemanticModelLookup:  # noqa: D102
        return self._semantic_model_lookup

    @property
    def metric_lookup(self) -> MetricLookup:  # noqa: D102
        return self._metric_lookup
