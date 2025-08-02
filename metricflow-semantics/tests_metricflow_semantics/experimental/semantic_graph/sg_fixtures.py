from __future__ import annotations

import logging
from functools import cached_property

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.mf_graph.path_finding.pathfinder import MetricflowPathfinder
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_path import (
    RecipeWriterPathfinder,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.sg_linkable_spec_resolver import (
    SemanticGraphLinkableSpecResolver,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import (
    SemanticGraph,
)
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.linkable_spec_index_builder import LinkableSpecIndexBuilder
from metricflow_semantics.model.semantics.linkable_spec_resolver import LegacyLinkableSpecResolver
from metricflow_semantics.model.semantics.manifest_object_lookup import SemanticManifestObjectLookup
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.test_helpers.snapshot_helpers import SnapshotConfiguration

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class SemanticGraphTestFixture:
    """A temporary fixture used in test cases to help migrating to the semantic graph."""

    request: FixtureRequest
    snapshot_configuration: SnapshotConfiguration
    semantic_manifest: SemanticManifest

    @cached_property
    def manifest_object_lookup(self) -> ManifestObjectLookup:  # noqa: D102
        return ManifestObjectLookup(self.semantic_manifest)

    @cached_property
    def semantic_graph(self) -> SemanticGraph:  # noqa: D102
        builder = SemanticGraphBuilder(manifest_object_lookup=self.manifest_object_lookup)
        return builder.build()

    def create_legacy_resolver(self) -> LegacyLinkableSpecResolver:  # noqa: D102
        semantic_manifest = self.semantic_manifest
        semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
        linkable_spec_index_builder = LinkableSpecIndexBuilder(
            semantic_manifest=semantic_manifest,
            semantic_model_lookup=semantic_manifest_lookup.semantic_model_lookup,
            manifest_object_lookup=SemanticManifestObjectLookup(semantic_manifest),
            max_entity_links=MAX_JOIN_HOPS,
        )
        linkable_spec_index = linkable_spec_index_builder.build_index()
        legacy_linkable_spec_resolver = LegacyLinkableSpecResolver(
            semantic_manifest=semantic_manifest,
            semantic_model_lookup=semantic_manifest_lookup.semantic_model_lookup,
            manifest_object_lookup=SemanticManifestObjectLookup(semantic_manifest),
            linkable_spec_index=linkable_spec_index,
        )

        return legacy_linkable_spec_resolver

    def create_sg_resolver(self) -> SemanticGraphLinkableSpecResolver:  # noqa: D102
        return SemanticGraphLinkableSpecResolver(
            semantic_graph=self.semantic_graph,
            path_finder=self.pathfinder,
        )

    @cached_property
    def sg_resolver(self) -> SemanticGraphLinkableSpecResolver:  # noqa: D102
        return self.create_sg_resolver()

    @cached_property
    def legacy_resolver(self) -> LegacyLinkableSpecResolver:  # noqa: D102
        return self.create_legacy_resolver()

    @cached_property
    def pathfinder(self) -> RecipeWriterPathfinder:  # noqa: D102
        return MetricflowPathfinder()
