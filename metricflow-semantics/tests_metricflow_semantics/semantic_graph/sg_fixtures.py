from __future__ import annotations

import logging
from functools import cached_property

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.semantic_graph.attribute_resolution.recipe_writer_path import (
    RecipeWriterPathfinder,
)
from metricflow_semantics.semantic_graph.attribute_resolution.sg_linkable_spec_resolver import (
    SemanticGraphGroupByItemSetResolver,
)
from metricflow_semantics.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.sg_interfaces import (
    SemanticGraph,
)
from metricflow_semantics.test_helpers.snapshot_helpers import SnapshotConfiguration
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.path_finding.pathfinder import MetricFlowPathfinder

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

    def create_sg_resolver(self) -> SemanticGraphGroupByItemSetResolver:  # noqa: D102
        return SemanticGraphGroupByItemSetResolver(
            manifest_object_lookup=self.manifest_object_lookup,
            semantic_graph=self.semantic_graph,
            path_finder=self.pathfinder,
        )

    @cached_property
    def sg_resolver(self) -> SemanticGraphGroupByItemSetResolver:  # noqa: D102
        return self.create_sg_resolver()

    @cached_property
    def pathfinder(self) -> RecipeWriterPathfinder:  # noqa: D102
        return MetricFlowPathfinder()
