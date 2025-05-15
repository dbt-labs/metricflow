from __future__ import annotations

import logging

from metricflow_semantics.experimental.semantic_graph.builder.semantic_graph_builder_rule import (
    RuleInput,
    SemanticGraphBuilderRule,
)
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph
from metricflow_semantics.experimental.semantic_graph.semantic_model_lookup import ManifestObjectLookup
from metricflow_semantics.time.time_spine_source import TimeSpineSource

logger = logging.getLogger(__name__)


class AddTimeEntitiesRule(SemanticGraphBuilderRule[RuleInput]):
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        super().__init__(manifest_object_lookup)

        time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(
            self._manifest_object_lookup.semantic_manifest
        )
        custom_grain_name_to_expanded_grain_mapping = TimeSpineSource.build_custom_granularities(time_spine_sources.values())

    def update_graph(self, semantic_graph: MutableSemanticGraph) -> None:
        raise NotImplementedError()
