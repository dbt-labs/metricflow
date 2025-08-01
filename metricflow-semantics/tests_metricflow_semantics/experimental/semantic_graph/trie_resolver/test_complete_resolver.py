from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.naming.keywords import DUNDER
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.nodes.node_labels import MeasureLabel, MetricLabel
from metricflow_semantics.experimental.semantic_graph.trie_resolver.complete_resolver import CompleteTrieResolver
from metricflow_semantics.experimental.semantic_graph.trie_resolver.simple_resolver import SimpleTrieResolver
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.sg_fixtures import SemanticGraphTestFixture

logger = logging.getLogger(__name__)


def test_measure(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_05_derived_metric_manifest: SemanticManifest,
) -> None:
    fixture = SemanticGraphTestFixture(
        request=request, snapshot_configuration=mf_test_configuration, semantic_manifest=sg_05_derived_metric_manifest
    )
    semantic_graph = fixture.semantic_graph
    resolver = CompleteTrieResolver(semantic_graph=semantic_graph, path_finder=fixture.pathfinder)
    measure_node = semantic_graph.node_with_label(MeasureLabel.get_instance("booking_count"))
    result = resolver.resolve_trie(source_nodes=FrozenOrderedSet((measure_node,)), element_filter=None)

    lines: list[str] = []
    for indexed_dunder_name, descriptor in result.dunder_name_trie.name_items():
        if descriptor.element_type is LinkableElementType.METRIC:
            prefix = DUNDER.join(indexed_dunder_name) + " - "
            for entity_key_query in descriptor.entity_key_queries_for_group_by_metric:
                model_ids = FrozenOrderedSet(
                    descriptor.derived_from_model_ids + entity_key_query.derived_from_model_ids
                )
                lines.append(
                    prefix
                    + DUNDER.join(entity_key_query.entity_key_query)
                    + " - "
                    + ",".join(model_id.model_name for model_id in model_ids)
                )
        else:
            lines.append(DUNDER.join(indexed_dunder_name))

    logger.debug(
        LazyFormat(
            "Resolved trie.",
            # dunder_name_trie=result.dunder_name_trie,
            # result=result,
            lines="\n".join(lines),
        )
    )


def test_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_05_derived_metric_manifest: SemanticManifest,
) -> None:
    fixture = SemanticGraphTestFixture(
        request=request, snapshot_configuration=mf_test_configuration, semantic_manifest=sg_05_derived_metric_manifest
    )
    semantic_graph = fixture.semantic_graph
    resolver = SimpleTrieResolver(semantic_graph=semantic_graph, path_finder=fixture.pathfinder)
    resolver._verbose_debug_logs = True

    metric_node = semantic_graph.node_with_label(MetricLabel.get_instance("views_per_booking"))
    result = resolver.resolve_trie(source_nodes=FrozenOrderedSet((metric_node,)), element_filter=None)
    logger.debug(
        LazyFormat(
            "Resolved trie.",
            # dunder_name_trie=result.dunder_name_trie,
            dunder_names=result.dunder_name_trie.dunder_names(),
            stat_delta=result.traversal_profile,
            execution_time=result.execution_time,
        )
    )
