from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.experimental.semantic_graph.trie_resolver.entity_key_resolver import (
    EntityKeyTrieResolver,
)
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.svg_snapshot import write_svg_snapshot_for_review

from tests_metricflow_semantics.experimental.mf_graph.formatting.svg_formatter import SvgFormatter
from tests_metricflow_semantics.experimental.semantic_graph.sg_fixtures import SemanticGraphTestFixture

logger = logging.getLogger(__name__)


def test_key_query_resolver(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_07_orphan_foreign_entity_manifest: SemanticManifest,
) -> None:
    fixture = SemanticGraphTestFixture(
        request=request,
        snapshot_configuration=mf_test_configuration,
        semantic_manifest=sg_07_orphan_foreign_entity_manifest,
    )

    semantic_graph = fixture.semantic_graph
    key_query_resolver = EntityKeyTrieResolver(semantic_graph)

    write_svg_snapshot_for_review(
        request=request,
        snapshot_configuration=mf_test_configuration,
        svg_file_contents=semantic_graph.format(SvgFormatter()),
    )

    result = key_query_resolver.resolve_entity_key_trie_mapping()

    logger.debug(LazyFormat("Got result", result=result))
    result_rows = []
    for node, name_trie in result.items():
        for indexed_dunder_name, descriptor in name_trie.name_items():
            result_rows.append([indexed_dunder_name, descriptor])
        # for model_ids, key_queries in key_query_group.pairs_of_model_id_tuple_and_key_query_tuple:
        #     result_rows.append([node, key_queries])
    logger.debug(LazyFormat("Got items", result_rows=result_rows))
