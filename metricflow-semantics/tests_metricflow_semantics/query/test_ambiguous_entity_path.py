from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.errors.error_classes import InvalidQueryException
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal, assert_str_snapshot_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def multi_hop_query_parser(  # noqa: D103
    simple_multi_hop_join_manifest_lookup: SemanticManifestLookup,
) -> MetricFlowQueryParser:
    return MetricFlowQueryParser(semantic_manifest_lookup=simple_multi_hop_join_manifest_lookup)


def test_resolvable_ambiguous_entity_path(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    multi_hop_query_parser: MetricFlowQueryParser,
) -> None:
    query_spec = multi_hop_query_parser.parse_and_validate_query(
        metric_names=["entity_1_metric"],
        group_by_names=["entity_0__country"],
    ).query_spec

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result_0",
        obj=query_spec,
    )


def test_ambiguous_entity_path_resolves_to_shortest_entity_path_item(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    multi_hop_query_parser: MetricFlowQueryParser,
) -> None:
    """Tests that 'entity_1__country' resolves to 'entity_1__country' not 'entity_1__entity_0__country'."""
    query_spec = multi_hop_query_parser.parse_and_validate_query(
        metric_names=["all_entity_metric"],
        group_by_names=["entity_1__country"],
    ).query_spec

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result_0",
        obj=query_spec,
    )


def test_non_resolvable_ambiguous_entity_path_due_to_multiple_matches(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    multi_hop_query_parser: MetricFlowQueryParser,
) -> None:
    """Tests an input with an ambiguous entity-path that can't be resolved due to multiple matches.

    'entity_0__country' matches ['entity_1__entity_0__country', 'entity_2__entity_0__country']
    """
    with pytest.raises(InvalidQueryException) as e:
        multi_hop_query_parser.parse_and_validate_query(
            metric_names=["entity_1_and_entity_2_metric"],
            group_by_names=["entity_0__country"],
        ).query_spec

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )


def test_non_resolvable_ambiguous_entity_path_due_to_mismatch(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    multi_hop_query_parser: MetricFlowQueryParser,
) -> None:
    """Tests an input with an ambiguous entity-path that can't be resolved due to a mismatch between metrics.

    'entity_0__country' matches ['entity_1__entity_0__country', 'entity_0__country']
    """
    with pytest.raises(InvalidQueryException) as e:
        multi_hop_query_parser.parse_and_validate_query(
            metric_names=["entity_0_metric", "entity_1_metric"],
            group_by_names=["entity_0__country"],
        ).query_spec

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )
