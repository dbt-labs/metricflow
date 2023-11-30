from __future__ import annotations

import logging
from typing import Sequence

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME

from metricflow.collection_helpers.pretty_print import mf_pformat_many
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.query.group_by_item.filter_spec_resolution.ambiguous_filter_resolution_test_case_builder import (
    AmbiguousFilterResolutionTestCase,
    FilterValidity,
)
from metricflow.test.snapshot_utils import assert_object_snapshot_equal

logger = logging.getLogger(__name__)


def test_parse_query_with_ambiguous_filters(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_filter_query_cases: Sequence[AmbiguousFilterResolutionTestCase],
) -> None:
    """Checks that the query parser succeeds or fails for the various filter cases that are possible.

    If successful, snapshot the query spec.
    """
    for case_num, case in enumerate(ambiguous_filter_query_cases):
        manifest_lookup = SemanticManifestLookup(semantic_manifest=case.semantic_manifest)

        column_association_resolver = DunderColumnAssociationResolver(semantic_manifest_lookup=manifest_lookup)

        query_parser = MetricFlowQueryParser(
            column_association_resolver=column_association_resolver,
            model=manifest_lookup,
        )

        metric_names = [metric_reference.element_name for metric_reference in case.metrics_to_query]
        case_id_str = f"case_{case_num:02}"
        test_context = {
            "case_id": case_id_str,
            "metrics_to_query": metric_names,
            "filter_ambiguity_case": case.filter_ambiguity_case,
        }
        filter_validity = case.filter_ambiguity_case.filter_validity
        try:
            assert (
                len(case.query_filter.where_filters) <= 1
            ), "All test cases should have been created with at most 1 filter"
            # Invalid filters will raise an exception here.
            query_spec = query_parser.parse_and_validate_query(
                metric_names=metric_names,
                group_by_names=(METRIC_TIME_ELEMENT_NAME,),
                where_constraint=case.query_filter.where_filters[0]
                if len(case.query_filter.where_filters) == 1
                else None,
            )
            # Snapshot valid query specs. TBD: There are many snapshots, and they are time-consuming to read.
            assert_object_snapshot_equal(
                request=request,
                mf_test_session_state=mf_test_session_state,
                obj_id=case_id_str,
                obj=mf_pformat_many(
                    "Query parser test for ambiguous filter:",
                    # Merge dicts.
                    obj_dict={**test_context, "query_spec": query_spec},
                ),
            )

            if filter_validity is FilterValidity.INVALID:
                assert False, mf_pformat_many(
                    "The ambiguous filter is invalid, but did not get an error. Context:",
                    obj_dict=test_context,
                )
        except InvalidQueryException as e:
            if filter_validity is FilterValidity.VALID:
                raise AssertionError(
                    mf_pformat_many(
                        "The ambiguous filter is valid, but got an error. Context:",
                        obj_dict=test_context,
                    )
                ) from e
