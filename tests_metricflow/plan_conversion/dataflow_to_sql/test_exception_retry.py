from __future__ import annotations

import logging
from typing import Mapping
from unittest.mock import patch

import pytest
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.mock_helpers import mf_function_patch_target

from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup

logger = logging.getLogger(__name__)


def test_retry_exception_propagation(
    caplog: pytest.LogCaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Test that exceptions are propagated by the retry handler in `DataflowToSqlPlanConverter`."""
    source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]

    # noinspection PyTypeChecker
    patch_target = mf_function_patch_target(DataflowToSqlPlanConverter.convert_using_specifics)
    with (
        pytest.raises(RuntimeError) as result,
        patch(patch_target) as method_raising_exception,
        # Test exception is going to generate error logs and won't be useful, so disable them.
        caplog.at_level(logging.FATAL),
    ):
        error_message = "Error raised from patch"
        method_raising_exception.side_effect = RuntimeError(error_message)
        dataflow_to_sql_converter.convert_to_sql_plan(
            sql_engine_type=sql_client.sql_engine_type,
            sql_query_plan_id=DagId.from_str("plan0"),
            dataflow_plan_node=source_node,
        )

    assert str(result.value) == error_message
