from __future__ import annotations

from dbt_semantic_interfaces.validations.validator_helpers import SemanticManifestValidationResults

from metricflow.api.metricflow_client import MetricFlowClient
from metricflow.dataflow.sql_table import SqlTable
from metricflow.engine.models import Dimension, Metric
from metricflow.random_id import random_id


def test_query(mf_client: MetricFlowClient) -> None:  # noqa: D
    result = mf_client.query(
        ["bookings"],
        ["ds"],
        limit=2,
        start_time="2019-01-01",
        end_time="2024-01-01",
    )
    assert result.query_spec
    assert result.dataflow_plan
    assert result.sql
    assert result.result_df is not None
    assert len(result.result_df) == 2
    assert result.result_table is None

    output_table = SqlTable(schema_name=mf_client.system_schema, table_name=f"test_table_{random_id()}")
    result = mf_client.query(
        ["bookings"], ["ds"], limit=2, start_time="2019-01-01", end_time="2024-01-01", as_table=output_table.sql
    )
    assert result.query_spec
    assert result.dataflow_plan
    assert result.sql
    assert result.result_df is None
    assert result.result_table == output_table


def test_explain(mf_client: MetricFlowClient) -> None:  # noqa: D
    result = mf_client.explain(
        ["bookings"],
        ["ds"],
        limit=2,
        start_time="2019-01-01",
        end_time="2024-01-01",
    )
    assert result.query_spec
    assert result.dataflow_plan
    assert result.execution_plan
    assert result.output_table is None

    output_table = SqlTable(schema_name=mf_client.system_schema, table_name=f"test_table_{random_id()}")
    result = mf_client.explain(
        ["bookings"], ["ds"], limit=2, start_time="2019-01-01", end_time="2024-01-01", as_table=output_table.sql
    )
    assert result.query_spec
    assert result.dataflow_plan
    assert result.execution_plan
    assert result.output_table == output_table


def test_list_metrics(mf_client: MetricFlowClient) -> None:  # noqa: D
    metrics = mf_client.list_metrics()
    assert metrics

    metric_name, metric_obj = next(iter(metrics.items()))

    assert metric_name == metric_obj.name
    assert isinstance(metric_obj, Metric)

    assert metric_obj.dimensions
    assert isinstance(metric_obj.dimensions[0], Dimension)


def test_list_dimensions(mf_client: MetricFlowClient) -> None:  # noqa: D
    dimensions = mf_client.list_dimensions(["bookings"])

    assert dimensions
    assert isinstance(dimensions[0], Dimension)

    dimensions = mf_client.list_dimensions(["bookings", "revenue"])
    assert len(dimensions) == 2
    assert [dim.name for dim in dimensions] == ["ds", "ds"]


def test_get_dimension_values(mf_client: MetricFlowClient) -> None:  # noqa: D
    dim_vals = mf_client.get_dimension_values("bookings", "ds", start_time="2020-01-01", end_time="2024-01-01")
    assert dim_vals


def test_validate_configs(mf_client: MetricFlowClient) -> None:  # noqa: D
    issues = mf_client.validate_configs()
    assert isinstance(issues, SemanticManifestValidationResults)
