from metricflow.api.simple_api import MetricFlowSimpleAPI
from metricflow.dataflow.sql_table import SqlTable
from metricflow.engine.models import Dimension, Materialization, Metric
from metricflow.object_utils import random_id


def test_query(mf_simple_api: MetricFlowSimpleAPI) -> None:  # noqa: D
    result = mf_simple_api.query(
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

    output_table = SqlTable(schema_name=mf_simple_api.system_schema, table_name=f"test_table_{random_id()}")
    result = mf_simple_api.query(
        ["bookings"], ["ds"], limit=2, start_time="2019-01-01", end_time="2024-01-01", as_table=output_table.sql
    )
    assert result.query_spec
    assert result.dataflow_plan
    assert result.sql
    assert result.result_df is None
    assert result.result_table == output_table


def test_explain(mf_simple_api: MetricFlowSimpleAPI) -> None:  # noqa: D
    result = mf_simple_api.explain(
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

    output_table = SqlTable(schema_name=mf_simple_api.system_schema, table_name=f"test_table_{random_id()}")
    result = mf_simple_api.explain(
        ["bookings"], ["ds"], limit=2, start_time="2019-01-01", end_time="2024-01-01", as_table=output_table.sql
    )
    assert result.query_spec
    assert result.dataflow_plan
    assert result.execution_plan
    assert result.output_table == output_table


def test_list_metrics(mf_simple_api: MetricFlowSimpleAPI) -> None:  # noqa: D
    metrics = mf_simple_api.list_metrics()
    assert metrics

    metric_name, metric_obj = next(iter(metrics.items()))

    assert metric_name == metric_obj.name
    assert isinstance(metric_obj, Metric)

    assert metric_obj.dimensions
    assert isinstance(metric_obj.dimensions[0], Dimension)


def test_list_dimensions(mf_simple_api: MetricFlowSimpleAPI) -> None:  # noqa: D
    dimensions = mf_simple_api.list_dimensions(["bookings"])

    assert dimensions
    assert isinstance(dimensions[0], Dimension)

    dimensions = mf_simple_api.list_dimensions(["bookings", "revenue"])
    assert len(dimensions) == 1
    assert dimensions[0].name == "ds"


def test_list_materializations(mf_simple_api: MetricFlowSimpleAPI) -> None:  # noqa: D
    materializations = mf_simple_api.list_materializations()
    assert materializations
    assert isinstance(materializations[0], Materialization)


def test_get_dimension_values(mf_simple_api: MetricFlowSimpleAPI) -> None:  # noqa: D
    dim_vals = mf_simple_api.get_dimension_values("bookings", "ds", start_time="2020-01-01", end_time="2024-01-01")
    assert dim_vals


def test_materializations(mf_simple_api: MetricFlowSimpleAPI) -> None:  # noqa: D
    mat_name = "test_materialization"
    output_table = mf_simple_api.materialize(
        materialization_name=mat_name, start_time="2020-01-01", end_time="2024-01-01"
    )
    assert isinstance(output_table, SqlTable)
    assert output_table.sql == f"{mf_simple_api.system_schema}.{mat_name}"

    dropped = mf_simple_api.drop_materialization(mat_name)
    assert dropped


def test_validate_configs(mf_simple_api: MetricFlowSimpleAPI) -> None:  # noqa: D
    issues = mf_simple_api.validate_configs()
    assert isinstance(issues, list)
