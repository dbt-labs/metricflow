from typing import Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_transformer import DbtTransformer


def test_dbt_transformer_transform_and_build(dbt_metrics: Tuple[MetricNode, ...]) -> None:  # noqa: D
    dbt_build_result = DbtTransformer().transform_and_build(dbt_metrics=dbt_metrics)
    assert (
        not dbt_build_result.issues.has_blocking_issues
    ), f"Unexpected issues found when buidling UserConfiguredModel from dbt metadata API metrics: {dbt_build_result.issues.to_pretty_json()}"
    assert not (
        len(dbt_build_result.model.metrics) > len(dbt_metrics)
    ), f"Created more metrics ({len(dbt_build_result.model.metrics)}) than there were dbt metrics ({len(dbt_metrics)}), possible duplication. "
    assert not (
        len(dbt_build_result.model.metrics) < len(dbt_metrics)
    ), f"Created fewer metrics ({len(dbt_build_result.model.metrics)}) than there were dbt metrics ({len(dbt_metrics)}), possible skipped metrics"
