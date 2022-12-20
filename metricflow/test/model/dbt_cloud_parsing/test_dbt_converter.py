from typing import Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_converter import DbtConverter
from metricflow.model.objects.metric import MetricType


def test_for_breaking_model_changes(dbt_metrics: Tuple[MetricNode, ...]) -> None:
    """This is intended to be a comprehensive test such that if the MetricFlow model changes in a breaking way, this should break

    This test is fairly brittle. That is intentional. The dbt mapping rules map dbt metric
    and model attributes to MetricFlow Model Element attributes. If the MetricFlow model
    renames attributes, adds a new required attribute, or some other breaking change we
    need to be aware so we appropriate update/create dbt mapping rules accordingly. This
    test detects when that is needed.
    """

    dbt_convert_result = DbtConverter().convert(dbt_metrics=dbt_metrics)
    assert (
        not dbt_convert_result.issues.has_blocking_issues
    ), f"Unexpected issues found when buidling UserConfiguredModel from dbt metadata API metrics: {dbt_convert_result.issues.to_pretty_json()}"
    assert not (
        len(dbt_convert_result.model.metrics) > len(dbt_metrics)
    ), f"Created more metrics ({len(dbt_convert_result.model.metrics)}) than there were dbt metrics ({len(dbt_metrics)}), possible duplication. "
    assert not (
        len(dbt_convert_result.model.metrics) < len(dbt_metrics)
    ), f"Created fewer metrics ({len(dbt_convert_result.model.metrics)}) than there were dbt metrics ({len(dbt_metrics)}), possible skipped metrics"

    # Note: The following is *somewhat* redudant because issues *should* be raised by parsing and semantic validations.
    for dbt_metric in dbt_metrics:
        # Ensure metric created properly
        mf_metric = next(
            (mf_metric for mf_metric in dbt_convert_result.model.metrics if mf_metric.name == dbt_metric.name), None
        )
        assert mf_metric is not None, f"Failed to create metric `{dbt_metric.name}` from dbt metric `{dbt_metric.name}`"
        assert (
            mf_metric.type is not None
        ), f"Metric `{mf_metric.name}` created from dbt metric `{dbt_metric.name}` is missing a `type`"
        assert mf_metric.type in [
            MetricType.MEASURE_PROXY,
            MetricType.DERIVED,
        ], f"Metric `{mf_metric.name}` created from dbt metric `{dbt_metric.name}` is a type other than `MEASURE_PROXY` or `DERIVED`"
        assert (
            mf_metric.type_params is not None
        ), f"Metric `{mf_metric.name}` created from dbt metric `{dbt_metric.name}` is missing `type_params`"
        if dbt_metric.description is not None:
            assert (
                mf_metric.description is not None
            ), f"Metric `{mf_metric.name}` created from dbt metric `{dbt_metric.name}` is missing a `description`"
        if dbt_metric.filters is not None:
            assert (
                mf_metric.constraint is not None
            ), f"Metric `{mf_metric.name}` created from dbt metric `{dbt_metric.name}` is missing a `constraint`"

        # Ensure data source created from metric's model (if it has one) was created properly
        if dbt_metric.model is not None and hasattr(dbt_metric.model, "name"):
            mf_data_source = next(
                (mf_ds for mf_ds in dbt_convert_result.model.data_sources if mf_ds.name == dbt_metric.model.name), None
            )
            assert (
                mf_data_source is not None
            ), f"Failed to create data source `{dbt_metric.model.name}` from dbt metric `{dbt_metric.name}`"
            assert (
                mf_data_source.sql_table is not None
            ), f"Data source `{mf_data_source.name}` created from dbt metric `{dbt_metric.name}` is missing an `sql_table`"
            if dbt_metric.model.description is not None:
                assert (
                    mf_data_source.description is not None
                ), f"Data source `{mf_data_source.name}` created from dbt metric `{dbt_metric.name}` is missing a `description`"

            # Ensure dimensions were created
            if dbt_metric.dimensions is not None or dbt_metric.timestamp is not None or dbt_metric.filters is not None:
                assert (
                    mf_data_source.dimensions is not None and len(mf_data_source.dimensions) > 0
                ), f"Expected dimensions to be created on data source `{mf_data_source.name}` from dbt metric `{dbt_metric.name}`, but none were"

            # Ensure measure created correctly
            mf_measure = next(
                (mf_measure for mf_measure in mf_data_source.measures if mf_measure.name == dbt_metric.name), None
            )
            assert (
                mf_measure is not None
            ), f"Expected a measure `{dbt_metric.name}` to exist on data source `{mf_data_source.name}` from dbt metric `{dbt_metric.name}`"
            assert (
                mf_measure.agg_time_dimension is not None
            ), f"Measure `{mf_measure.name}` created from dbt metric `{dbt_metric.name}` is missing an `agg_time_dimension`"
            assert (
                mf_measure.agg is not None
            ), f"Measure `{mf_measure.name}` created from dbt metric `{dbt_metric.name}` is missing an `agg`"
            assert (
                mf_measure.expr is not None
            ), f"Measure `{mf_measure.name}` created from dbt metric `{dbt_metric.name}` is missing an `expr`"
