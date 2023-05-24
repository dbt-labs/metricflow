from __future__ import annotations

from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType

from metricflow.dataflow.builder.measure_additiveness import group_measure_specs_by_additiveness
from metricflow.specs.specs import MeasureSpec, NonAdditiveDimensionSpec


def test_bucket_measure_specs_by_additiveness() -> None:  # noqa: D
    # Semi-additive Bucket 1
    measure_1 = MeasureSpec(
        element_name="measure_1",
        non_additive_dimension_spec=NonAdditiveDimensionSpec(
            name="ds",
            window_choice=AggregationType.MIN,
        ),
    )

    # Semi-additive Bucket 2
    measure_2 = MeasureSpec(
        element_name="measure_2",
        non_additive_dimension_spec=NonAdditiveDimensionSpec(
            name="ds",
            window_choice=AggregationType.MIN,
            window_groupings=("id_1", "id_2"),
        ),
    )
    measure_3 = MeasureSpec(
        element_name="measure_3",
        non_additive_dimension_spec=NonAdditiveDimensionSpec(
            name="ds",
            window_choice=AggregationType.MIN,
            window_groupings=("id_2", "id_1"),
        ),
    )

    # Additive Bucket
    measure_4 = MeasureSpec(element_name="measure_4")

    result = group_measure_specs_by_additiveness((measure_1, measure_2, measure_3, measure_4))
    assert result.additive_measures == (measure_4,)
    assert (measure_1,) in result.grouped_semi_additive_measures
    assert (measure_2, measure_3) in result.grouped_semi_additive_measures
