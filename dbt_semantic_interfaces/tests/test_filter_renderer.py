import logging

from dbt_semantic_interfaces.objects.where_filter.filter_renderer import (
    FilterRenderer,
    FilterCallParameterSets,
    DimensionCallParameterSet,
    EntityCallParameterSet,
    TimeDimensionCallParameterSet,
    FilterFunctionCallRenderer,
)
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity
from dbt_semantic_interfaces.references import DimensionReference, EntityReference

logger = logging.getLogger(__name__)


def test_extract_dimension_call_parameter_sets() -> None:  # noqa: D
    parse_result = FilterRenderer.extract_parameter_sets(
        """{{ dimension('is_instant') }} AND {{ dimension('country', entity_path=['listing']) }} == 'US'"""
    )

    assert parse_result == FilterCallParameterSets(
        dimension_call_parameter_sets=(
            DimensionCallParameterSet(
                dimension_reference=DimensionReference(element_name="is_instant"),
                entity_path=(),
            ),
            DimensionCallParameterSet(
                entity_path=(EntityReference(element_name="listing"),),
                dimension_reference=DimensionReference(element_name="country"),
            ),
        ),
        entity_call_parameter_sets=(),
    )


def test_extract_time_dimension_call_parameter_sets() -> None:  # noqa: D
    parse_result = FilterRenderer.extract_parameter_sets(
        """{{ time_dimension('created_at', 'month', entity_path=['listing']) }} = '2020-01-01'"""
    )

    assert parse_result == FilterCallParameterSets(
        time_dimension_call_parameter_sets=(
            TimeDimensionCallParameterSet(
                time_dimension_reference=DimensionReference(element_name="created_at"),
                entity_path=(EntityReference(element_name="listing"),),
                time_granularity=TimeGranularity.MONTH,
            ),
        )
    )


def test_extract_entity_call_parameter_sets() -> None:  # noqa: D
    parse_result = FilterRenderer.extract_parameter_sets(
        """{{ entity('listing') }} AND {{ entity('user', entity_path=['listing']) }} == 'TEST_USER_ID'"""
    )

    assert parse_result == FilterCallParameterSets(
        dimension_call_parameter_sets=(),
        entity_call_parameter_sets=(
            EntityCallParameterSet(
                entity_path=(),
                entity_reference=EntityReference("listing"),
            ),
            EntityCallParameterSet(
                entity_path=(EntityReference("listing"),),
                entity_reference=EntityReference("user"),
            ),
        ),
    )


def test_render() -> None:  # noqa: D
    class _TestFilterFunctionCallRenderer(FilterFunctionCallRenderer):  # noqa: D
        def render_dimension_call(self, dimension_call_parameter_set: DimensionCallParameterSet) -> str:  # noqa: D
            if dimension_call_parameter_set == DimensionCallParameterSet(
                dimension_reference=DimensionReference(element_name="is_instant"),
                entity_path=(),
            ):
                return "dimension0"
            return "invalid"

        def render_time_dimension_call(  # noqa: D
            self, time_dimension_call_parameter_set: TimeDimensionCallParameterSet
        ) -> str:
            if time_dimension_call_parameter_set == TimeDimensionCallParameterSet(
                time_dimension_reference=DimensionReference(element_name="created_at"),
                entity_path=(),
                time_granularity=TimeGranularity.MONTH,
            ):
                return "time_dimension0"
            return "invalid"

        def render_entity_call(self, entity_call_parameter_set: EntityCallParameterSet) -> str:  # noqa: D
            if entity_call_parameter_set == EntityCallParameterSet(
                entity_path=(),
                entity_reference=EntityReference("listing"),
            ):
                return "entity0"
            return "invalid"

    assert (
        FilterRenderer.render(
            templated_filter_sql="{{ dimension('is_instant') }} "
            "AND {{ time_dimension('created_at', 'month') }} "
            "AND {{ entity('listing') }}",
            call_renderer=_TestFilterFunctionCallRenderer(),
        )
        == "dimension0 AND time_dimension0 AND entity0"
    )
