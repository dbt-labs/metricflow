from __future__ import annotations

from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


def test_with_time_granularity() -> None:
    """Test that with_time_granularity correctly applies time granularity to the spec."""
    # Create a basic GroupByMetricSpec
    spec = GroupByMetricSpec(
        element_name="bookings",
        entity_links=(),
        metric_subquery_entity_links=(EntityReference("account_id"),),
    )

    # Apply time granularity
    day_spec = spec.with_time_granularity(TimeGranularity.DAY)
    month_spec = spec.with_time_granularity(TimeGranularity.MONTH)
    
    # Verify that the time granularity is correctly applied
    expanded_day = ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY)
    expanded_month = ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH)
    
    assert day_spec.element_name == f"bookings__{expanded_day.name}"
    assert month_spec.element_name == f"bookings__{expanded_month.name}"
    
    # Verify that other properties remain unchanged
    assert day_spec.entity_links == spec.entity_links
    assert day_spec.metric_subquery_entity_links == spec.metric_subquery_entity_links
    
    # Test with None time granularity
    none_spec = spec.with_time_granularity(None)
    assert none_spec == spec
