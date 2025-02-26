from __future__ import annotations

from unittest.mock import MagicMock, patch

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import EntityReference, LinkableElementReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
)
from metricflow_semantics.specs.column_assoc import ColumnAssociation, ColumnAssociationResolver
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.rendered_spec_tracker import RenderedSpecTracker
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.specs.where_filter.where_filter_metric import WhereFilterMetric
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


class TestWhereFilterMetric:
    """Tests for the WhereFilterMetric class."""

    def test_explicit_metric_time_inheritance(self) -> None:
        """Test that WhereFilterMetric correctly inherits time granularity when metric_time is EXPLICITLY in group_by."""
        # Create mocks
        column_association_resolver = MagicMock(spec=ColumnAssociationResolver)
        resolved_spec_lookup = MagicMock(spec=FilterSpecResolutionLookUp)
        where_filter_location = MagicMock(spec=WhereFilterLocation)
        rendered_spec_tracker = MagicMock(spec=RenderedSpecTracker)

        # Create a GroupByMetricSpec with metric_time in group_by
        group_by_metric_spec = GroupByMetricSpec(
            element_name="bookings",
            entity_links=(),
            metric_subquery_entity_links=(EntityReference("account_id"),),
        )

        # Create a TimeDimensionSpec for the parent query
        parent_time_dimension_spec = TimeDimensionSpec(
            element_name="ds",
            entity_links=(),
            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
        )

        # Create a mock for the with_time_granularity method
        with_time_granularity_result = GroupByMetricSpec(
            element_name="bookings__month",
            entity_links=(),
            metric_subquery_entity_links=(EntityReference("account_id"),),
        )

        # Set up the mocks to return the appropriate values
        column_association_resolver.resolve_spec.return_value = ColumnAssociation(column_name="bookings__month_column")

        resolved_spec_lookup.checked_resolved_spec.return_value = group_by_metric_spec
        resolved_spec_lookup.checked_resolved_linkable_elements.return_value = []

        # Create the WhereFilterMetric with metric_time EXPLICITLY in group_by
        # This simulates the case where the YML definition includes 'metric_time' in the filter's group_by list
        metric = WhereFilterMetric(
            column_association_resolver=column_association_resolver,
            resolved_spec_lookup=resolved_spec_lookup,
            where_filter_location=where_filter_location,
            rendered_spec_tracker=rendered_spec_tracker,
            element_name="bookings",
            group_by=[
                LinkableElementReference(METRIC_TIME_ELEMENT_NAME),
                LinkableElementReference("account_id"),
            ],
            parent_time_dimension_spec=parent_time_dimension_spec,
        )

        # Mock the with_time_granularity method
        with patch.object(GroupByMetricSpec, "with_time_granularity", return_value=with_time_granularity_result):
            # Call __str__ to trigger the logic
            str(metric)

            # Verify that column_association_resolver.resolve_spec was called with the result of with_time_granularity
            # This confirms that time granularity inheritance was applied
            column_association_resolver.resolve_spec.assert_called_with(with_time_granularity_result)

    def test_no_metric_time_inheritance(self) -> None:
        """Test that WhereFilterMetric doesn't inherit time granularity when metric_time is not in group_by."""
        # Create mocks
        column_association_resolver = MagicMock(spec=ColumnAssociationResolver)
        resolved_spec_lookup = MagicMock(spec=FilterSpecResolutionLookUp)
        where_filter_location = MagicMock(spec=WhereFilterLocation)
        rendered_spec_tracker = MagicMock(spec=RenderedSpecTracker)

        # Create a GroupByMetricSpec without metric_time in group_by
        group_by_metric_spec = GroupByMetricSpec(
            element_name="bookings",
            entity_links=(),
            metric_subquery_entity_links=(EntityReference("account_id"),),
        )

        # Create a TimeDimensionSpec for the parent query
        parent_time_dimension_spec = TimeDimensionSpec(
            element_name="ds",
            entity_links=(),
            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
        )

        # Set up the mocks to return the appropriate values
        column_association_resolver.resolve_spec.return_value = ColumnAssociation(column_name="bookings_column")

        resolved_spec_lookup.checked_resolved_spec.return_value = group_by_metric_spec
        resolved_spec_lookup.checked_resolved_linkable_elements.return_value = []

        # Create the WhereFilterMetric without metric_time in group_by
        # This simulates the case where the YML definition does not include 'metric_time' in the filter's group_by list
        metric = WhereFilterMetric(
            column_association_resolver=column_association_resolver,
            resolved_spec_lookup=resolved_spec_lookup,
            where_filter_location=where_filter_location,
            rendered_spec_tracker=rendered_spec_tracker,
            element_name="bookings",
            group_by=[
                LinkableElementReference("account_id"),
            ],
            parent_time_dimension_spec=parent_time_dimension_spec,
        )

        # Create a mock for the with_time_granularity method
        with_time_granularity_result = GroupByMetricSpec(
            element_name="bookings__month",
            entity_links=(),
            metric_subquery_entity_links=(EntityReference("account_id"),),
        )

        # Call __str__ to trigger the logic
        with patch.object(GroupByMetricSpec, "with_time_granularity", return_value=with_time_granularity_result):
            str(metric)

            # Verify that column_association_resolver.resolve_spec was called with the original spec
            # This confirms that time granularity inheritance was NOT applied
            column_association_resolver.resolve_spec.assert_called_with(group_by_metric_spec)

    def test_metric_time_without_parent_time_dimension(self) -> None:
        """Test that WhereFilterMetric doesn't inherit time granularity when parent query doesn't have a time dimension."""
        # Create mocks
        column_association_resolver = MagicMock(spec=ColumnAssociationResolver)
        resolved_spec_lookup = MagicMock(spec=FilterSpecResolutionLookUp)
        where_filter_location = MagicMock(spec=WhereFilterLocation)
        rendered_spec_tracker = MagicMock(spec=RenderedSpecTracker)

        # Create a GroupByMetricSpec with metric_time in group_by
        group_by_metric_spec = GroupByMetricSpec(
            element_name="bookings",
            entity_links=(),
            metric_subquery_entity_links=(EntityReference("account_id"),),
        )

        # Set up the mocks to return the appropriate values
        column_association_resolver.resolve_spec.return_value = ColumnAssociation(column_name="bookings_column")

        resolved_spec_lookup.checked_resolved_spec.return_value = group_by_metric_spec
        resolved_spec_lookup.checked_resolved_linkable_elements.return_value = []

        # Create the WhereFilterMetric with metric_time in group_by but no parent time dimension
        # This simulates the case where the YML definition includes 'metric_time' in the filter's group_by list
        # but the parent query doesn't have a time dimension
        metric = WhereFilterMetric(
            column_association_resolver=column_association_resolver,
            resolved_spec_lookup=resolved_spec_lookup,
            where_filter_location=where_filter_location,
            rendered_spec_tracker=rendered_spec_tracker,
            element_name="bookings",
            group_by=[
                LinkableElementReference(METRIC_TIME_ELEMENT_NAME),
                LinkableElementReference("account_id"),
            ],
            parent_time_dimension_spec=None,  # No parent time dimension
        )

        # Create a mock for the with_time_granularity method
        with_time_granularity_result = GroupByMetricSpec(
            element_name="bookings__month",
            entity_links=(),
            metric_subquery_entity_links=(EntityReference("account_id"),),
        )

        # Call __str__ to trigger the logic
        with patch.object(GroupByMetricSpec, "with_time_granularity", return_value=with_time_granularity_result):
            str(metric)

            # Verify that column_association_resolver.resolve_spec was called with the original spec
            # This confirms that time granularity inheritance was NOT applied
            column_association_resolver.resolve_spec.assert_called_with(group_by_metric_spec)

    def test_case_insensitivity(self) -> None:
        """Test that WhereFilterMetric correctly handles case insensitivity for metric_time."""
        # Create mocks
        column_association_resolver = MagicMock(spec=ColumnAssociationResolver)
        resolved_spec_lookup = MagicMock(spec=FilterSpecResolutionLookUp)
        where_filter_location = MagicMock(spec=WhereFilterLocation)
        rendered_spec_tracker = MagicMock(spec=RenderedSpecTracker)

        # Create a GroupByMetricSpec with metric_time in group_by
        group_by_metric_spec = GroupByMetricSpec(
            element_name="bookings",
            entity_links=(),
            metric_subquery_entity_links=(EntityReference("account_id"),),
        )

        # Create a TimeDimensionSpec for the parent query
        parent_time_dimension_spec = TimeDimensionSpec(
            element_name="ds",
            entity_links=(),
            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
        )

        # Create a mock for the with_time_granularity method
        with_time_granularity_result = GroupByMetricSpec(
            element_name="bookings__month",
            entity_links=(),
            metric_subquery_entity_links=(EntityReference("account_id"),),
        )

        # Set up the mocks to return the appropriate values
        column_association_resolver.resolve_spec.return_value = ColumnAssociation(column_name="bookings__month_column")

        resolved_spec_lookup.checked_resolved_spec.return_value = group_by_metric_spec
        resolved_spec_lookup.checked_resolved_linkable_elements.return_value = []

        # Create the WhereFilterMetric with METRIC_TIME (uppercase) in group_by
        # This simulates the case where the YML definition includes 'METRIC_TIME' in the filter's group_by list
        metric = WhereFilterMetric(
            column_association_resolver=column_association_resolver,
            resolved_spec_lookup=resolved_spec_lookup,
            where_filter_location=where_filter_location,
            rendered_spec_tracker=rendered_spec_tracker,
            element_name="bookings",
            group_by=[
                LinkableElementReference("METRIC_TIME"),  # Uppercase
                LinkableElementReference("account_id"),
            ],
            parent_time_dimension_spec=parent_time_dimension_spec,
        )

        # Mock the with_time_granularity method
        with patch.object(GroupByMetricSpec, "with_time_granularity", return_value=with_time_granularity_result):
            # Call __str__ to trigger the logic
            str(metric)

            # Verify that column_association_resolver.resolve_spec was called with the result of with_time_granularity
            # This confirms that time granularity inheritance was applied despite the case difference
            column_association_resolver.resolve_spec.assert_called_with(with_time_granularity_result)
