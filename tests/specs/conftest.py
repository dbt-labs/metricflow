from __future__ import annotations

from dbt_semantic_interfaces.references import MetricReference

from metricflow.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation

EXAMPLE_FILTER_LOCATION = WhereFilterLocation.for_metric(MetricReference("example_metric"))
