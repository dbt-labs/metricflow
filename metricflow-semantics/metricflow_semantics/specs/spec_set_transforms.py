from __future__ import annotations

from typing import Set

from metricflow_semantics.specs.spec_set import InstanceSpecSet, InstanceSpecSetTransform


class ToElementNameSet(InstanceSpecSetTransform[Set[str]]):
    """Gets all element names for all specs in the set."""

    def transform(self, spec_set: InstanceSpecSet) -> Set[str]:  # noqa: D102
        return (
            {x.element_name for x in spec_set.metric_specs}
            .union({x.element_name for x in spec_set.measure_specs})
            .union({x.element_name for x in spec_set.dimension_specs})
            .union({x.element_name for x in spec_set.time_dimension_specs})
            .union({x.element_name for x in spec_set.entity_specs})
            .union({x.element_name for x in spec_set.metric_specs})
            .union({x.element_name for x in spec_set.group_by_metric_specs})
        )
