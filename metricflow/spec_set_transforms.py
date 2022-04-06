from typing import Set

from metricflow.specs import InstanceSpecSetTransform, InstanceSpecSet


class ToElementNameSet(InstanceSpecSetTransform[Set[str]]):
    """Gets all element names for all specs in the set."""

    def transform(self, spec_set: InstanceSpecSet) -> Set[str]:  # noqa: D
        return (
            {x.element_name for x in spec_set.metric_specs}
            .union({x.element_name for x in spec_set.measure_specs})
            .union({x.element_name for x in spec_set.dimension_specs})
            .union({x.element_name for x in spec_set.time_dimension_specs})
            .union({x.element_name for x in spec_set.identifier_specs})
        )
