from __future__ import annotations

from metricflow.specs.specs import InstanceSpecSet, InstanceSpecSetTransform


class MatchingLinkableSpecsTransform(InstanceSpecSetTransform[bool]):
    """Returns true if two spec sets have the same set of linkable specs."""

    def __init__(self, left_spec_set: InstanceSpecSet) -> None:  # noqa: D
        self._left_spec_set = left_spec_set

    def transform(self, spec_set: InstanceSpecSet) -> bool:  # noqa: D
        return (
            set(self._left_spec_set.dimension_specs) == set(spec_set.dimension_specs)
            and set(self._left_spec_set.time_dimension_specs) == set(spec_set.time_dimension_specs)
            and set(self._left_spec_set.entity_specs) == set(spec_set.entity_specs)
        )
