from __future__ import annotations

import collections
from dataclasses import dataclass
from typing import Dict, Optional, Sequence, Tuple

from metricflow.specs.specs import MeasureSpec, NonAdditiveDimensionSpec


@dataclass(frozen=True)
class GroupedMeasureSpecsByAdditiveness:
    """Results after grouping measures by their additive properties."""

    grouped_semi_additive_measures: Sequence[Tuple[MeasureSpec, ...]]
    additive_measures: Tuple[MeasureSpec, ...]

    @property
    def measures_by_additiveness(self) -> Dict[Optional[NonAdditiveDimensionSpec], Tuple[MeasureSpec, ...]]:
        """Returns a mapping from additiveness spec to a tuple of measure specs.

        This is useful if you wish to consume the tuples of MeasureSpecs in a single pass without having to
        divide calls up by the existence of an additiveness specification
        """
        additiveness_to_measures: Dict[Optional[NonAdditiveDimensionSpec], Tuple[MeasureSpec, ...]] = {}
        if self.additive_measures:
            additiveness_to_measures[None] = self.additive_measures

        for grouped_specs in self.grouped_semi_additive_measures:
            assert len(grouped_specs) > 0, "received empty set of measure specs, this should not happen!"
            # These all have the same additiveness spec value
            non_additive_spec = grouped_specs[0].non_additive_dimension_spec
            additiveness_to_measures[non_additive_spec] = grouped_specs

        return additiveness_to_measures


def group_measure_specs_by_additiveness(measure_specs: Sequence[MeasureSpec]) -> GroupedMeasureSpecsByAdditiveness:
    """Bucket the provided measure specs by.

    - Additive Measures
    - Semi-additive measures containing the same non-additive dimension attributes
    """
    bucket = collections.defaultdict(list)
    additive_bucket = []
    for spec in measure_specs:
        non_additive_dimension_spec = spec.non_additive_dimension_spec
        if non_additive_dimension_spec:
            bucket[non_additive_dimension_spec.bucket_hash].append(spec)
        else:
            additive_bucket.append(spec)
    return GroupedMeasureSpecsByAdditiveness(
        grouped_semi_additive_measures=tuple(tuple(measures) for measures in bucket.values()),
        additive_measures=tuple(additive_bucket),
    )
