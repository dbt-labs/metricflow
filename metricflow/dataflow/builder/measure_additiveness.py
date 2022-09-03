import collections

from dataclasses import dataclass
from typing import Sequence, Tuple

from metricflow.specs import MeasureSpec


@dataclass(frozen=True)
class GroupedMeasureSpecsByAdditiveness:
    """Results after grouping measures by their additive properties"""

    grouped_semi_additive_measures: Sequence[Tuple[MeasureSpec, ...]]
    additive_measures: Tuple[MeasureSpec, ...]

    @property
    def measures_by_additiveness(self) -> Tuple[Tuple[MeasureSpec, ...], ...]:
        """Returns a single tuple of tuples of MeasureSpecs grouped by additiveness

        The additive measures are identifiable by the nature of their NonAdditiveDimensionSpec property.
        """
        if self.additive_measures:
            return (self.additive_measures,) + tuple(self.grouped_semi_additive_measures)

        return tuple(self.grouped_semi_additive_measures)


def group_measure_specs_by_additiveness(measure_specs: Sequence[MeasureSpec]) -> GroupedMeasureSpecsByAdditiveness:
    """Bucket the provided measure specs by

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
