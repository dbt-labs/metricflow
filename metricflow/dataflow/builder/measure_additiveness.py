import collections

from dataclasses import dataclass
from typing import Sequence, Tuple

from metricflow.specs import MeasureSpec


@dataclass(frozen=True)
class GroupedMeasureSpecsByAdditiveness:
    """Results after grouping measures by their additive properties"""

    grouped_semi_additive_measures: Sequence[Tuple[MeasureSpec, ...]]
    additive_measures: Tuple[MeasureSpec, ...]


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
