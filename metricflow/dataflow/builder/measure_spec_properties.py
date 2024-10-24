from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from dbt_semantic_interfaces.references import SemanticModelElementReference
from metricflow_semantics.specs.measure_spec import MeasureSpec
from metricflow_semantics.specs.non_additive_dimension_spec import NonAdditiveDimensionSpec


@dataclass(frozen=True)
class MeasureSpecProperties:
    """Input dataclass for grouping properties of a sequence of MeasureSpecs."""

    measure_specs: Sequence[MeasureSpec]
    agg_time_dimension: SemanticModelElementReference
    non_additive_dimension_spec: Optional[NonAdditiveDimensionSpec] = None
