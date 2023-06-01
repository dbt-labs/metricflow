"""Helper classes to convert model objects to spec object representations.

In some cases we need to take a model object and convert it to a spec for use in a DataflowPlan or similar.
This fundamentally requires us to combine a model object with a spec object and, in some cases, follow semantic
metadata about linked specs in order to resolve dimension names and the like.

Since model objects and specs should not depend on each other, we do these conversions separately via external
classes, rather than the perhaps more natural approach of adding a to_spec() method on the model objects. These
shims likely point to the need for a bit of an internal refactor, but that's a concern for another time.
"""
from __future__ import annotations

from dbt_semantic_interfaces.protocols.measure import Measure

from metricflow.specs.specs import (
    MeasureSpec,
    NonAdditiveDimensionSpec,
)


class MeasureConverter:
    """Static class for converting Measure model objects to MeasureSpec instances."""

    @staticmethod
    def convert_to_measure_spec(measure: Measure) -> MeasureSpec:
        """Converts a Measure to a MeasureSpec, and properly handles non-additive dimension properties."""
        non_additive_dimension_spec = (
            NonAdditiveDimensionSpec(
                name=measure.non_additive_dimension.name,
                window_choice=measure.non_additive_dimension.window_choice,
                window_groupings=tuple(measure.non_additive_dimension.window_groupings),
            )
            if measure.non_additive_dimension is not None
            else None
        )

        return MeasureSpec(
            element_name=measure.name,
            non_additive_dimension_spec=non_additive_dimension_spec,
        )
