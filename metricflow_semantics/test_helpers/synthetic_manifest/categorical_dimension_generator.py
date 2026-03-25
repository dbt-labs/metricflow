from __future__ import annotations

from functools import cached_property

from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)


class CategoricalDimensionGenerator:
    """Helps generate the categorical dimensions in the semantic manifest.

    The index for the dimension refers to the index when all unique dimensions in the semantic manifest are enumerated.
    """

    def __init__(self, parameter_set: SyntheticManifestParameterSet) -> None:  # noqa: D107
        self._parameter_set = parameter_set

    def get_dimension_name(self, dimension_index: int) -> str:  # noqa: D102
        """Return the name of the dimension for the given index."""
        return f"dimension_{dimension_index:03}"

    @cached_property
    def unique_dimension_count(self) -> int:  # noqa: D102
        return (
            self._parameter_set.categorical_dimensions_per_semantic_model
            * self._parameter_set.dimension_semantic_model_count
        )

    def get_next_wrapped_index(self, dimension_index: int) -> int:
        """Return the next valid dimension index, wrapping back to 0 if it reaches the last index."""
        if dimension_index < 0:
            raise ValueError(f"{dimension_index=} should be > 0")

        if dimension_index >= self.unique_dimension_count:
            raise ValueError(f"{dimension_index=} should be < {self.unique_dimension_count}")

        return (dimension_index + 1) % self.unique_dimension_count
