from __future__ import annotations

from tests_metricflow.performance.synthetic_manifest_parameter_set import SyntheticManifestParameterSet


class MeasureGenerator:
    """Helps generate the measures in the semantic manifest.

    The index for the measure refers to the index when measures in the semantic manifest are enumerated.
    """

    def __init__(self, parameter_set: SyntheticManifestParameterSet) -> None:  # noqa: D107
        self._parameter_set = parameter_set

    def get_measure_name(self, measure_index: int) -> str:  # noqa: D102
        return f"measure_{measure_index:03}"

    @property
    def unique_measure_count(self) -> int:  # noqa: D102
        return self._parameter_set.measures_per_semantic_model * self._parameter_set.measure_semantic_model_count

    def get_next_wrapped_index(self, measure_index: int) -> int:
        """Return the next valid measure index, wrapping back to 0 if it reaches the last index."""
        if measure_index < 0:
            raise ValueError(f"{measure_index=} should be > 0")

        if measure_index >= self.unique_measure_count:
            raise ValueError(f"{measure_index=} should be < {self.unique_measure_count}")

        return (measure_index + 1) % self.unique_measure_count
