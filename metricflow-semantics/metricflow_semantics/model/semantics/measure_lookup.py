from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Sequence, Tuple

from dbt_semantic_interfaces.protocols import Measure, SemanticModel
from dbt_semantic_interfaces.references import (
    EntityReference,
    MeasureReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantics.semantic_model_helper import SemanticModelHelper
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


@dataclass(frozen=True)
class MeasureRelationshipPropertySet:
    """Properties of a measure that include how it relates to other elements in the semantic model."""

    model_reference: SemanticModelReference
    model_primary_entity: EntityReference

    # This is the time dimension along which the measure will be aggregated when a metric built on this measure
    # is queried with metric_time.
    agg_time_dimension_reference: TimeDimensionReference
    # This is the grain of the above dimension in the semantic model.
    agg_time_granularity: TimeGranularity
    # Specs that can be used to query the aggregation time dimension.
    agg_time_dimension_specs: Tuple[TimeDimensionSpec, ...]


class MeasureLookup:
    """Looks up properties related to measures.

    The functionality of this method was split off from `SemanticModelLookup`, and there are additional items to
    migrate.
    """

    def __init__(  # noqa: D107
        self,
        semantic_models: Sequence[SemanticModel],
        custom_granularities: Mapping[str, ExpandedTimeGranularity],
    ) -> None:
        self._measure_reference_to_property_set: Dict[MeasureReference, MeasureRelationshipPropertySet] = {}
        self._measure_reference_to_measure: Dict[MeasureReference, Measure] = {}
        for semantic_model in semantic_models:
            semantic_model_reference = semantic_model.reference

            primary_entity = SemanticModelHelper.resolved_primary_entity(semantic_model)
            time_dimension_reference_to_grain = SemanticModelHelper.get_time_dimension_grains(semantic_model)

            for measure in semantic_model.measures:
                measure_reference = measure.reference
                self._measure_reference_to_measure[measure_reference] = measure

                agg_time_dimension_reference = semantic_model.checked_agg_time_dimension_for_measure(measure_reference)
                agg_time_granularity = time_dimension_reference_to_grain.get(agg_time_dimension_reference)
                if agg_time_granularity is None:
                    raise ValueError(
                        f"Could not find the defined grain of the aggregation time dimension for {measure=}"
                    )
                self._measure_reference_to_property_set[measure.reference] = MeasureRelationshipPropertySet(
                    model_reference=semantic_model_reference,
                    model_primary_entity=primary_entity,
                    agg_time_dimension_reference=semantic_model.checked_agg_time_dimension_for_measure(
                        measure_reference
                    ),
                    agg_time_granularity=agg_time_granularity,
                    agg_time_dimension_specs=tuple(
                        TimeDimensionSpec.generate_possible_specs_for_time_dimension(
                            time_dimension_reference=agg_time_dimension_reference,
                            entity_links=(primary_entity,),
                            custom_granularities=custom_granularities,
                        )
                    ),
                )

    def get_properties(self, measure_reference: MeasureReference) -> MeasureRelationshipPropertySet:
        """Return properties of the measure as it relates to other elements in the semantic model."""
        property_set = self._measure_reference_to_property_set.get(measure_reference)
        if property_set is None:
            raise ValueError(
                str(
                    LazyFormat(
                        "Unable to get properties as the given measure reference is unknown",
                        measure_reference=measure_reference,
                        known_measures=list(self._measure_reference_to_property_set.keys()),
                    )
                )
            )

        return property_set

    def get_measure(self, measure_reference: MeasureReference) -> Measure:
        """Return the measure object with the given reference."""
        measure = self._measure_reference_to_measure.get(measure_reference)
        if measure is None:
            raise ValueError(
                str(
                    LazyFormat(
                        "Unable to get the measure as the given reference is unknown",
                        measure_reference=measure_reference,
                        known_measures=self._measure_reference_to_property_set.keys(),
                    )
                )
            )

        return measure
