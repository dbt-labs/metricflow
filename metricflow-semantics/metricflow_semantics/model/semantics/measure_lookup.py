from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Dict, Mapping, Sequence, Tuple

from dbt_semantic_interfaces.protocols import Measure, SemanticModel
from dbt_semantic_interfaces.references import (
    EntityReference,
    MeasureReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import DimensionType, TimeGranularity

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantics.element_group import ElementGrouper
from metricflow_semantics.model.semantics.semantic_model_helper import SemanticModelHelper
from metricflow_semantics.model.spec_converters import MeasureConverter
from metricflow_semantics.specs.measure_spec import SimpleMetricInputSpec
from metricflow_semantics.specs.non_additive_dimension_spec import NonAdditiveDimensionSpec
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
        self._measure_non_additive_dimension_specs: Dict[MeasureReference, NonAdditiveDimensionSpec] = {}
        self._model_reference_to_measures: dict[SemanticModelReference, AnyLengthTuple[Measure]] = {}
        self._model_reference_to_aggregation_time_dimensions: dict[
            SemanticModelReference, ElementGrouper[TimeDimensionReference, SimpleMetricInputSpec]
        ] = {}

        for semantic_model in semantic_models:
            semantic_model_reference = semantic_model.reference
            self._model_reference_to_measures[semantic_model_reference] = tuple(semantic_model.measures)
            self._model_reference_to_aggregation_time_dimensions[semantic_model_reference] = ElementGrouper[
                TimeDimensionReference, SimpleMetricInputSpec
            ]()

            primary_entity = SemanticModelHelper.resolved_primary_entity(semantic_model)
            time_dimension_reference_to_grain = SemanticModelHelper.get_time_dimension_grains(semantic_model)

            for measure in semantic_model.measures:
                measure_reference = measure.reference
                self._measure_reference_to_measure[measure_reference] = measure

                # Handle non-additive dimension specs
                if measure.non_additive_dimension:
                    non_additive_dimension_spec = NonAdditiveDimensionSpec(
                        name=measure.non_additive_dimension.name,
                        window_choice=measure.non_additive_dimension.window_choice,
                        window_groupings=tuple(measure.non_additive_dimension.window_groupings),
                    )
                    self._measure_non_additive_dimension_specs[measure.reference] = non_additive_dimension_spec

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

                # Populate `_model_reference_to_aggregation_time_dimensions`.
                agg_time_dimension_reference = semantic_model.checked_agg_time_dimension_for_measure(measure.reference)
                # Ensure agg_time_dimension is lowercased - this transformation was not enforced on earlier manifests
                agg_time_dimension_reference = TimeDimensionReference(agg_time_dimension_reference.element_name.lower())

                matching_dimensions = tuple(
                    dimension
                    for dimension in semantic_model.dimensions
                    if dimension.type == DimensionType.TIME
                    and dimension.time_dimension_reference == agg_time_dimension_reference
                )

                matching_dimensions_count = len(matching_dimensions)
                if matching_dimensions_count != 1:
                    raise RuntimeError(
                        f"Found {matching_dimensions_count} matching dimensions for {agg_time_dimension_reference} "
                        f"in {semantic_model}"
                    )
                agg_time_dimension = matching_dimensions[0]
                if agg_time_dimension.type_params is None or agg_time_dimension.type_params.time_granularity is None:
                    raise RuntimeError(
                        f"Aggregation time dimension does not have a time granularity set: {agg_time_dimension}"
                    )

                self._model_reference_to_aggregation_time_dimensions[semantic_model.reference].add_value(
                    key=TimeDimensionReference(
                        element_name=agg_time_dimension.name,
                    ),
                    value=MeasureConverter.convert_to_measure_spec(measure=measure),
                )

    def get_properties(self, measure_reference: MeasureReference) -> MeasureRelationshipPropertySet:
        """Return properties of the measure as it relates to other elements in the semantic model."""
        property_set = self._measure_reference_to_property_set.get(measure_reference)
        if property_set is None:
            raise ValueError(
                LazyFormat(
                    "Unable to get properties as the given measure reference is unknown",
                    measure_reference=measure_reference,
                    known_measures=list(self._measure_reference_to_property_set.keys()),
                )
            )

        return property_set

    def get_measure(self, measure_reference: MeasureReference) -> Measure:
        """Return the measure object with the given reference."""
        measure = self._measure_reference_to_measure.get(measure_reference)
        if measure is None:
            raise ValueError(
                LazyFormat(
                    "Unable to get the measure as the given reference is unknown",
                    measure_reference=measure_reference,
                    known_measures=self._measure_reference_to_property_set.keys(),
                )
            )

        return measure

    @cached_property
    def measure_references(self) -> Sequence[MeasureReference]:
        """Return all measure references from the collection of semantic models."""
        return tuple(self._measure_reference_to_measure.keys())

    @cached_property
    def non_additive_dimension_specs_by_measure(self) -> Mapping[MeasureReference, NonAdditiveDimensionSpec]:
        """Return a mapping from all semi-additive measures to their corresponding non-additive dimension parameters.

        This includes all measures with non-additive dimension parameters, if any, from the collection of semantic models.
        """
        return self._measure_non_additive_dimension_specs

    def get_measures_by_model(self, model_reference: SemanticModelReference) -> Sequence[Measure]:  # noqa: D102
        try:
            return self._model_reference_to_measures[model_reference]
        except KeyError:
            raise KeyError(
                LazyFormat(
                    "Unable to get measures as the given model is not known",
                    model_name=model_reference.semantic_model_name,
                    known_model_names=[
                        model_reference.semantic_model_name for model_reference in self._model_reference_to_measures
                    ],
                )
            )

    def get_aggregation_time_dimensions_with_measures(
        self, semantic_model_reference: SemanticModelReference
    ) -> ElementGrouper[TimeDimensionReference, SimpleMetricInputSpec]:
        """Return all time dimensions in a semantic model with their associated measures."""
        try:
            return self._model_reference_to_aggregation_time_dimensions[semantic_model_reference]
        except KeyError:
            raise KeyError(
                LazyFormat(
                    "Can't get aggregation time dimensions as the given model is not known",
                    model_name=semantic_model_reference.semantic_model_name,
                    known_model_names=[
                        model_reference.semantic_model_name for model_reference in self._model_reference_to_measures
                    ],
                )
            )
