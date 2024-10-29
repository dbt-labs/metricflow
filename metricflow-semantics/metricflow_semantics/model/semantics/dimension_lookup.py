from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Sequence

from dbt_semantic_interfaces.protocols import SemanticModel
from dbt_semantic_interfaces.references import DimensionReference
from dbt_semantic_interfaces.type_enums import DimensionType

from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat


@dataclass(frozen=True)
class DimensionInvariant:
    """For a given manifest, all defined dimensions with the same name should have these same properties."""

    dimension_type: DimensionType
    is_partition: bool


class DimensionLookup:
    """Looks up properties related to dimensions."""

    def __init__(self, semantic_models: Sequence[SemanticModel]) -> None:  # noqa: D107
        self._dimension_reference_to_invariant: Dict[DimensionReference, DimensionInvariant] = {}
        for semantic_model in semantic_models:
            for dimension in semantic_model.dimensions:
                invariant = DimensionInvariant(
                    dimension_type=dimension.type,
                    is_partition=dimension.is_partition,
                )
                dimension_reference = dimension.reference
                existing_invariant = self._dimension_reference_to_invariant.get(dimension_reference)
                if existing_invariant is not None and existing_invariant != invariant:
                    raise ValueError(
                        str(
                            LazyFormat(
                                "Dimensions with the same name have been defined with conflicting values that "
                                "should have been the same in a given semantic manifest. This should have been caught "
                                "during validation.",
                                dimension_reference=dimension_reference,
                                existing_invariant=existing_invariant,
                                conflicting_invariant=invariant,
                                semantic_model_reference=semantic_model.reference,
                            )
                        )
                    )

                self._dimension_reference_to_invariant[dimension_reference] = invariant

    def get_invariant(self, dimension_reference: DimensionReference) -> DimensionInvariant:
        """Get invariants for the given dimension in the semantic manifest."""
        # dimension_reference might be a TimeDimensionReference, so change types.
        dimension_reference = DimensionReference(element_name=dimension_reference.element_name)
        invariant = self._dimension_reference_to_invariant[dimension_reference]
        if invariant is None:
            raise ValueError(
                str(
                    LazyFormat(
                        "Unknown dimension reference",
                        dimension_reference=dimension_reference,
                        known_dimension_references=list(self._dimension_reference_to_invariant.keys()),
                    )
                )
            )

        return invariant
