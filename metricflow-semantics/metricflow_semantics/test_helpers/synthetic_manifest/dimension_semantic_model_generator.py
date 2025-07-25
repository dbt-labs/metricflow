from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.type_enums import DimensionType, EntityType

from metricflow_semantics.test_helpers.synthetic_manifest.categorical_dimension_generator import (
    CategoricalDimensionGenerator,
)
from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)


class DimensionSemanticModelGenerator:
    """Helps generate a semantic model containing dimensions.

    Each of the generated semantic models contain an entity common to the semantic models containing measures so that
    any measure can be queried by any dimension.
    """

    def __init__(  # noqa: D107
        self,
        parameter_set: SyntheticManifestParameterSet,
        categorical_dimension_generator: CategoricalDimensionGenerator,
    ) -> None:
        self._parameter_set = parameter_set
        self._dimension_generator = categorical_dimension_generator

    def generate_semantic_models(self) -> Sequence[PydanticSemanticModel]:  # noqa: D102
        semantic_models = []
        for semantic_model_index in range(self._parameter_set.dimension_semantic_model_count):
            entities = [
                PydanticEntity(
                    name=self._get_dimension_semantic_model_primary_entity_name(semantic_model_index),
                    type=EntityType.PRIMARY,
                ),
                PydanticEntity(
                    name=self._parameter_set.common_entity_name,
                    type=EntityType.UNIQUE,
                ),
            ]

            dimensions = [
                PydanticDimension(
                    name=self._get_dimension_name(
                        index_in_manifest=semantic_model_index,
                        index_in_model=dimension_index,
                    ),
                    type=DimensionType.CATEGORICAL,
                )
                for dimension_index in range(self._parameter_set.categorical_dimensions_per_semantic_model)
            ]

            semantic_model_name = self._get_dimension_semantic_model_name(semantic_model_index)
            semantic_models.append(
                PydanticSemanticModel(
                    name=semantic_model_name,
                    node_relation=PydanticNodeRelation(
                        schema_name="demo",
                        alias=semantic_model_name,
                    ),
                    entities=entities,
                    dimensions=dimensions,
                )
            )

        return semantic_models

    def _get_dimension_semantic_model_name(self, index_in_manifest: int) -> str:
        return f"dimension_model_{index_in_manifest:03}"

    def _get_dimension_semantic_model_primary_entity_name(self, semantic_model_index: int) -> str:
        return f"{self._get_dimension_semantic_model_name(semantic_model_index)}_primary_entity"

    def _get_dimension_name(self, index_in_manifest: int, index_in_model: int) -> str:
        """Get the name of the dimension given the index.

        Args:
            index_in_manifest: The index of the semantic model in the manifest. e.g. the 2nd semantic model in the
            semantic manifest.
            index_in_model: The index of the dimension in the semantic model. e.g. the 2nd dimension in the semantic
            model.

        Returns:
            The name of the dimension given the index.
        """
        return self._dimension_generator.get_dimension_name(
            index_in_manifest * self._parameter_set.categorical_dimensions_per_semantic_model + index_in_model
        )
