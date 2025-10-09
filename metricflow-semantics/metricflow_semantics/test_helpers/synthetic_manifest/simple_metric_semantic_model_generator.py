from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension, PydanticDimensionTypeParams
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.type_enums import DimensionType, EntityType, TimeGranularity

from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)


class SimpleMetricSemanticModelGenerator:
    """Helps generate semantic models containing simple-metric inputs.

    Each of the generated semantic models contain an entity common to the semantic models containing dimensions so that any
    measure can be queried by any dimension.
    """

    def __init__(  # noqa: D107
        self,
        parameter_set: SyntheticManifestParameterSet,
    ) -> None:
        self._parameter_set = parameter_set

    def generate_semantic_models(self) -> Sequence[PydanticSemanticModel]:  # noqa: D102
        semantic_models = []
        for semantic_model_index in range(self._parameter_set.simple_metric_semantic_model_count):
            entities = [
                PydanticEntity(
                    name=self._get_primary_entity_name(semantic_model_index),
                    type=EntityType.PRIMARY,
                    description=None,
                    role=None,
                    config=None,
                ),
                PydanticEntity(
                    name=self._parameter_set.common_entity_name,
                    type=EntityType.UNIQUE,
                    description=None,
                    role=None,
                    config=None,
                ),
            ]

            dimensions = [
                PydanticDimension(
                    name="ds",
                    type=DimensionType.TIME,
                    type_params=PydanticDimensionTypeParams(
                        time_granularity=TimeGranularity.DAY,
                    ),
                    description=None,
                    metadata=None,
                    config=None,
                ),
            ]
            semantic_model_name = self.get_semantic_model_name(semantic_model_index)
            semantic_models.append(
                PydanticSemanticModel(
                    name=semantic_model_name,
                    node_relation=PydanticNodeRelation(
                        schema_name="demo",
                        alias=semantic_model_name,
                    ),
                    entities=entities,
                    dimensions=dimensions,
                    defaults=None,
                    description=None,
                    primary_entity=None,
                    metadata=None,
                    config=None,
                )
            )

        return semantic_models

    def get_semantic_model_name(self, semantic_model_index: int) -> str:  # noqa: D102
        return f"simple_metric_model_{semantic_model_index:03}"

    def _get_primary_entity_name(self, semantic_model_index: int) -> str:
        return f"simple_metric_model_{semantic_model_index:03}_primary_entity"
