from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension, PydanticDimensionTypeParams
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.type_enums import AggregationType, DimensionType, EntityType, TimeGranularity

from tests_metricflow.performance.measure_generator import MeasureGenerator
from tests_metricflow.performance.synthetic_manifest_parameter_set import SyntheticManifestParameterSet


class MeasureSemanticModelGenerator:
    """Helps generate semantic models containing measures.

    Each of the generated semantic models contain an entity common to the semantic models containing dimensions so that any
    measure can be queried by any dimension.
    """

    def __init__(  # noqa: D107
        self,
        parameter_set: SyntheticManifestParameterSet,
        measure_generator: MeasureGenerator,
    ) -> None:
        self._parameter_set = parameter_set
        self._measure_generator = measure_generator

    def generate_semantic_models(self) -> Sequence[PydanticSemanticModel]:  # noqa: D102
        semantic_models = []
        measures_per_semantic_model = self._parameter_set.measures_per_semantic_model
        next_measure_index = 0

        for semantic_model_index in range(self._parameter_set.measure_semantic_model_count):
            measures = []

            for _ in range(measures_per_semantic_model):
                measures.append(
                    PydanticMeasure(
                        name=self._measure_generator.get_measure_name(next_measure_index),
                        agg=AggregationType.SUM,
                        agg_time_dimension="ds",
                    )
                )
                next_measure_index = self._measure_generator.get_next_wrapped_index(next_measure_index)

            entities = [
                PydanticEntity(
                    name=self._get_primary_entity_name_for_measure_semantic_model(semantic_model_index),
                    type=EntityType.PRIMARY,
                ),
                PydanticEntity(
                    name=self._parameter_set.common_entity_name,
                    type=EntityType.UNIQUE,
                ),
            ]

            dimensions = [
                PydanticDimension(
                    name="ds",
                    type=DimensionType.TIME,
                    type_params=PydanticDimensionTypeParams(
                        time_granularity=TimeGranularity.DAY,
                    ),
                ),
            ]
            semantic_model_name = self._get_measure_semantic_model_name(semantic_model_index)
            semantic_models.append(
                PydanticSemanticModel(
                    name=semantic_model_name,
                    node_relation=PydanticNodeRelation(
                        schema_name="demo",
                        alias=semantic_model_name,
                    ),
                    measures=measures,
                    entities=entities,
                    dimensions=dimensions,
                )
            )

        return semantic_models

    def _get_measure_semantic_model_name(self, semantic_model_index: int) -> str:
        return f"measure_model_{semantic_model_index:03}"

    def _get_primary_entity_name_for_measure_semantic_model(self, semantic_model_index: int) -> str:
        return f"measure_model_{semantic_model_index:03}_primary_entity"
