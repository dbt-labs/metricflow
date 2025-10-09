from __future__ import annotations

from typing import List

from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.project_configuration import PydanticProjectConfiguration
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.implementations.time_spine import PydanticTimeSpine, PydanticTimeSpinePrimaryColumn
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow_semantics.test_helpers.synthetic_manifest.categorical_dimension_generator import (
    CategoricalDimensionGenerator,
)
from metricflow_semantics.test_helpers.synthetic_manifest.dimension_semantic_model_generator import (
    DimensionSemanticModelGenerator,
)
from metricflow_semantics.test_helpers.synthetic_manifest.metric_generator import MetricGenerator
from metricflow_semantics.test_helpers.synthetic_manifest.saved_query_generator import SavedQueryGenerator
from metricflow_semantics.test_helpers.synthetic_manifest.simple_metric_semantic_model_generator import (
    SimpleMetricSemanticModelGenerator,
)
from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)


class SyntheticManifestGenerator:
    """Generates a synthetic semantic manifest that can be used for performance profiling."""

    def __init__(self, parameter_set: SyntheticManifestParameterSet) -> None:  # noqa: D107
        self._parameter_set = parameter_set
        self._categorical_dimension_generator = CategoricalDimensionGenerator(parameter_set)
        self._simple_metric_semantic_model_generator = SimpleMetricSemanticModelGenerator(
            parameter_set=parameter_set,
        )
        self._dimension_semantic_model_generator = DimensionSemanticModelGenerator(
            parameter_set=parameter_set,
            categorical_dimension_generator=self._categorical_dimension_generator,
        )
        self._metric_generator = MetricGenerator(
            parameter_set=parameter_set,
            semantic_model_generator=self._simple_metric_semantic_model_generator,
        )
        self._saved_query_generator = SavedQueryGenerator(
            parameter_set=parameter_set,
            metric_generator=self._metric_generator,
            categorical_dimension_generator=self._categorical_dimension_generator,
        )

    def generate_manifest(self) -> PydanticSemanticManifest:
        """Generate a manifest using the given parameters."""
        semantic_models: List[PydanticSemanticModel] = []

        semantic_models.extend(self._simple_metric_semantic_model_generator.generate_semantic_models())
        semantic_models.extend(self._dimension_semantic_model_generator.generate_semantic_models())

        return PydanticSemanticManifest(
            semantic_models=semantic_models,
            metrics=list(self._metric_generator.generate_metrics()),
            project_configuration=PydanticProjectConfiguration(
                time_spines=[
                    PydanticTimeSpine(
                        node_relation=PydanticNodeRelation(
                            alias="time_spine_source_table",
                            schema_name="demo",
                        ),
                        primary_column=PydanticTimeSpinePrimaryColumn(
                            name="ds",
                            time_granularity=TimeGranularity.DAY,
                        ),
                    )
                ]
            ),
            saved_queries=self._saved_query_generator.generate_saved_queries(),
        )
