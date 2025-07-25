from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.implementations.saved_query import PydanticSavedQuery, PydanticSavedQueryQueryParams
from dbt_semantic_interfaces.references import EntityReference

from metricflow_semantics.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.test_helpers.synthetic_manifest.categorical_dimension_generator import (
    CategoricalDimensionGenerator,
)
from metricflow_semantics.test_helpers.synthetic_manifest.metric_generator import MetricGenerator
from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)


class SavedQueryGenerator:
    """Helps generate saved queries for the synthetic manifest."""

    def __init__(  # noqa: D107
        self,
        parameter_set: SyntheticManifestParameterSet,
        metric_generator: MetricGenerator,
        categorical_dimension_generator: CategoricalDimensionGenerator,
    ) -> None:
        self._parameter_set = parameter_set
        self._metric_generator = metric_generator
        self._dimension_generator = categorical_dimension_generator
        self._naming_scheme = ObjectBuilderNamingScheme()

    def _get_saved_query_name(self, saved_query_index: int) -> str:
        return f"saved_query_{saved_query_index:03}"

    def generate_saved_queries(self) -> Sequence[PydanticSavedQuery]:  # noqa: D102
        saved_queries = []
        next_metric_index = self._metric_generator.get_first_index_at_max_depth()
        next_categorical_dimension_index = 0

        for saved_query_index in range(self._parameter_set.saved_query_count):
            metrics = []
            for _ in range(self._parameter_set.metrics_per_saved_query):
                metrics.append(self._metric_generator.get_metric_name(next_metric_index))
                next_metric_index = self._metric_generator.get_next_wrapped_width_index(next_metric_index)
            categorical_dimensions = []
            for _ in range(self._parameter_set.categorical_dimensions_per_saved_query):
                categorical_dimensions.append(
                    self._naming_scheme.input_str(
                        DimensionSpec(
                            element_name=self._dimension_generator.get_dimension_name(next_categorical_dimension_index),
                            entity_links=(EntityReference(self._parameter_set.common_entity_name),),
                        )
                    )
                )
                next_categorical_dimension_index = self._dimension_generator.get_next_wrapped_index(
                    next_categorical_dimension_index
                )

            saved_queries.append(
                PydanticSavedQuery(
                    name=self._get_saved_query_name(saved_query_index),
                    query_params=PydanticSavedQueryQueryParams(
                        metrics=metrics,
                        group_by=categorical_dimensions,
                    ),
                )
            )

        return saved_queries
