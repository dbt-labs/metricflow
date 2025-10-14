from __future__ import annotations

from typing import Dict, List, Tuple

from msi_pydantic_shim import Field
from typing_extensions import override

from metricflow_semantic_interfaces.implementations.base import HashableBaseModel
from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from metricflow_semantic_interfaces.implementations.metric import PydanticMetric
from metricflow_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from metricflow_semantic_interfaces.implementations.saved_query import PydanticSavedQuery
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from metricflow_semantic_interfaces.protocols import ProtocolHint, SemanticManifest


class PydanticSemanticManifest(HashableBaseModel, ProtocolHint[SemanticManifest]):
    """Model holds all the information the SemanticLayer needs to render a query."""

    @override
    def _implements_protocol(self) -> SemanticManifest:
        return self

    semantic_models: List[PydanticSemanticModel]
    metrics: List[PydanticMetric]
    project_configuration: PydanticProjectConfiguration
    saved_queries: List[PydanticSavedQuery] = Field(default_factory=list)

    def build_measure_name_to_model_and_measure_map(
        self,
    ) -> Dict[str, Tuple[PydanticSemanticModel, PydanticMeasure]]:  # noqa: E501
        """Build a mapping from measure name to the semantic model name that contains it."""
        measure_to_model = {}
        for semantic_model in self.semantic_models:
            for measure in semantic_model.measures:
                measure_to_model[measure.name] = (semantic_model, measure)
        return measure_to_model
