from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

from metricflow.converters.osi.models import (
    OSIDataset,
    OSIDialect,
    OSIDialectExpression,
    OSIDimension,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSISemanticModel,
)
from metricflow_semantic_interfaces.protocols.dimension import Dimension
from metricflow_semantic_interfaces.protocols.entity import Entity
from metricflow_semantic_interfaces.protocols.measure import Measure
from metricflow_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from metricflow_semantic_interfaces.protocols.semantic_model import SemanticModel
from metricflow_semantic_interfaces.type_enums import (
    DimensionType,
    EntityType,
)


class MSIToOSIConverter:
    """Converts an MSI SemanticManifest into an OSI Document."""

    def __init__(self, dialect: OSIDialect = OSIDialect.ANSI_SQL) -> None:  # noqa: D107
        self._dialect = dialect

    def convert(self, manifest: SemanticManifest, model_name: str = "semantic_model") -> OSIDocument:  # noqa: D102
        datasets = [self._convert_semantic_model(sm) for sm in manifest.semantic_models]

        return OSIDocument(
            version="0.1.1",
            dialects=[self._dialect],
            semantic_model=[
                OSISemanticModel(
                    name=model_name,
                    datasets=datasets,
                    relationships=None,
                    metrics=None,
                )
            ],
        )

    def _convert_semantic_model(self, sm: SemanticModel) -> OSIDataset:
        fields: List[OSIField] = []
        for entity in sm.entities:
            fields.append(self._convert_entity(entity))
        for dim in sm.dimensions:
            fields.append(self._convert_dimension(dim))
        for measure in sm.measures:
            fields.append(self._convert_measure(measure))

        primary_key, unique_keys = self._extract_keys(sm.entities)

        return OSIDataset(
            name=sm.name,
            source=sm.node_relation.relation_name,
            primary_key=primary_key,
            unique_keys=unique_keys if unique_keys else None,
            description=sm.description,
            fields=fields if fields else None,
        )

    def _convert_dimension(self, dim: Dimension) -> OSIField:
        expr = dim.expr if dim.expr is not None else dim.name
        is_time = dim.type == DimensionType.TIME

        return OSIField(
            name=dim.name,
            expression=self._make_expression(expr),
            dimension=OSIDimension(is_time=is_time),
            label=dim.label,
            description=dim.description,
        )

    def _convert_entity(self, entity: Entity) -> OSIField:
        expr = entity.expr if entity.expr is not None else entity.name

        return OSIField(
            name=entity.name,
            expression=self._make_expression(expr),
            label=entity.label,
            description=entity.description,
        )

    def _convert_measure(self, measure: Measure) -> OSIField:
        expr = measure.expr if measure.expr is not None else measure.name

        return OSIField(
            name=measure.name,
            expression=self._make_expression(expr),
            label=measure.label,
            description=measure.description,
        )

    @staticmethod
    def _extract_keys(entities: Sequence[Entity]) -> Tuple[Optional[List[str]], List[List[str]]]:
        primary_key: Optional[List[str]] = None
        unique_keys: List[List[str]] = []

        for entity in entities:
            col = entity.expr if entity.expr is not None else entity.name
            if entity.type == EntityType.PRIMARY:
                primary_key = [col]
            elif entity.type == EntityType.UNIQUE:
                unique_keys.append([col])

        return primary_key, unique_keys

    def _make_expression(self, expr: str) -> OSIExpression:
        return OSIExpression(dialects=[OSIDialectExpression(dialect=self._dialect, expression=expr)])
