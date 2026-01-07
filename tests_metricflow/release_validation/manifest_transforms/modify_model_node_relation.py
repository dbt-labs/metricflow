from __future__ import annotations

import copy
import logging
from typing import Optional

from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.transformations.transform_rule import SemanticManifestTransformRule
from metricflow_semantics.sql.sql_table import SqlTable
from typing_extensions import override

logger = logging.getLogger(__name__)


class ModifyModelNodeRelationRule(SemanticManifestTransformRule[PydanticSemanticManifest]):
    """Modifies the node relation in semantic models to the provided schema and/or table."""

    def __init__(self, schema_name: Optional[str] = None, table_name: Optional[str] = None) -> None:  # noqa: D107
        self._schema_name = schema_name
        self._table_name = table_name

    @override
    def transform_model(self, semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        transformed_manifest = copy.deepcopy(semantic_manifest)

        # Replace all element expressions in semantic models.
        for semantic_model in transformed_manifest.semantic_models:
            semantic_model.node_relation = PydanticNodeRelation.from_string(
                SqlTable(
                    schema_name=self._schema_name or semantic_model.node_relation.schema_name,
                    table_name=self._table_name or semantic_model.node_relation.alias,
                ).sql
            )

        return transformed_manifest
