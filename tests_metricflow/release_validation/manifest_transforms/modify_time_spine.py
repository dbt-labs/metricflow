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


class ModifyTimeSpineTableRule(SemanticManifestTransformRule[PydanticSemanticManifest]):
    """Modifies the node relation in semantic models to the provided schema and/or table."""

    def __init__(self, schema_name: Optional[str] = None, table_name: Optional[str] = None) -> None:  # noqa: D107
        self._schema_name = schema_name
        self._table_name = table_name

    @override
    def transform_model(self, semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        transformed_manifest = copy.deepcopy(semantic_manifest)
        for (
            time_spine_table_configuration
        ) in transformed_manifest.project_configuration.time_spine_table_configurations:
            previous_location = SqlTable.from_string(time_spine_table_configuration.location)
            time_spine_table_configuration.location = SqlTable(
                schema_name=self._schema_name or previous_location.schema_name,
                table_name=self._table_name or previous_location.table_name,
            ).sql

        for time_spine in transformed_manifest.project_configuration.time_spines:
            previous_relation = time_spine.node_relation
            time_spine.node_relation = PydanticNodeRelation.from_string(
                SqlTable(
                    schema_name=self._schema_name or previous_relation.schema_name,
                    table_name=self._table_name or previous_relation.alias,
                ).sql
            )

        return transformed_manifest
