from __future__ import annotations

import pytest

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter


@pytest.fixture(scope="session")
def dataflow_to_sql_converter(  # noqa: D
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> DataflowToSqlQueryPlanConverter:
    return DataflowToSqlQueryPlanConverter(
        column_association_resolver=DunderColumnAssociationResolver(simple_semantic_manifest_lookup),
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )
