from __future__ import annotations

import pytest

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer, SqlQueryPlanRenderer


@pytest.fixture
def default_sql_plan_renderer() -> SqlQueryPlanRenderer:  # noqa: D
    return DefaultSqlQueryPlanRenderer()


@pytest.fixture(scope="session")
def dataflow_to_sql_converter(  # noqa: D
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> DataflowToSqlQueryPlanConverter:
    return DataflowToSqlQueryPlanConverter(
        column_association_resolver=DunderColumnAssociationResolver(simple_semantic_manifest_lookup),
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )


@pytest.fixture(scope="session")
def extended_date_dataflow_to_sql_converter(  # noqa: D
    extended_date_semantic_manifest_lookup: SemanticManifestLookup,
) -> DataflowToSqlQueryPlanConverter:
    return DataflowToSqlQueryPlanConverter(
        column_association_resolver=DunderColumnAssociationResolver(extended_date_semantic_manifest_lookup),
        semantic_manifest_lookup=extended_date_semantic_manifest_lookup,
    )
