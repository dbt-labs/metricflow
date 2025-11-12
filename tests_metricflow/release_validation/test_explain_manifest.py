from __future__ import annotations

import copy
import logging
import re
from collections.abc import Sequence
from functools import cached_property
from pathlib import Path
from random import Random
from typing import Final, Mapping, Optional, Union

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.saved_query import PydanticSavedQuery
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import (
    PydanticSemanticManifestTransformer,
)
from dbt_semantic_interfaces.transformations.transform_rule import SemanticManifestTransformRule
from dbt_semantic_interfaces.type_enums import DimensionType, MetricType
from metricflow_semantics.errors.error_classes import MetricFlowException, SemanticManifestConfigurationError
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.manifest_helpers import mf_load_manifest_from_json_file
from metricflow_semantics.test_helpers.time_helpers import ConfigurableTimeSource
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from tests_metricflow.release_validation.query_generator import ExhaustiveQueryGenerator
from tests_metricflow.table_snapshot.table_snapshots import (
    SqlTableColumnDefinition,
    SqlTableColumnType,
    SqlTableSnapshot,
    SqlTableSnapshotLoader,
)

logger = logging.getLogger(__name__)


def extract_variable_expression(jinja_template_str: str) -> Sequence[str]:
    """Return the variable expressions in the Jinja template.

    For example:
        `{{ user.first_name }} AND {{ user.last_name }}` -> ["{{ user.first_name }}", "{{ user.last_name}}"]
    """
    # Pattern to match {{ ... }} expressions, capturing everything between {{ and }}
    pattern = r"\{\{[^}]*\}\}"
    matches = re.findall(pattern, jinja_template_str)
    return matches


class ReplaceSqlRule(SemanticManifestTransformRule[PydanticSemanticManifest]):
    def __init__(self, empty_table: SqlTable) -> None:  # noqa: D101
        self._empty_table_node_relation = PydanticNodeRelation.from_string(empty_table.sql)

    def _update_filter(self, filter_intersection: Optional[PydanticWhereFilterIntersection]) -> None:
        if filter_intersection is None:
            return

        for where_filter in filter_intersection.where_filters:
            variable_expressions = extract_variable_expression(where_filter.where_sql_template)
            rewritten_expressions = tuple(f"({expression} IS NOT NULL)" for expression in variable_expressions)
            where_filter.where_sql_template = " AND ".join(rewritten_expressions)

    @override
    def transform_model(self, semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        transformed_manifest = copy.deepcopy(semantic_manifest)

        for semantic_model in transformed_manifest.semantic_models:
            for measure in semantic_model.measures:
                measure.expr = "1"
            for entity in semantic_model.entities:
                entity.expr = "1"
            for dimension in semantic_model.dimensions:
                dimension_type = dimension.type
                if dimension_type is DimensionType.CATEGORICAL:
                    dimension.expr = "'1'"
                elif dimension_type is DimensionType.TIME:
                    dimension.expr = "CAST('2020-01-01' AS TIMESTAMP)"
                else:
                    assert_values_exhausted(dimension_type)

            semantic_model.node_relation = copy.deepcopy(self._empty_table_node_relation)

        for metric in transformed_manifest.metrics:
            self._update_filter(metric.filter)

            for input_metric in metric.type_params.metrics or ():
                self._update_filter(input_metric.filter)

            metric_type = metric.type
            if metric_type is MetricType.SIMPLE:
                metric.type_params.expr = "1"
            elif metric_type is MetricType.DERIVED:
                referenced_metric_names = [
                    (input_metric.alias or input_metric.name) for input_metric in metric.type_params.metrics or ()
                ]
                metric.type_params.expr = " + ".join(referenced_metric_names)
            elif metric_type is MetricType.RATIO:
                numerator = metric.type_params.numerator
                if numerator is not None:
                    self._update_filter(metric.type_params.numerator.filter)
                denominator = metric.type_params.denominator
                if denominator is not None:
                    self._update_filter(metric.type_params.denominator.filter)
            elif (
                metric_type is MetricType.SIMPLE
                or metric_type is MetricType.CONVERSION
                or metric_type is MetricType.CUMULATIVE
            ):
                pass
            else:
                assert_values_exhausted(metric_type)

        for saved_query in transformed_manifest.saved_queries:
            self._update_filter(saved_query.query_params.where)

        return transformed_manifest


@fast_frozen_dataclass()
class RandomQueryDescriptor:
    max_metric_count: int
    max_group_by_item_count: int
    max_filter_count: int
    max_filter_group_by_item_count: int

    group_by_dimension_weight: int
    group_by_entity_weight: int
    group_by_metric_weight: int


class ExplainSavedQueryException(MetricFlowException):
    def __init__(
        self, message: Optional[Union[str, LazyFormat]] = None, *, saved_query_name: str, sql: Optional[str] = None
    ) -> None:
        super().__init__(message)
        self.saved_query_name: Final[str] = saved_query_name
        self.sql: Final[Optional[str]] = sql


class ExplainTester:
    def __init__(self, sql_client: SqlClient, semantic_manifest: PydanticSemanticManifest) -> None:
        self._semantic_manifest = semantic_manifest
        column_association_resolver = DunderColumnAssociationResolver()
        semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
        query_parser = MetricFlowQueryParser(semantic_manifest_lookup=semantic_manifest_lookup)
        self._mf_engine = MetricFlowEngine(
            semantic_manifest_lookup=semantic_manifest_lookup,
            sql_client=sql_client,
            time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
            query_parser=query_parser,
            column_association_resolver=column_association_resolver,
        )
        self._sql_client = sql_client

    @cached_property
    def _saved_query_name_to_saved_query(self) -> Mapping[str, PydanticSavedQuery]:
        return {saved_query.name: saved_query for saved_query in self._semantic_manifest.saved_queries}

    def explain_saved_query(self, saved_query_name: str) -> None:
        saved_query = self._saved_query_name_to_saved_query.get(saved_query_name)
        if saved_query is None:
            raise ExplainSavedQueryException(
                LazyFormat(
                    "Unknown saved query name",
                    saved_query_name=saved_query_name,
                    known_saved_query_names=list(self._saved_query_name_to_saved_query),
                ),
                saved_query_name=saved_query_name,
            )
        logger.info(LazyFormat("Explaining saved query", saved_query_name=saved_query_name))
        request = MetricFlowQueryRequest.create_with_random_request_id(
            saved_query_name=saved_query_name,
        )
        self.explain_query(request)

    def explain_all_saved_queries(self) -> None:
        for saved_query in self._semantic_manifest.saved_queries:
            self.explain_saved_query(saved_query.name)

    def explain_query(self, request: MetricFlowQueryRequest) -> None:
        try:
            explain_result = self._mf_engine.explain(request)
            sql = explain_result.sql_statement.sql
            logger.info(LazyFormat("Generated SQL", sql=sql))
        except SemanticManifestConfigurationError as e:
            exception_message = str(e)
            if "This query requires a time spine with granularity" not in exception_message:
                raise e
            return
        except BaseException as e:
            raise RuntimeError(LazyFormat("Error generating EXPLAIN SQL", request=request)) from e

        self._sql_client.dry_run(sql)

    def explain_random_query(self, random_query_descriptor: RandomQueryDescriptor, random_seed: int) -> None:
        random_instance = Random(random_seed)


class UpdateTimeSpineRule(SemanticManifestTransformRule[PydanticSemanticManifest]):
    def __init__(self, time_spine_table: SqlTable) -> None:  # noqa: D101
        self._time_spine_node_relation = PydanticNodeRelation.from_string(time_spine_table.sql)
        self._time_spine_table_sql = time_spine_table.sql

    @override
    def transform_model(self, semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        transformed_manifest = copy.deepcopy(semantic_manifest)

        for (
            time_spine_table_configuration
        ) in transformed_manifest.project_configuration.time_spine_table_configurations:
            time_spine_table_configuration.location = self._time_spine_table_sql

        for time_spine in transformed_manifest.project_configuration.time_spines:
            time_spine.node_relation = self._time_spine_node_relation

        return transformed_manifest


# def find_custom_grain_names_in_filter_intersection(filter_intersection: Optional[PydanticWhereFilterIntersection]) -> None:
#     if filter_intersection is None:
#         return
#
#     for where_filter in filter_intersection.where_filters:
#         variable_expressions = extract_variable_expression(where_filter.where_sql_template)
#         rewritten_expressions = tuple(f"({expression} IS NOT NULL)" for expression in variable_expressions)
#         where_filter.where_sql_template = " AND ".join(rewritten_expressions)
#         where_filter.call_parameter_sets()


def get_time_spine_column_names(semantic_manifest: SemanticManifest) -> Sequence[str]:
    column_names = set()
    for time_spine_table_configuration in semantic_manifest.project_configuration.time_spine_table_configurations:
        column_names.add(time_spine_table_configuration.column_name)

    for time_spine in semantic_manifest.project_configuration.time_spines:
        column_names.add(time_spine.primary_column.name)
        for custom_grain in time_spine.custom_granularities:
            column_names.add(custom_grain.column_name or custom_grain.name)

    return sorted(column_names)


def check_explain_saved_queries_in_manifest(
    manifest_path: Path,
    source_schema: str,
    ddl_sql_client: SqlClientWithDDLMethods,
    saved_query_name: Optional[str] = None,
) -> None:
    # Skip empty manifests.
    if manifest_path.stat().st_size == 0:
        return

    semantic_manifest = mf_load_manifest_from_json_file(manifest_path)

    # Create a dummy source table with 1 row to use in the node relation field for semantic models.
    empty_table = SqlTable(schema_name=source_schema, table_name="dummy_source")
    empty_table_snapshot = SqlTableSnapshot(
        table_name=empty_table.table_name,
        column_definitions=(SqlTableColumnDefinition(name="int_column", type=SqlTableColumnType.INT),),
        rows=(("1",),),
        file_path=None,
    )
    snapshot_loader = SqlTableSnapshotLoader(ddl_sql_client=ddl_sql_client, schema_name=source_schema)
    snapshot_loader.load(empty_table_snapshot)

    # Create a dummy time-spine table with the column names referenced in the manifest.
    time_spine_table = SqlTable(schema_name=source_schema, table_name="dummy_time_spine")

    time_spine_table_column_names = get_time_spine_column_names(semantic_manifest)
    time_spine_table_snapshot = SqlTableSnapshot(
        table_name=time_spine_table.table_name,
        column_definitions=tuple(
            SqlTableColumnDefinition(name=column_name, type=SqlTableColumnType.TIME)
            for column_name in time_spine_table_column_names
        ),
        rows=(tuple("2020-01-01" for _ in time_spine_table_column_names),),
        file_path=None,
    )
    try:
        ddl_sql_client.execute(f"DROP TABLE IF EXISTS {time_spine_table.sql}")
        snapshot_loader.load(time_spine_table_snapshot)
    except BaseException as e:
        raise RuntimeError(
            LazyFormat("Error loading table snapshot", time_spine_table_snapshot=time_spine_table_snapshot)
        ) from e

    semantic_manifest = UpdateTimeSpineRule(time_spine_table).transform_model(semantic_manifest)
    semantic_manifest = ReplaceSqlRule(empty_table).transform_model(semantic_manifest)
    semantic_manifest = PydanticSemanticManifestTransformer.transform(semantic_manifest)

    explain_tester = ExplainTester(
        sql_client=ddl_sql_client,
        semantic_manifest=semantic_manifest,
    )

    try:
        if saved_query_name is not None:
            explain_tester.explain_saved_query(saved_query_name)
        else:
            explain_tester.explain_all_saved_queries()
    except ExplainSavedQueryException as e:
        raise RuntimeError(
            LazyFormat("Error with EXPLAIN", manifest_path=manifest_path, saved_query_name=e.saved_query_name)
        )
    logger.info(
        LazyFormat("All saved queries passed EXPLAIN test", saved_query_count=len(semantic_manifest.saved_queries))
    )


def test_explain_saved_queries(
    mf_test_configuration: MetricFlowTestConfiguration,
    ddl_sql_client: SqlClientWithDDLMethods,
    sql_client: SqlClient,
    simple_semantic_manifest: PydanticSemanticManifest,
    create_source_tables: None,
) -> None:
    # manifest_path = Path("git_ignored/semantic_manifest_4286131.json")

    # Check all manifests
    manifest_directory = Path("git_ignored/tmp/dbt_manifest/manifest_json")
    manifest_paths = []
    for manifest_path in manifest_directory.rglob("*.json"):
        manifest_paths.append(manifest_path)

    for manifest_path in manifest_paths:
        logger.info(LazyFormat("Checking manifest path", manifest_path=manifest_path))
        check_explain_saved_queries_in_manifest(manifest_path, mf_test_configuration.mf_source_schema, ddl_sql_client)


def test_explain_one_saved_query(
    mf_test_configuration: MetricFlowTestConfiguration,
    ddl_sql_client: SqlClientWithDDLMethods,
    sql_client: SqlClient,
    simple_semantic_manifest: PydanticSemanticManifest,
    create_source_tables: None,
) -> None:
    # manifest_path = Path("git_ignored/tmp/dbt_manifest/manifest_json/semantic_manifest_70403103975183.json")
    # saved_query_name = "bidlevel_extracted_vs_exported_ratio_export"
    manifest_path = Path("/Users/paul_work/tmp/us_foods_manifest.json")
    saved_query_name = None

    check_explain_saved_queries_in_manifest(
        manifest_path, mf_test_configuration.mf_source_schema, ddl_sql_client, saved_query_name=saved_query_name
    )


def check_explain_all_queries_in_manifest(
    manifest_path: Path,
    source_schema: str,
    ddl_sql_client: SqlClientWithDDLMethods,
) -> None:
    # Skip empty manifests.
    if manifest_path.stat().st_size == 0:
        return

    semantic_manifest = mf_load_manifest_from_json_file(manifest_path)

    # Create a dummy source table with 1 row to use in the node relation field for semantic models.
    empty_table = SqlTable(schema_name=source_schema, table_name="dummy_source")
    empty_table_snapshot = SqlTableSnapshot(
        table_name=empty_table.table_name,
        column_definitions=(SqlTableColumnDefinition(name="int_column", type=SqlTableColumnType.INT),),
        rows=(("1",),),
        file_path=None,
    )
    snapshot_loader = SqlTableSnapshotLoader(ddl_sql_client=ddl_sql_client, schema_name=source_schema)
    snapshot_loader.load(empty_table_snapshot)

    # Create a dummy time-spine table with the column names referenced in the manifest.
    time_spine_table = SqlTable(schema_name=source_schema, table_name="dummy_time_spine")

    time_spine_table_column_names = get_time_spine_column_names(semantic_manifest)
    time_spine_table_snapshot = SqlTableSnapshot(
        table_name=time_spine_table.table_name,
        column_definitions=tuple(
            SqlTableColumnDefinition(name=column_name, type=SqlTableColumnType.TIME)
            for column_name in time_spine_table_column_names
        ),
        rows=(tuple("2020-01-01" for _ in time_spine_table_column_names),),
        file_path=None,
    )
    try:
        ddl_sql_client.execute(f"DROP TABLE IF EXISTS {time_spine_table.sql}")
        snapshot_loader.load(time_spine_table_snapshot)
    except BaseException as e:
        raise RuntimeError(
            LazyFormat("Error loading table snapshot", time_spine_table_snapshot=time_spine_table_snapshot)
        ) from e

    semantic_manifest = UpdateTimeSpineRule(time_spine_table).transform_model(semantic_manifest)
    semantic_manifest = ReplaceSqlRule(empty_table).transform_model(semantic_manifest)
    semantic_manifest = PydanticSemanticManifestTransformer.transform(semantic_manifest)

    query_generator = ExhaustiveQueryGenerator(semantic_manifest)
    explain_tester = ExplainTester(
        sql_client=ddl_sql_client,
        semantic_manifest=semantic_manifest,
    )

    successful_explain_count = 0
    queries = query_generator.generate_queries()
    query_count = len(queries)
    logger.info(LazyFormat("Generated possible queries", query_count=query_count))
    for i, query in enumerate(queries):
        logger.info(f"Running query [{i+1}/{query_count}]")
        try:
            explain_tester.explain_query(query)
            successful_explain_count += 1
        except Exception as e:
            raise RuntimeError(LazyFormat("Error with EXPLAIN", manifest_path=manifest_path, request=query)) from e
    logger.info(LazyFormat("Finished explaining all queries", saved_query_count=len(semantic_manifest.saved_queries)))


def test_explain_all_queries(
    mf_test_configuration: MetricFlowTestConfiguration,
    ddl_sql_client: SqlClientWithDDLMethods,
    sql_client: SqlClient,
    simple_semantic_manifest: PydanticSemanticManifest,
    create_source_tables: None,
) -> None:
    manifest_path = Path("git_ignored/tmp/dbt_manifest/manifest_json/semantic_manifest_4_pretty.json")
    # semantic_manifest = mf_load_manifest_from_json_file(manifest_path)
    # semantic_manifest = PydanticSemanticManifestTransformer.transform(semantic_manifest)
    #
    # query_generator = ExhaustiveQueryGenerator(semantic_manifest)
    # query_generator.count_possible_group_by_items()

    check_explain_all_queries_in_manifest(manifest_path, mf_test_configuration.mf_source_schema, ddl_sql_client)
