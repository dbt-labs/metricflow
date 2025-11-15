from __future__ import annotations

import copy
import logging
import re
import sys
from collections.abc import Sequence
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from enum import Enum
from functools import cached_property
from pathlib import Path
from random import Random
from typing import Final, Iterator, Mapping, Optional, TextIO, Union

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.project_configuration import PydanticProjectConfiguration
from dbt_semantic_interfaces.implementations.saved_query import PydanticSavedQuery
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.transformations.pydantic_rule_set import PydanticSemanticManifestTransformRuleSet
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import (
    PydanticSemanticManifestTransformer,
)
from dbt_semantic_interfaces.transformations.transform_rule import SemanticManifestTransformRule
from dbt_semantic_interfaces.type_enums import DimensionType, MetricType
from metricflow_semantics.errors.error_classes import MetricFlowException, SemanticManifestConfigurationError, \
    InvalidQueryException
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.manifest_helpers import mf_load_manifest_from_json_file
from metricflow_semantics.test_helpers.time_helpers import ConfigurableTimeSource
from metricflow_semantics.toolkit.cache.result_cache import ResultCache
from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.sql_client_fixtures import make_test_sql_client
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
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

            # Could be the case if the filter only contains a custom SQL expression.
            if len(variable_expressions) == 0:
                where_filter.where_sql_template = "TRUE"
                continue

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
                    self._update_filter(numerator.filter)
                denominator = metric.type_params.denominator
                if denominator is not None:
                    self._update_filter(denominator.filter)
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
    logger.info(LazyFormat("Starting check", manifest_path=manifest_path))

    if manifest_path in {Path("git_ignored/tmp/dbt_manifest/manifest_json/semantic_manifest_70437463654977.json")}:
        return

    # Skip empty files.
    if manifest_path.stat().st_size == 0:
        return

    semantic_manifest = mf_load_manifest_from_json_file(manifest_path)

    # Skip empty manifests.
    if len(semantic_manifest.semantic_models) == 0 or len(semantic_manifest.metrics) == 0:
        return

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
            LazyFormat(
                "Saved query error with EXPLAIN", manifest_path=manifest_path, saved_query_name=e.saved_query_name
            )
        )
    except Exception as e:
        raise RuntimeError(LazyFormat("Error with explain", manifest_path=manifest_path)) from e
    logger.info(
        LazyFormat("All saved queries passed EXPLAIN test", saved_query_count=len(semantic_manifest.saved_queries))
    )


class ManifestStore:
    _manifest_cache: ResultCache[str, PydanticSemanticManifest] = ResultCache()

    @classmethod
    def get_manifest(
        cls,
        manifest_handle: ManifestHandle,
    ) -> PydanticSemanticManifest:
        cache_key = manifest_handle.manifest_name
        result = cls._manifest_cache.get(cache_key)
        if result:
            return result.value

        if manifest_handle.manifest_path.stat().st_size == 0:
            return cls._manifest_cache.set_and_get(
                cache_key,
                PydanticSemanticManifest(
                    semantic_models=[],
                    metrics=[],
                    project_configuration=PydanticProjectConfiguration(),
                ),
            )

        semantic_manifest = mf_load_manifest_from_json_file(manifest_handle.manifest_path)
        semantic_manifest = UpdateTimeSpineRule(manifest_handle.time_spine_table).transform_model(semantic_manifest)
        semantic_manifest = ReplaceSqlRule(manifest_handle.dummy_table).transform_model(semantic_manifest)

        primary_rules: Sequence[SemanticManifestTransformRule[PydanticSemanticManifest]] = [
            UpdateTimeSpineRule(manifest_handle.time_spine_table),
            ReplaceSqlRule(manifest_handle.dummy_table),
        ]
        secondary_rules = PydanticSemanticManifestTransformRuleSet().all_rules[1]

        return cls._manifest_cache.set_and_get(
            cache_key,
            PydanticSemanticManifestTransformer.transform(
                semantic_manifest,
                ordered_rule_sequences=(
                    primary_rules,
                    secondary_rules,
                ),
            ),
        )


@fast_frozen_dataclass()
class ExplainExecutionEnvironment:
    semantic_manifest: PydanticSemanticManifest
    mf_engine: MetricFlowEngine


@fast_frozen_dataclass()
class ManifestHandle:
    manifest_name: str
    manifest_path: Path
    dummy_table: SqlTable
    time_spine_table: SqlTable

    @staticmethod
    def create(manifest_path: Path) -> ManifestHandle:
        manifest_name = manifest_path.name.replace(".json", "")
        source_schema = f"mf_explain_runner_{manifest_name}"
        dummy_table = SqlTable(schema_name=source_schema, table_name="dummy_table")
        time_spine_table = SqlTable(schema_name=source_schema, table_name="time_spine_table")

        return ManifestHandle(
            manifest_name=manifest_name,
            manifest_path=manifest_path,
            dummy_table=dummy_table,
            time_spine_table=time_spine_table,
        )


@fast_frozen_dataclass()
class ExplainRunnerInput:
    manifest_handle: ManifestHandle
    mf_request: MetricFlowQueryRequest
    log_file_path: Path
    pass_file_path: Path
    fail_file_path: Path


class ExplainStatus(Enum):
    PASS = "pass"
    FAIL = "pass"
    EXCEPTION_IGNORED = "exception_ignored"


class ExplainRunner:
    _manifest_path_to_engine: dict[Path, MetricFlowEngine] = {}

    _engine_cache: ResultCache[str, MetricFlowEngine] = ResultCache()
    _invalid_manifests: set[Path] = set()

    _sql_client_cache: ResultCache[None, SqlClientWithDDLMethods] = ResultCache()
    _execution_environment_cache: ResultCache[str, ExplainExecutionEnvironment] = ResultCache()

    @classmethod
    def _get_sql_client(cls) -> SqlClientWithDDLMethods:
        result = cls._sql_client_cache.get(None)
        if result:
            return result.value

        return cls._sql_client_cache.set_and_get(
            None, make_test_sql_client(url="duckdb://", password="", schema="default_schema")
        )

    @classmethod
    def _create_execution_environment(
        cls,
        manifest_handle: ManifestHandle,
    ) -> ExplainExecutionEnvironment:
        sql_client = cls._get_sql_client()

        semantic_manifest = ManifestStore.get_manifest(manifest_handle)

        schema_names: MutableOrderedSet[str] = MutableOrderedSet()
        if manifest_handle.dummy_table.schema_name is not None:
            schema_names.add(manifest_handle.dummy_table.schema_name)
        if manifest_handle.time_spine_table.schema_name is not None:
            schema_names.add(manifest_handle.time_spine_table.schema_name)

        for schema_name in schema_names:
            query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
            logger.info(LazyFormat("Executing query", query=query))
            sql_client.execute(query)
            # sql_client.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            # sql_client.execute(f"SELECT 1")
        # Create a dummy source table with 1 row to use in the node relation field for semantic models.
        dummy_table = manifest_handle.dummy_table
        dummy_table_snapshot = SqlTableSnapshot(
            table_name=dummy_table.table_name,
            column_definitions=(SqlTableColumnDefinition(name="int_column", type=SqlTableColumnType.INT),),
            rows=(("1",),),
            file_path=None,
        )

        assert dummy_table.schema_name is not None
        snapshot_loader = SqlTableSnapshotLoader(ddl_sql_client=sql_client, schema_name=dummy_table.schema_name)
        try:
            snapshot_loader.load(dummy_table_snapshot)
        except Exception as e:
            raise RuntimeError(
                LazyFormat("Error loading table snapshot", dummy_table_snapshot=dummy_table_snapshot)
            ) from e

        # Create a dummy time-spine table with the column names referenced in the manifest.
        time_spine_table = manifest_handle.time_spine_table
        assert time_spine_table.schema_name is not None
        snapshot_loader = SqlTableSnapshotLoader(ddl_sql_client=sql_client, schema_name=time_spine_table.schema_name)
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
            sql_client.execute(f"DROP TABLE IF EXISTS {time_spine_table.sql}")
            snapshot_loader.load(time_spine_table_snapshot)
        except Exception as e:
            raise RuntimeError(
                LazyFormat("Error loading table snapshot", time_spine_table_snapshot=time_spine_table_snapshot)
            ) from e

        column_association_resolver = DunderColumnAssociationResolver()
        semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
        query_parser = MetricFlowQueryParser(semantic_manifest_lookup=semantic_manifest_lookup)
        mf_engine = MetricFlowEngine(
            semantic_manifest_lookup=semantic_manifest_lookup,
            sql_client=sql_client,
            time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
            query_parser=query_parser,
            column_association_resolver=column_association_resolver,
        )

        return ExplainExecutionEnvironment(
            semantic_manifest=semantic_manifest,
            mf_engine=mf_engine,
        )

    @classmethod
    def _get_execution_environment(cls, manifest_handle: ManifestHandle) -> ExplainExecutionEnvironment:
        cache_key = manifest_handle.manifest_name
        result = cls._execution_environment_cache.get(cache_key)
        if result:
            return result.value

        return cls._execution_environment_cache.set_and_get(
            cache_key, cls._create_execution_environment(manifest_handle)
        )

    @classmethod
    def explain_query(cls, runner_input: ExplainRunnerInput) -> ExplainStatus:
        with cls._redirect_output_to_file(runner_input.log_file_path):
            try:
                execution_environment = cls._get_execution_environment(runner_input.manifest_handle)
                result = execution_environment.mf_engine.explain(runner_input.mf_request)
            except InvalidQueryException as e:
                logger.exception(
                    LazyFormat(
                        "Error running EXPLAIN",
                        manifest_handle=runner_input.manifest_handle,
                        mf_request=runner_input.mf_request,
                    )
                )
                with open(runner_input.pass_file_path, "w") as fp:
                    fp.write(str(LazyFormat("Ignoring invalid query", exception=str(e))))
                    fp.flush()
                return ExplainStatus.EXCEPTION_IGNORED
            except Exception as e:
                logger.exception(
                    LazyFormat(
                        "Error running EXPLAIN",
                        manifest_handle=runner_input.manifest_handle,
                        mf_request=runner_input.mf_request,
                    )
                )
                with open(runner_input.fail_file_path, "w") as fp:
                    fp.write(str(e))
                    fp.flush()
                return ExplainStatus.FAIL
            sql = result.sql_statement.sql
            logger.info(LazyFormat("Successfully ran request", mf_request=runner_input.mf_request, sql=sql))
            with open(runner_input.pass_file_path, "w") as fp:
                fp.write(sql)
                fp.flush()
            return ExplainStatus.PASS

    @classmethod
    @contextmanager
    def _redirect_output_to_file(cls, log_file_path: Path) -> Iterator[TextIO]:
        """Provides a context manager the redirects output, stderr, and logging output to the given file.

        Useful for debugging as without the log file, the output is invisible. This method is not thread safe due to
        mutation of global state (i.e. logging configuration, `redirect_*`).
        """
        with (
            open(log_file_path, "w") as log_file,
            redirect_stdout(log_file),
            redirect_stderr(log_file),
        ):
            # Setup logging. Note: a new process does not inherit the logging configuration of the parent process.
            logging_handler = logging.StreamHandler(log_file)
            logging_handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(filename)s:%(lineno)d [%(threadName)s] - %(message)s")
            )
            root_logger = logging.getLogger()
            previous_level = root_logger.getEffectiveLevel()
            try:
                root_logger.setLevel(logging.INFO)
                root_logger.addHandler(logging_handler)
                yield log_file
            finally:
                root_logger.removeHandler(logging_handler)
                root_logger.setLevel(previous_level)
                sys.stdout.flush()
                sys.stderr.flush()
                log_file.flush()
