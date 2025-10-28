from __future__ import annotations

from copy import deepcopy
from typing import Tuple

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.protocols.dimension import DimensionType
from dbt_semantic_interfaces.protocols.entity import EntityType
from dbt_semantic_interfaces.test_utils import semantic_model_with_guaranteed_meta
from dbt_semantic_interfaces.type_enums import MetricType
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.syntactic_sugar import mf_first_item

from metricflow.protocols.sql_client import SqlClient
from metricflow.validation.data_warehouse_model_validator import (
    DataWarehouseModelValidator,
    DataWarehouseTaskBuilder,
    DataWarehouseValidationTask,
)
from tests_metricflow.snapshot_utils import (
    assert_sql_snapshot_equal,
)


@pytest.fixture(scope="session")
def dw_backed_warehouse_validation_model(
    create_source_tables: None,
    data_warehouse_validation_model: PydanticSemanticManifest,
) -> PydanticSemanticManifest:
    """Model-generating fixture to ensure the underlying tables are created for querying.

    Without an explicit invocation of the create_data_warehouse_validation_model_tables fixture the
    tables used by the data_warehouse_validation_manifest are not guaranteed to exist. This fixture
    guarantees execution of the underlying create table statements for the model, and can be used in
    any test that executes warehouse validation queries. It is not needed for test cases which simply
    use the model to construct tasks without executing them.
    """
    return data_warehouse_validation_model


def test_build_semantic_model_tasks(  # noqa: D103
    data_warehouse_validation_model: PydanticSemanticManifest,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_semantic_model_tasks(manifest=data_warehouse_validation_model)
    assert len(tasks) == len(data_warehouse_validation_model.semantic_models)

    tasks = DataWarehouseTaskBuilder.gen_semantic_model_tasks(
        manifest=data_warehouse_validation_model, semantic_model_filters=[]
    )
    assert len(tasks) == 0


def test_task_runner(sql_client: SqlClient, mf_test_configuration: MetricFlowTestConfiguration) -> None:  # noqa: D103
    dw_validator = DataWarehouseModelValidator(sql_client=sql_client)

    def good_query() -> Tuple[str, SqlBindParameterSet]:
        return ("SELECT 'foo' AS foo", SqlBindParameterSet())

    tasks = [
        DataWarehouseValidationTask(
            query_and_params_callable=good_query, description="Validating foo", error_message="Could not select foo"
        ),
    ]

    issues = dw_validator.run_tasks(tasks=tasks)
    assert len(issues.all_issues) == 0

    def bad_query() -> Tuple[str, SqlBindParameterSet]:
        return ("SELECT (true) AS col1 FROM doesnt_exist", SqlBindParameterSet())

    err_msg_bad = "Could not access table 'doesnt_exist' in data warehouse"
    bad_task = DataWarehouseValidationTask(
        query_and_params_callable=bad_query, description="Validating foo", error_message=err_msg_bad
    )

    tasks.append(bad_task)
    issues = dw_validator.run_tasks(tasks=tasks)
    assert len(issues.all_issues) == 1
    assert len(issues.errors) == 1
    assert err_msg_bad in issues.errors[0].message


def test_validate_semantic_models(  # noqa: D103
    dw_backed_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    model = deepcopy(dw_backed_warehouse_validation_model)

    dw_validator = DataWarehouseModelValidator(sql_client=sql_client)

    issues = dw_validator.validate_semantic_models(model)
    assert len(issues.all_issues) == 0

    model.semantic_models.append(
        semantic_model_with_guaranteed_meta(
            name="test_semantic_model2",
            dimensions=[],
        )
    )

    issues = dw_validator.validate_semantic_models(model)
    assert len(issues.all_issues) == 1
    assert len(issues.errors) == 1
    assert "Unable to access semantic model `test_semantic_model2`" in issues.all_issues[0].message


def test_build_dimension_tasks(  # noqa: D103
    data_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_dimension_tasks(
        manifest=data_warehouse_validation_model,
        sql_client=sql_client,
    )
    # on semantic model query with all dimensions
    assert len(tasks) == 1
    # 1 categorical dimension task, 1 time dimension task, 4 granularity based time dimension tasks, 6 date_part tasks
    assert len(tasks[0].on_fail_subtasks) == 12

    tasks = DataWarehouseTaskBuilder.gen_dimension_tasks(
        manifest=data_warehouse_validation_model,
        sql_client=sql_client,
        semantic_model_filters=[],
    )
    assert len(tasks) == 0


def test_validate_dimensions(  # noqa: D103
    dw_backed_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    model = deepcopy(dw_backed_warehouse_validation_model)

    dw_validator = DataWarehouseModelValidator(sql_client=sql_client)

    issues = dw_validator.validate_dimensions(model)
    assert len(issues.all_issues) == 0

    dimensions = list(model.semantic_models[0].dimensions)
    dimensions.append(PydanticDimension(name="doesnt_exist", type=DimensionType.CATEGORICAL))
    model.semantic_models[0].dimensions = dimensions

    issues = dw_validator.validate_dimensions(model)
    # One isssue is created for the short circuit query failure, and another is
    # created for the sub task checking the specific dimension
    assert len(issues.all_issues) == 2


def test_build_entities_tasks(  # noqa: D103
    data_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_entity_tasks(
        manifest=data_warehouse_validation_model,
        sql_client=sql_client,
    )
    assert len(tasks) == 1  # on semantic model query with all entities
    assert len(tasks[0].on_fail_subtasks) == 1  # a sub task for each entity on the semantic model

    tasks = DataWarehouseTaskBuilder.gen_entity_tasks(
        manifest=data_warehouse_validation_model, sql_client=sql_client, semantic_model_filters=[]
    )
    assert len(tasks) == 0


def test_validate_entities(  # noqa: D103
    dw_backed_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    model = deepcopy(dw_backed_warehouse_validation_model)

    dw_validator = DataWarehouseModelValidator(sql_client=sql_client)

    issues = dw_validator.validate_entities(model)
    assert len(issues.all_issues) == 0

    entities = list(model.semantic_models[0].entities)
    entities.append(PydanticEntity(name="doesnt_exist", type=EntityType.UNIQUE))
    model.semantic_models[0].entities = entities

    issues = dw_validator.validate_entities(model)
    # One isssue is created for the short circuit query failure, and another is
    # created for the sub task checking the specific entity
    assert len(issues.all_issues) == 2


def test_build_simple_metric_input_tasks(  # noqa: D103
    data_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_simple_metric_tasks(
        manifest=data_warehouse_validation_model,
        sql_client=sql_client,
    )
    assert len(tasks) == 1  # on semantic model query with all simple-metric inputs
    assert len(tasks[0].on_fail_subtasks) == 1  # a sub task for each simple-metric input on the semantic model

    tasks = DataWarehouseTaskBuilder.gen_simple_metric_tasks(
        manifest=data_warehouse_validation_model, sql_client=sql_client, semantic_model_filters=[]
    )
    assert len(tasks) == 0


def test_validate_simple_metrics(  # noqa: D103
    dw_backed_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    semantic_manifest = deepcopy(dw_backed_warehouse_validation_model)

    dw_validator = DataWarehouseModelValidator(sql_client=sql_client)

    issues = dw_validator.validate_simple_metrics(semantic_manifest)
    assert len(issues.all_issues) == 0

    metrics = semantic_manifest.metrics
    model = mf_first_item(semantic_manifest.semantic_models)
    metrics.append(
        PydanticMetric(
            name="doesnt_exist",
            description=None,
            type=MetricType.SIMPLE,
            type_params=PydanticMetricTypeParams(
                measure=None,
                numerator=None,
                denominator=None,
                expr=None,
                window=None,
                grain_to_date=None,
                metrics=None,
                conversion_type_params=None,
                cumulative_type_params=None,
                input_measures=[],
                metric_aggregation_params=PydanticMetricAggregationParams(
                    semantic_model=model.name,
                    agg=AggregationType.SUM,
                    agg_params=None,
                    agg_time_dimension=None,
                    non_additive_dimension=None,
                ),
                join_to_timespine=False,
                fill_nulls_with=None,
            ),
            filter=None,
            metadata=None,
            config=None,
        )
    )

    issues = dw_validator.validate_simple_metrics(semantic_manifest)
    # One issue is created for the short circuit query failure, and another is
    # created for the subtask checking the specific simple-metric input
    assert len(issues.all_issues) == 2


@pytest.mark.sql_engine_snapshot
def test_build_metric_tasks(  # noqa: D103
    request: FixtureRequest,
    data_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_metric_tasks(
        manifest=data_warehouse_validation_model,
        sql_client=sql_client,
    )
    assert len(tasks) == 1
    (query_string, _params) = tasks[0].query_and_params_callable()

    assert_sql_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query0",
        sql=query_string,
        sql_engine=sql_client.sql_engine_type,
    )

    tasks = DataWarehouseTaskBuilder.gen_metric_tasks(
        manifest=data_warehouse_validation_model,
        sql_client=sql_client,
        metric_filters=[],
    )
    assert len(tasks) == 0


@pytest.mark.sql_engine_snapshot
def test_build_saved_query_tasks(  # noqa: D103
    request: FixtureRequest,
    simple_semantic_manifest: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_saved_query_tasks(
        manifest=simple_semantic_manifest,
        sql_client=sql_client,
    )
    assert len(tasks) == 5

    tasks = DataWarehouseTaskBuilder.gen_saved_query_tasks(
        manifest=simple_semantic_manifest, sql_client=sql_client, saved_query_filters=["p0_booking"]
    )
    assert len(tasks) == 1
    (query_string, _params) = tasks[0].query_and_params_callable()

    assert_sql_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query0",
        sql=query_string,
        sql_engine=sql_client.sql_engine_type,
    )
