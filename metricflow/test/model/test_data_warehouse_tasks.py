from __future__ import annotations

from copy import deepcopy
from typing import Tuple

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.protocols.dimension import DimensionType
from dbt_semantic_interfaces.protocols.entity import EntityType
from dbt_semantic_interfaces.test_utils import semantic_model_with_guaranteed_meta
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType

from metricflow.model.data_warehouse_model_validator import (
    DataWarehouseModelValidator,
    DataWarehouseTaskBuilder,
    DataWarehouseValidationTask,
)
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_snapshot_text_equal, make_schema_replacement_function


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


def test_build_semantic_model_tasks(  # noqa:D
    data_warehouse_validation_model: PydanticSemanticManifest,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_semantic_model_tasks(manifest=data_warehouse_validation_model)
    assert len(tasks) == len(data_warehouse_validation_model.semantic_models)


def test_task_runner(sql_client: SqlClient, mf_test_session_state: MetricFlowTestSessionState) -> None:  # noqa: D
    dw_validator = DataWarehouseModelValidator(sql_client=sql_client)

    def good_query() -> Tuple[str, SqlBindParameters]:
        return ("SELECT 'foo' AS foo", SqlBindParameters())

    tasks = [
        DataWarehouseValidationTask(query_and_params_callable=good_query, error_message="Could not select foo"),
    ]

    issues = dw_validator.run_tasks(tasks=tasks)
    assert len(issues.all_issues) == 0

    def bad_query() -> Tuple[str, SqlBindParameters]:
        return ("SELECT (true) AS col1 FROM doesnt_exist", SqlBindParameters())

    err_msg_bad = "Could not access table 'doesnt_exist' in data warehouse"
    bad_task = DataWarehouseValidationTask(query_and_params_callable=bad_query, error_message=err_msg_bad)

    tasks.append(bad_task)
    issues = dw_validator.run_tasks(tasks=tasks)
    assert len(issues.all_issues) == 1
    assert len(issues.errors) == 1
    assert err_msg_bad in issues.errors[0].message


def test_validate_semantic_models(  # noqa: D
    dw_backed_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
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


def test_build_dimension_tasks(  # noqa: D
    data_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_dimension_tasks(
        manifest=data_warehouse_validation_model,
        sql_client=sql_client,
    )
    # on semantic model query with all dimensions
    assert len(tasks) == 1
    # 1 categorical dimension task, 1 time dimension task, 4 granularity based time dimension tasks, 7 date_part tasks
    assert len(tasks[0].on_fail_subtasks) == 13


def test_validate_dimensions(  # noqa: D
    dw_backed_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
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


def test_build_entities_tasks(  # noqa: D
    data_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_entity_tasks(
        manifest=data_warehouse_validation_model,
        sql_client=sql_client,
    )
    assert len(tasks) == 1  # on semantic model query with all entities
    assert len(tasks[0].on_fail_subtasks) == 1  # a sub task for each entity on the semantic model


def test_validate_entities(  # noqa: D
    dw_backed_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
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


def test_build_measure_tasks(  # noqa: D
    data_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_measure_tasks(
        manifest=data_warehouse_validation_model,
        sql_client=sql_client,
    )
    assert len(tasks) == 1  # on semantic model query with all measures
    assert len(tasks[0].on_fail_subtasks) == 1  # a sub task for each measure on the semantic model


def test_validate_measures(  # noqa: D
    dw_backed_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    model = deepcopy(dw_backed_warehouse_validation_model)

    dw_validator = DataWarehouseModelValidator(sql_client=sql_client)

    issues = dw_validator.validate_measures(model)
    assert len(issues.all_issues) == 0

    measures = list(model.semantic_models[0].measures)
    measures.append(PydanticMeasure(name="doesnt_exist", agg=AggregationType.SUM, agg_time_dimension="ds"))
    model.semantic_models[0].measures = measures

    issues = dw_validator.validate_measures(model)
    # One isssue is created for the short circuit query failure, and another is
    # created for the sub task checking the specific measure
    assert len(issues.all_issues) == 2


def test_build_metric_tasks(  # noqa: D
    request: FixtureRequest,
    data_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_metric_tasks(
        manifest=data_warehouse_validation_model,
        sql_client=sql_client,
    )
    assert len(tasks) == 1
    (query_string, _params) = tasks[0].query_and_params_callable()
    assert_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        group_id="data_warehouse_validation_manifest",
        snapshot_id="query0",
        snapshot_text=query_string,
        snapshot_file_extension=".sql",
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_session_state.mf_system_schema, source_schema=mf_test_session_state.mf_source_schema
        ),
        additional_sub_directories_for_snapshots=(sql_client.sql_engine_type.value,),
    )


def test_validate_metrics(  # noqa: D
    dw_backed_warehouse_validation_model: PydanticSemanticManifest,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    model = deepcopy(dw_backed_warehouse_validation_model)
    dw_validator = DataWarehouseModelValidator(sql_client=sql_client)

    issues = dw_validator.validate_metrics(model)
    assert len(issues.all_issues) == 0

    # Update model to have a new measure which creates a new metric by proxy
    new_measures = list(model.semantic_models[0].measures)
    new_measures.append(
        PydanticMeasure(
            name="count_cats",
            agg=AggregationType.SUM,
            expr="is_cat",  # doesn't exist as column
            create_metric=True,
        )
    )
    model.semantic_models[0].measures = new_measures
    model.metrics = []
    model = PydanticSemanticManifestTransformer.transform(model)

    # Validate new metric created by proxy causes an issue (because the column used doesn't exist)
    dw_validator = DataWarehouseModelValidator(sql_client=sql_client)
    issues = dw_validator.validate_metrics(model)
    assert len(issues.all_issues) == 1
    assert len(issues.errors) == 1
    assert "Unable to query metric `count_cats`" in issues.errors[0].message
